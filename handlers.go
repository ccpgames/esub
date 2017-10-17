package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:    1024,
	WriteBufferSize:   1024,
	CheckOrigin:       func(r *http.Request) bool { return true },
	EnableCompression: true,
}

// HandlePing -- GET /ping -- no metrics
func HandlePing(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	logWrite(w, http.StatusOK, []byte("OK"))
}

// HandleInfo -- GET /info -- static info about us
func HandleInfo(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logWrite(w, http.StatusOK, infoJSON)
	shipMetric(ctx, true)
}

func closeConn(conn *websocket.Conn, code int, reason string) error {
	message := []byte{}
	if code != websocket.CloseNoStatusReceived {
		message = websocket.FormatCloseMessage(code, reason)
	}
	w, err := conn.NextWriter(websocket.CloseMessage)
	if err != nil {
		return err
	}

	if _, writeErr := io.Copy(w, bytes.NewBuffer(message)); writeErr != nil {
		return writeErr
	}

	if closeErr := w.Close(); closeErr != nil {
		return closeErr
	}
	return conn.Close()
}

func closePsubInvalid(ctx context.Context, conn *websocket.Conn) {
	shipMetric(ctx, false)
	err := closeConn(conn, websocket.CloseMandatoryExtension, "invalid token")
	if err != nil {
		log.Printf("failed to close socket: %+v", err)
	}
}

// HandlePSub -- GET /psub/:sub
func HandlePSub(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)
	shared := r.URL.Query().Get("shared") == "1"
	ctx, cancel := context.WithCancel(ctx)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		cancel()
		return
	}

	if !ValidPSub(key, token, shared) {
		closePsubInvalid(ctx, conn)
		log.Println("invalid psub req")
		cancel()
		return
	}

	sub := RegisterPSub(cancel, key, token, shared, conn)

	if !CONFIRM {
		conn.SetPongHandler(sub.PongHandler)
		go sub.ReadLoop()
	}

	// maybe this could happen in race conditions...
	if !<-sub.Valid {
		DeregisterPSub(sub)
		closePsubInvalid(ctx, conn)
		cancel()
		return
	}

	conn.SetCloseHandler(closeWS(ctx, sub.Mutex, psubConn{sub}, conn))

	for {
		rep, subErr := sub.Get(ctx)
		if subErr != nil {
			DeregisterPSub(sub)
			return
		} else if !WriteToPSub(conn, rep, sub) {
			DeregisterPSub(sub)
			RedirectPSub(sub, rep)
			cancel()
			return
		} else {
			shipMetric(ctx, true)
		}
	}
}

// HandleSub -- GET /sub/:sub
func HandleSub(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)

	if key == "" {
		logWrite(w, http.StatusNotFound, []byte("Not found"))
		shipMetric(ctx, false)
		return
	}

	// start a child cancel context
	ctx, cancel := context.WithCancel(ctx)

	// create/register the sub
	sub := RegisterSub(cancel, key, token)

	// check for validity
	if !<-sub.Valid {
		logWrite(
			w,
			http.StatusForbidden,
			[]byte("Failed to create sub. Key is used and token does not match."),
		)
		shipMetric(ctx, false)
		return
	}

	// we could be waiting here a while
	rep, err := sub.Get(ctx)

	// could be canceled, could be done. either way clean up if we're the owner
	subDeleteChan <- sub

	if err != nil {
		logWrite(
			w,
			http.StatusInternalServerError,
			[]byte(fmt.Sprintf("Failed to fullfil sub: %+v", err.Error())),
		)
		shipMetric(ctx, false)
		return
	}

	// write the body, we're done
	logWrite(w, http.StatusOK, rep.data)

	// send our metric
	metric := shipMetric(ctx, true)

	// send a metric for how long we held the rep in memory
	SendMetric(
		Metric{
			Name:    "sub_reply",
			Route:   metric.Route,
			Key:     metric.Key,
			Auth:    metric.Auth,
			Success: metric.Success,
		},
		1,
		rep.repTime,
	)
}

// HandlePRep -- GET /prep/ -- websocket reps
func HandlePRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	ctx, cancel := context.WithCancel(ctx)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		cancel()
		return
	}

	pRep := RegisterPRep(cancel, conn)

	go pRep.ReadLoop()

	conn.SetCloseHandler(closeWS(ctx, pRep.WriteMutex, pRepConn{pRep}, conn))

	for {
		rep, wsErr := pRep.Read(ctx)
		if wsErr != nil {
			// invalid format or disconnect, kill the client
			DeregisterPRep(pRep)
			return
		}

		// start a timer for metrics
		repTime := time.Now().UTC()

		// checks for validity, passes to sub
		repErr := receiveRep(rep.key, rep.token, rep.psub, rep.rep, repTime)

		var confirmed bool
		if CONFIRM {
			confirmed = pRep.Confirm(repErr)
		} else {
			confirmed = false
		}

		// send a metric for this pRep message
		SendMetric(
			Metric{
				Name:      "prep_message",
				Route:     "prep",
				Key:       rep.key,
				Auth:      rep.token != "",
				Success:   repErr.code == 0,
				Confirmed: confirmed,
			},
			1,
			repTime,
		)
	}
}

// HandleRep -- POST /rep/:sub
func HandleRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	// fetch our chain-created sub key
	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)
	start := ctx.Value(keyStart).(time.Time)

	// optionally specify replying to a psub, minor performance gain
	psubRep := r.URL.Query().Get("psub") == "1"

	// read and receive the rep, auth and existence checks are here
	repError := receiveRep(key, token, psubRep, postRep{r}, start)
	if repError.code > 0 {
		logWrite(w, repError.code, repError.message)
		shipMetric(ctx, false)
		return
	}

	// write our basic response
	logWrite(w, http.StatusOK, []byte("OK"))

	// send our timing metric
	shipMetric(ctx, true)
}

// HandleKeys -- GET /keys
func HandleKeys(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	// TODO: add optional token auth to this route

	// fetch the current list of local subs
	subs := SubList()

	if subs == nil {
		// no active subs, still a success
		logWrite(w, http.StatusNoContent, nil)
		shipMetric(ctx, true)
		return
	}

	// dump keys to json
	jsonKeys, err := json.Marshal(subs)
	if err != nil {
		logWrite(
			w,
			http.StatusInternalServerError,
			[]byte(fmt.Sprintf("Failed to dump JSON: %+v", err.Error())),
		)
		shipMetric(ctx, false)
		return
	}

	// write the response
	w.Header().Set("Content-Type", "application/json")
	logWrite(w, http.StatusOK, jsonKeys)
	shipMetric(ctx, true)
}

func closeWS(
	ctx context.Context,
	m sync.Locker,
	w wsConn,
	conn *websocket.Conn,
) func(code int, text string) error {
	return func(code int, text string) error {
		shipMetric(ctx, false)
		w.Deregister()
		m.Lock()
		if connClosed := closeConn(conn, code, ""); connClosed != nil {
			// one way or another, be sure to close the socket
			if closeErr := conn.Close(); closeErr != nil {
				log.Printf("failed to close socket: %+v: %+v", connClosed, closeErr)
			}
		}
		m.Unlock()
		w.Cancel()
		return nil
	}
}

type wsConn interface {
	Cancel()
	Deregister()
}

type psubConn struct {
	sub Sub
}

func (p psubConn) Cancel() {
	p.sub.Cancel()
}

func (p psubConn) Deregister() {
	DeregisterPSub(p.sub)
}

type pRepConn struct {
	pRep PRep
}

func (p pRepConn) Cancel() {
	p.pRep.Cancel()
}

func (p pRepConn) Deregister() {
	DeregisterPRep(p.pRep)
}
