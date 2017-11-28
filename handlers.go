package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
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
	SendTiming(ctx, httpRequest, "info", r.Method, "true", "false")
}

func closeConn(conn *websocket.Conn, code int) error {
	message := []byte{}
	if code != websocket.CloseNoStatusReceived {
		message = websocket.FormatCloseMessage(code, "")
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

// HandlePSub -- GET /psub/:sub
func HandlePSub(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)
	shared := r.URL.Query().Get("shared") == "1"
	ctx, cancel := context.WithCancel(ctx)
	auth := strconv.FormatBool(token != "")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		cancel()
		SendTiming(ctx, httpRequest, "psub", r.Method, "false", auth)
		return
	}

	psub, sub, err := RegisterPSub(cancel, key, token, shared, conn)
	if err != nil {
		cancel()
		SendTiming(ctx, httpRequest, "psub", r.Method, "false", auth)
		return
	}

	if !CONFIRM {
		conn.SetPongHandler(sub.PongHandler)
		go psub.ReadLoop(sub)
	}

	psubIface := &psubConn{sub: sub, psub: psub}
	conn.SetCloseHandler(
		closeWS(ctx, sub.Mutex, psubIface, conn, "psub"),
	)

	for {
		rep, subErr := sub.Get(ctx)
		if subErr != nil {
			SendTiming(ctx, httpRequest, "psub", r.Method, "false", auth)
			return
		} else if !psub.WriteToSub(sub, rep) {
			psub.Redirect(sub, rep)
			SendTiming(ctx, httpRequest, "psub", r.Method, "false", auth)
			return
		}
	}
}

// HandleSub -- GET /sub/:sub
func HandleSub(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)
	auth := strconv.FormatBool(token != "")

	if key == "" {
		logWrite(w, http.StatusNotFound, []byte("Not found"))
		SendTiming(ctx, httpRequest, "sub", r.Method, "false", auth)
		return
	}

	// start a child cancel context
	ctx, cancel := context.WithCancel(ctx)

	// create/register the sub
	sub, err := RegisterSub(cancel, key, token)

	// check for validity
	if err != nil {
		logWrite(
			w,
			http.StatusForbidden,
			[]byte(err.Error()),
		)
		SendTiming(ctx, httpRequest, "sub", r.Method, "false", auth)
		return
	}

	// we could be waiting here a while
	rep, err := sub.Get(ctx)

	// could be canceled, could be done. either way clean up if we're the owner
	sub.Deregister()

	if err != nil {
		logWrite(
			w,
			http.StatusInternalServerError,
			[]byte(fmt.Sprintf("Failed to fullfil sub: %+v", err.Error())),
		)
		SendTiming(ctx, httpRequest, "sub", r.Method, "false", auth)
		return
	}

	// write the body, we're done
	logWrite(w, http.StatusOK, rep.data)

	// send our metric
	SendTiming(ctx, httpRequest, "sub", r.Method, "true", auth)

}

// HandlePRep -- GET /prep/ -- websocket reps
func HandlePRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	ctx, cancel := context.WithCancel(ctx)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		cancel()
		SendTiming(ctx, httpRequest, "prep", r.Method)
		return
	}

	pRep := RegisterPRep(cancel, conn)

	go pRep.ReadLoop()

	conn.SetCloseHandler(
		closeWS(ctx, pRep.WriteMutex, pRepConn{pRep}, conn, "prep"),
	)

	for {
		rep, wsErr := pRep.Read(ctx)
		if wsErr != nil {
			// invalid format or disconnect, kill the client
			pRep.Deregister()
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

		SendHistogram(
			httpRequest,
			time.Since(repTime).Seconds(),
			"prep",
			r.Method,
			strconv.FormatBool(repErr.code == 0),
			strconv.FormatBool(rep.token != ""),
			strconv.FormatBool(confirmed),
		)

	}
}

// HandleRep -- POST /rep/:sub
func HandleRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	// fetch our chain-created sub key
	key := ctx.Value(keyKey).(string)
	token := ctx.Value(keyToken).(string)
	start := ctx.Value(keyStart).(time.Time)
	auth := strconv.FormatBool(token != "")

	// optionally specify replying to a psub, minor performance gain
	psubRep := r.URL.Query().Get("psub") == "1"

	// read and receive the rep, auth and existence checks are here
	repError := receiveRep(key, token, psubRep, postRep{r}, start)
	if repError.code > 0 {
		logWrite(w, repError.code, repError.message)
		SendTiming(ctx, httpRequest, "rep", r.Method, "false", auth)
		return
	}

	// write our basic response
	logWrite(w, http.StatusOK, []byte("OK"))

	// send our timing metric
	SendTiming(ctx, httpRequest, "rep", r.Method, "true", auth)
}

// HandleKeys -- GET /keys
func HandleKeys(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	// TODO: add optional token auth to this route

	// fetch the current list of local subs
	subs := listSubs()
	subs = append(subs, listPSubs()...)

	if subs == nil {
		// no active subs, still a success
		logWrite(w, http.StatusNoContent, nil)
		SendTiming(ctx, httpRequest, "keys", r.Method, "true")
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
		SendTiming(ctx, httpRequest, "keys", r.Method, "false")
		return
	}

	// write the response
	w.Header().Set("Content-Type", "application/json")
	logWrite(w, http.StatusOK, jsonKeys)
	SendTiming(ctx, httpRequest, "keys", r.Method, "true")
}

func closeWS(
	ctx context.Context,
	m sync.Locker,
	w wsConn,
	conn *websocket.Conn,
	route string,
) func(code int, text string) error {
	return func(code int, text string) error {
		SendTiming(ctx, httpRequest, route, http.MethodGet, "false")

		w.Deregister()
		m.Lock()
		if connClosed := closeConn(conn, code); connClosed != nil {
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
	psub *PSub
	sub  *Sub
}

func (p psubConn) Cancel() {
	p.sub.Cancel()
}

func (p psubConn) Deregister() {
	p.psub.Deregister(p.sub)
}

type pRepConn struct {
	pRep *PRep
}

func (p pRepConn) Cancel() {
	p.pRep.Cancel()
}

func (p pRepConn) Deregister() {
	p.pRep.Deregister()
}
