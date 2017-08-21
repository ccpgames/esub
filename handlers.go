package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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

	conn.SetCloseHandler(func(code int, text string) error {
		shipMetric(ctx, false)
		DeregisterPSub(sub)
		if connClosed := closeConn(conn, code, ""); connClosed != nil {
			// one way or another, be sure to close the socket
			if closeErr := conn.Close(); closeErr != nil {
				log.Printf("failed to close socket: %+v: %+v", connClosed, closeErr)
			}
		}
		sub.Cancel()
		return nil
	})

	for {
		rep, subErr := sub.Get(ctx)
		if subErr != nil {
			DeregisterPSub(sub)
			return
		} else if !WriteToPSub(conn, rep, sub) {
			DeregisterPSub(sub)
			RedirectPSub(ctx, sub, rep)
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

// HandleRep -- POST /rep/:sub
func HandleRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	// fetch our chain-created sub key
	key := ctx.Value(keyKey).(string)

	// optionally specify replying to a psub, minor performance gain
	psubRep := r.URL.Query().Get("psub") == "1"

	// fetch the sub object, can be null
	sub, err := SubQuery(ctx, key, psubRep)

	// canceled request
	if err != nil {
		shipMetric(ctx, false)
		return
	}

	// check the key is known to us
	if sub.Key != key || key == "" {
		logWrite(w, http.StatusNotFound, []byte("Unknown key"))
		shipMetric(ctx, false)
		return
	}

	// late auth check
	if sub.Auth != ctx.Value(keyToken) {
		logWrite(w, http.StatusForbidden, []byte("Invalid token"))
		shipMetric(ctx, false)
		return
	}

	// read the body into memory
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logWrite(
			w,
			http.StatusInternalServerError,
			[]byte(fmt.Sprintf("Failed to read body: %+v", err)),
		)
		shipMetric(ctx, false)
		return
	}

	subReply := SubReply{body, ctx.Value(keyStart).(time.Time)}

	if sub.Websocket != nil {
		// sub is a psub
		if !WriteToPSub(sub.Websocket, subReply, sub) {
			DeregisterPSub(sub)
			if !RedirectPSub(ctx, sub, subReply) {
				shipMetric(ctx, false)
				logWrite(w, http.StatusNotFound, []byte("No listeners remain"))
				return
			}
		}
	} else {
		// fulfill the sub
		sub.RepChan <- subReply
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
