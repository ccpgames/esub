package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/twinj/uuid"

	"github.com/gorilla/websocket"
)

type repReader interface {
	Read() ([]byte, error)
}

type postRep struct {
	req *http.Request
}

type wsRep struct {
	data string
}

func (r wsRep) Read() ([]byte, error) {
	return []byte(r.data), nil
}

func (r postRep) Read() ([]byte, error) {
	return ioutil.ReadAll(r.req.Body)
}

type replyError struct {
	code    int
	message []byte
}

// PRep -- per connected pRep client
type PRep struct {
	ID         uuid.UUID
	Cancel     context.CancelFunc
	Websocket  *websocket.Conn
	ReadMutex  *sync.Mutex
	WriteMutex *sync.Mutex
	RepChan    chan Rep
}

// Rep -- per read rep message
type Rep struct {
	key   string
	token string
	psub  bool
	rep   wsRep
}

// WebsocketRep -- the format pRep messages must conform to
type WebsocketRep struct {
	Key   string `json:"key"`
	Token string `json:"token"`
	PSub  bool   `json:"psub"`
	Data  string `json:"data"`
}

type pRepList struct {
	result chan float64
}

type pRepQuery struct {
	query  string
	result chan float64
}

var pReps map[string]time.Time
var pRepAddChan chan PRep
var pRepDeleteChan chan PRep
var pRepListChan chan pRepList
var pRepQueryChan chan pRepQuery

// PRepInitialize starts the channels for pRep support
func PRepInitialize() {
	pReps = make(map[string]time.Time)
	pRepAddChan = make(chan PRep, 100)
	pRepDeleteChan = make(chan PRep, 100)
	pRepListChan = make(chan pRepList, 100)
	pRepQueryChan = make(chan pRepQuery, 100)

	go goPRepWriter()
}

// channels to add, remove and list pRep IDs
func goPRepWriter() {
	for {
		select {

		case ra := <-pRepAddChan:
			pReps[ra.ID.String()] = time.Now().UTC()

		case rd := <-pRepDeleteChan:
			delete(pReps, rd.ID.String())

		case rl := <-pRepListChan:
			rl.result <- float64(len(pReps))

		case rq := <-pRepQueryChan:
			rq.result <- time.Since(pReps[rq.query]).Seconds()

		}
	}
}

func pRepCount() float64 {
	query := pRepList{result: make(chan float64)}
	pRepListChan <- query
	return <-query.result
}

// RegisterPRep -- create and register a new persistent Rep object
func RegisterPRep(cancel context.CancelFunc, conn *websocket.Conn) PRep {
	pRep := PRep{
		ID:         uuid.NewV4(),
		Cancel:     cancel,
		Websocket:  conn,
		ReadMutex:  &sync.Mutex{},
		WriteMutex: &sync.Mutex{},
		RepChan:    make(chan Rep),
	}

	pRepAddChan <- pRep
	return pRep
}

// DeregisterPRep -- removes a pRep from tracking
func DeregisterPRep(p PRep) {
	p.Cancel()
	pRepDeleteChan <- p
}

// ReadLoop -- goroutine to forever read from the websocket
func (p PRep) ReadLoop() {
	for {
		p.ReadMutex.Lock()
		_, msg, err := p.Websocket.ReadMessage()
		p.ReadMutex.Unlock()

		if err != nil {
			p.Cancel()
			return
		}

		var rep WebsocketRep
		jsonErr := json.Unmarshal(msg, &rep)
		if jsonErr != nil {
			p.Cancel()
			return
		}

		p.RepChan <- Rep{rep.Key, rep.Token, rep.PSub, wsRep{rep.Data}}
	}
}

// Read -- wait to read a new message from the pRep client
func (p PRep) Read(ctx context.Context) (Rep, error) {
	for {
		select {
		case <-ctx.Done():
			return Rep{}, ctx.Err()
		case r := <-p.RepChan:
			return r, nil
		}
	}
}

// Confirm -- sends a confirmation message based on the rep code
func (p PRep) Confirm(r replyError) bool {
	p.WriteMutex.Lock()
	var data []byte
	if r.code > 0 {
		data = r.message
	} else {
		// note that when confirm is true, every operation is blocking.
		// because of this, we don't need to include the message id.
		// but we easily could reply with the pRep.UUID here, or some
		// other supplied ID field... /shrug
		data = []byte("ok")
	}

	err := p.Websocket.WriteMessage(websocket.TextMessage, data)
	p.WriteMutex.Unlock()

	return err == nil
}

// DRY rep receiving for /rep/:rep and/or /prep
func receiveRep(
	key string,
	token string,
	psubRep bool,
	reader repReader,
	start time.Time,
) replyError {

	// fetch the sub object,
	sub := SubQuery(key, psubRep)

	// check the key is known to us
	if sub.Key != key || key == "" {
		return replyError{404, []byte("Unknown key")}
	}

	// late auth check
	if sub.Auth != token {
		return replyError{403, []byte("Invalid token")}
	}

	// read the body into memory
	body, err := reader.Read()
	if err != nil {
		return replyError{500, []byte(fmt.Sprintf("Failed to read body: %+v", err))}
	}

	subReply := SubReply{body, start}

	if sub.Websocket != nil {
		// sub is a psub
		if !WriteToPSub(sub.Websocket, subReply, sub) {
			DeregisterPSub(sub)
			if !RedirectPSub(sub, subReply) {
				return replyError{404, []byte("No listeners remain")}
			}
		}
	} else {
		// fulfill the sub
		sub.RepChan <- subReply
	}

	return replyError{}
}
