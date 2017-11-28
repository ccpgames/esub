package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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

// PReps stores a list of all connected PRep clients
type PReps struct {
	Mutex *sync.Mutex
	PReps map[string]*PRep // PRep.ID: PRep
}

var pReps = PReps{Mutex: &sync.Mutex{}, PReps: map[string]*PRep{}}

// PRepCount -- return a count of the number of connected PRep clients
func PRepCount() float64 {
	pReps.Mutex.Lock()
	count := len(pReps.PReps)
	pReps.Mutex.Unlock()
	return float64(count)
}

// RegisterPRep -- create and register a new persistent Rep object
func RegisterPRep(cancel context.CancelFunc, conn *websocket.Conn) *PRep {
	pRep := &PRep{
		ID:         uuid.NewV4(),
		Cancel:     cancel,
		Websocket:  conn,
		ReadMutex:  &sync.Mutex{},
		WriteMutex: &sync.Mutex{},
		RepChan:    make(chan Rep),
	}

	pReps.Mutex.Lock()
	pReps.PReps[pRep.ID.String()] = pRep
	pReps.Mutex.Unlock()

	return pRep
}

// Deregister -- removes a pRep from tracking
func (p *PRep) Deregister() {
	p.Cancel()
	pReps.Mutex.Lock()
	delete(pReps.PReps, p.ID.String())
	pReps.Mutex.Unlock()
	if err := p.Websocket.Close(); err != nil {
		log.Printf("Error closing prep: %s", err.Error())
	}
}

// ReadLoop -- goroutine to forever read from the websocket
func (p *PRep) ReadLoop() {
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
	psub, sub := SubQuery(key, psubRep)

	// check the key is known to us
	if sub == nil {
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

	subReply := &SubReply{body, start}

	if psub != nil {
		// sub is a psub
		if !psub.WriteToSub(sub, subReply) {
			psub.Deregister(sub)
			if !psub.Redirect(sub, subReply) {
				return replyError{404, []byte("No listeners remain")}
			}
		}
	} else {
		// fulfill the sub
		sub.RepChan <- subReply
	}

	return replyError{}
}
