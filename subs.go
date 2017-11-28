package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/twinj/uuid"
)

// Sub -- per connected client
type Sub struct {
	// Key is our Sub key, used in the paths
	Key string
	// Auth is the token, or empty string, has to match either way
	Auth string
	// ID is a UUID4 to allow Key overwrites/cancels by other threads
	ID uuid.UUID
	// Context CancelFunc for overwriters to call to kill us
	Cancel context.CancelFunc
	// Channel to receive the Rep into
	RepChan chan *SubReply

	// PSub keys:
	// websocket connection to this client
	Websocket *websocket.Conn
	// if this PSub can be shared with other readers
	Shared bool
	// for writing to the websocket, since it doesn't...
	Mutex *sync.Mutex
	// track our last ping time, if confirm is false
	LastPing *time.Time
	// track our last received rep, for failovers
	LastRep *SubReply
}

// SubReply -- temporary storage for the reply data and timestamp
type SubReply struct {
	data    []byte
	repTime time.Time
}

// SubList keeps a list of all connected subs
type SubList struct {
	Mutex *sync.Mutex
	Subs  map[string]*Sub // Sub.Key: Sub
}

var subList = SubList{Mutex: &sync.Mutex{}, Subs: map[string]*Sub{}}

// RegisterSub -- add a new sub
func RegisterSub(cancel context.CancelFunc, key, token string) (*Sub, error) {

	subList.Mutex.Lock()
	defer subList.Mutex.Unlock()

	val, found := subList.Subs[key]
	if found {
		if val.Auth != "" && val.Auth != token {
			return nil, errors.New("Sub already exists, token mismatch")
		}
		val.Cancel()
	}

	sub := &Sub{
		Key:     key,
		Auth:    token,
		ID:      uuid.NewV4(),
		RepChan: make(chan *SubReply),
		Cancel:  cancel,
	}

	subList.Subs[key] = sub

	return sub, nil
}

// Deregister -- remove this sub from sub tracking
func (s *Sub) Deregister() {
	subList.Mutex.Lock()
	delete(subList.Subs, s.Key)
	subList.Mutex.Unlock()
}

// SubQuery -- main query for either sub or psubs
func SubQuery(key string, psub bool) (*PSub, *Sub) {
	if psub {
		return PSubQuery(key)
	}

	subList.Mutex.Lock()
	sub, found := subList.Subs[key]
	subList.Mutex.Unlock()

	if found {
		return nil, sub
	}

	return PSubQuery(key)
}

func listSubs() (keys []string) {
	subList.Mutex.Lock()
	for k := range subList.Subs {
		keys = append(keys, k)
	}
	subList.Mutex.Unlock()
	return keys
}

// SubCount -- returns a count of active Sub clients
func SubCount() float64 {
	subList.Mutex.Lock()
	subs := float64(len(subList.Subs))
	subList.Mutex.Unlock()
	return subs
}

// PongHandler accepts psub keepalive pongs
func (s *Sub) PongHandler(_ string) error {
	s.Mutex.Lock()
	now := time.Now().UTC()
	s.LastPing = &now
	s.Mutex.Unlock()
	return nil
}

// Get -- make a Sub wait for the Rep channel
func (s *Sub) Get(ctx context.Context) (*SubReply, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case rep := <-s.RepChan:
		return rep, nil
	}
}
