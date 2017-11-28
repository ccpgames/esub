package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/twinj/uuid"
)

// PSub -- describes potentially shared psub groups
type PSub struct {
	Mutex *sync.Mutex
	// Index for shared psub round robin delivery
	Index   int
	Subs    map[string]*Sub // Sub.ID: Sub
	SubList []string        // Sub.ID, for indexing Subs
}

// PSubList keeps a list of all connected psubs
type PSubList struct {
	Mutex *sync.Mutex
	PSubs map[string]*PSub // Sub.Key: PSub
}

var psubList = PSubList{Mutex: &sync.Mutex{}, PSubs: map[string]*PSub{}}

// CONFIRM -- if we're using read receipts in psubs
var CONFIRM = os.Getenv("ESUB_CONFIRM_RECEIPT") != "" &&
	os.Getenv("ESUB_CONFIRM_RECEIPT") != "0"

// RegisterPSub -- add a new psub
func RegisterPSub(
	cancel context.CancelFunc,
	key, token string,
	shared bool,
	conn *websocket.Conn,
) (*PSub, *Sub, error) {

	psubList.Mutex.Lock()
	defer psubList.Mutex.Unlock()

	psub, found := psubList.PSubs[key]
	if found {
		psub.Mutex.Lock()
		defer psub.Mutex.Unlock()
		for _, sub := range psub.Subs {
			if sub.Auth != "" && sub.Auth != token {
				return nil, nil, errors.New("psub already exists, token mismatch")
			} else if sub.Shared != shared {
				return nil, nil, errors.New("psub already exists, shared mismatch")
			}
		}
	}

	now := time.Now().UTC()
	sub := &Sub{
		Key:       key,
		Auth:      token,
		ID:        uuid.NewV4(),
		RepChan:   make(chan *SubReply),
		Cancel:    cancel,
		Shared:    shared,
		Websocket: conn,
		Mutex:     &sync.Mutex{},
		LastPing:  &now,
	}

	if found {
		if shared {
			psub.Subs[sub.ID.String()] = sub
			psub.SubList = append(psub.SubList, sub.ID.String())
			return psub, sub, nil
		}
		// otherwise this sub is taking over
		for _, sub := range psub.Subs {
			sub.Cancel()
		}
	}

	psubList.PSubs[key] = &PSub{
		Mutex:   &sync.Mutex{},
		Subs:    map[string]*Sub{sub.ID.String(): sub},
		SubList: []string{sub.ID.String()},
	}

	return psub, sub, nil
}

// Deregister -- removes a sub and the psub if it's now empty
func (p *PSub) Deregister(sub *Sub) {
	sub.Cancel()

	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	subID := sub.ID.String()
	delete(p.Subs, subID)

	for i, v := range p.SubList {
		if v == subID {
			p.SubList = append(p.SubList[:i], p.SubList[i+1:]...)
			break
		}
	}

	if len(p.Subs) == 0 {
		psubList.Mutex.Lock()
		delete(psubList.PSubs, sub.Key)
		psubList.Mutex.Unlock()
	} else if p.Index >= len(p.Subs) {
		p.Index = 0
	}

}

// PSubQuery -- query for a psub and a child sub based on sub key
func PSubQuery(key string) (*PSub, *Sub) {
	psubList.Mutex.Lock()
	psub, found := psubList.PSubs[key]
	psubList.Mutex.Unlock()

	if found {
		return psub, psub.SubQuery()
	}
	return nil, nil
}

// SubQuery -- return the next sub in the SubList, increase Index
func (p *PSub) SubQuery() *Sub {
	p.Mutex.Lock()

	if p.Index >= len(p.Subs) {
		p.Index = 0
	}
	sub := p.Subs[p.SubList[p.Index]]
	p.Index++

	p.Mutex.Unlock()
	return sub
}

// WriteToSub -- send a message to a Sub's websocket
func (p *PSub) WriteToSub(sub *Sub, reply *SubReply) (success bool) {
	sub.Mutex.Lock()
	defer sub.Mutex.Unlock()

	writeErr := sub.Websocket.WriteMessage(websocket.TextMessage, reply.data)
	if writeErr != nil {
		return false
	}

	if CONFIRM {
		setErr := sub.Websocket.SetReadDeadline(time.Now().UTC().Add(1 * time.Second))
		if setErr != nil {
			return false
		}

		if _, _, err := sub.Websocket.ReadMessage(); err != nil {
			return false
		}

	} else {
		sub.LastRep = reply
	}

	return true
}

// Redirect -- send the rep to the next sub in the psub
// returns a boolean of the message(s) being successfully redirected
func (p *PSub) Redirect(sub *Sub, reply *SubReply) bool {
	if !sub.Shared {
		return false
	}
	return redirectPSub(sub, reply)
}

// redirectPSub -- redirect until a shared psub receives the rep
func redirectPSub(sub *Sub, replies ...*SubReply) bool {

	if !sub.LastRep.repTime.IsZero() {
		// without confirming receipt with a read from the client,
		// we can only tell the ws connection is dead after the second
		// unsuccessful write. so we add our last "successful" rep to
		// the array of messages that should be redirected here. this
		// ensures no messages are ever dropped unless all members die
		//
		// if read reciepts are in use, lastRep is nil
		replies = append(replies, sub.LastRep)
	}

	psub, s := SubQuery(sub.Key, true)
	if psub == nil {
		log.Printf("shared psub '%s' has no connections, dropping %d message(s)",
			sub.Key, len(replies))
		return false
	}

	for i := 0; i < len(replies); i++ {
		if !psub.WriteToSub(s, replies[i]) {
			psub.Deregister(s)
			return redirectPSub(s, replies[i:]...)
		}
	}

	return true
}

func listPSubs() (keys []string) {
	psubList.Mutex.Lock()
	for k := range psubList.PSubs {
		keys = append(keys, k)
	}
	psubList.Mutex.Unlock()
	return keys
}

// PSubCount -- returns a count of active PSub Sub clients
func PSubCount() float64 {
	psubList.Mutex.Lock()
	psubs := 0
	for _, p := range psubList.PSubs {
		psubs += len(p.Subs)
	}
	psubList.Mutex.Unlock()
	return float64(psubs)
}

// ReadLoop will process PONG replies by being the sole reader
// ReadLoop is only used when read reciepts are not used
func (p *PSub) ReadLoop(s *Sub) {
	for {
		if _, _, err := s.Websocket.NextReader(); err != nil {
			break
		}
	}
}

// TimeoutPSubs -- ensures that we have recent pings from all connected psubs
// used only if not using read reciepts
func TimeoutPSubs() {
	if CONFIRM {
		return
	}

	// amount of time before considering a psub dead
	pingEnv := os.Getenv("ESUB_PING_FREQUENCY")
	pingFreq := 60
	if pingEnv != "" {
		pingFreqInt64, err := strconv.ParseInt(pingEnv, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		pingFreq = int(pingFreqInt64)
	}

	pingTime := time.Duration(pingFreq) * time.Second

	for {
		expiredSubs := expiredPSubs(pingTime)
		for psub, subs := range expiredSubs {
			for _, sub := range subs {
				psub.Deregister(sub)
			}
		}
		time.Sleep(pingTime)
	}
}

func expiredPSubs(pingTime time.Duration) map[*PSub][]*Sub {
	now := time.Now().UTC()
	psubKeys := listPSubs()

	expiredSubs := map[*PSub][]*Sub{}

	for _, psubKey := range psubKeys {
		psubList.Mutex.Lock()
		psub, found := psubList.PSubs[psubKey]
		psubList.Mutex.Unlock()

		if !found { // race condition
			continue
		}

		psub.Mutex.Lock()
		for _, sub := range psub.Subs {
			expired := false
			sub.Mutex.Lock()
			if sub.LastPing != nil && !sub.LastPing.IsZero() {
				expired = now.After(sub.LastPing.Add(pingTime))
			}
			sub.Mutex.Unlock()
			if expired {
				log.Printf("psub %s for %s has timed out", sub.ID, sub.Key)
				expiredSubs[psub] = append(expiredSubs[psub], sub)
			}
		}
		psub.Mutex.Unlock()

	}

	return expiredSubs
}
