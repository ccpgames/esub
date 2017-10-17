package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/twinj/uuid"
)

// CONFIRM -- if we're using read receipts in psubs
var CONFIRM = os.Getenv("ESUB_CONFIRM_RECEIPT") != "" &&
	os.Getenv("ESUB_CONFIRM_RECEIPT") != "0"

func subKeys(m map[string]Sub) (keys []string) {
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// SubReply -- temporary storage for the reply data and timestamp
type SubReply struct {
	data    []byte
	repTime time.Time
}

func psubKeys(m map[string][]Sub) (keys []string) {
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

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
	RepChan chan SubReply

	// Channel of our validity (if we overwrite w/ incorrect key)
	Valid chan bool
	// PSub keys:
	// websocket connection to this client
	Websocket *websocket.Conn
	// if this PSub can be shared with other readers
	Shared bool

	// good thing this isn't a part of any underlying library...
	Mutex *sync.Mutex
}

type subQueryStruct struct {
	Key     string
	psub    bool
	channel chan Sub
}

type psubQueryStruct struct {
	Key     string
	ID      string
	channel chan Sub
}

type psubLastRepStruct struct {
	psub    Sub
	lastRep chan SubReply
}

type psubValidityStruct struct {
	key, token string
	shared     bool
	valid      chan bool
}

type psubLastRepUpdateStruct struct {
	psub Sub
	rep  SubReply
}

type psubPingStruct struct {
	key string
	id  string
}

var subList map[string]Sub
var psubList map[string][]Sub
var psubIndex map[string]int
var psubLastReps map[string]SubReply
var psubPings map[string]map[string]time.Time

var subChan chan Sub
var psubChan chan Sub
var subQueryChan chan subQueryStruct
var psubQueryChan chan psubQueryStruct
var subDeleteChan chan Sub
var psubDeleteChan chan Sub
var subCountChan chan chan float64
var subListChan chan chan []string
var psubListChan chan chan []Sub
var psubLastRepChan chan psubLastRepStruct
var psubLastRepUpdateChan chan psubLastRepUpdateStruct
var psubValidityChan chan psubValidityStruct
var psubPingChan chan psubPingStruct
var psubPingListChan chan chan map[string]map[string]time.Time

// Initialize -- starts the sub channels dispatcher
func Initialize(ctx context.Context) {
	subList = make(map[string]Sub)
	psubList = make(map[string][]Sub)
	psubIndex = make(map[string]int)
	psubLastReps = make(map[string]SubReply)
	psubPings = make(map[string]map[string]time.Time)

	subChan = make(chan Sub)
	psubChan = make(chan Sub)
	subQueryChan = make(chan subQueryStruct)
	psubQueryChan = make(chan psubQueryStruct)
	subDeleteChan = make(chan Sub, 100)
	psubDeleteChan = make(chan Sub, 100)
	subCountChan = make(chan chan float64, 100)
	subListChan = make(chan chan []string, 100)
	psubListChan = make(chan chan []Sub, 100)
	psubLastRepChan = make(chan psubLastRepStruct, 100)
	psubLastRepUpdateChan = make(chan psubLastRepUpdateStruct, 100)
	psubValidityChan = make(chan psubValidityStruct, 100)
	psubPingChan = make(chan psubPingStruct)
	psubPingListChan = make(chan chan map[string]map[string]time.Time)

	go goSubWriter()
	go goPSubWriter()
	go goPingWriter()

	if !CONFIRM {
		go goTimeoutPSubs(ctx)
	}
}

func validPSub(key, token string, shared bool) bool {
	val, found := psubList[key]
	if found {
		for _, sub := range val {
			if sub.Auth != token || sub.Shared != shared {
				return false
			}
		}
		return true
	}
	return true
}

func addPSub(sub Sub) {
	val, found := psubList[sub.Key]
	if found && len(val) > 0 {
		if val[0].Auth != "" && val[0].Auth != sub.Auth {
			sub.Valid <- false
			return
		}
		if sub.Shared {
			psubList[sub.Key] = append(val, sub)
		} else {
			for i := 0; i < len(val); i++ {
				val[i].Cancel()
			}
			psubList[sub.Key] = []Sub{sub}
			psubIndex[sub.Key] = 0
		}
	} else {
		psubList[sub.Key] = []Sub{sub}
		psubIndex[sub.Key] = 0
	}
	sub.Valid <- true
}

func addSub(sub Sub) {
	val, found := subList[sub.Key]
	// cancel the previous sub on overwrite
	if found {
		if val.Auth != "" && val.Auth != sub.Auth {
			// if the known sub has auth it must match, otherwise
			// the DOS path is too obvious and unprotected. if you
			// want to cycle your tokens, cycle your key at the same
			// time. esubs ideally use two uuids for key and token
			sub.Valid <- false
			return
		}
		val.Cancel()
	}
	subList[sub.Key] = sub
	sub.Valid <- true
}

func countSubs() float64 {
	subs := len(subList)
	for _, v := range psubList {
		subs += len(v)
	}
	return float64(subs)
}

func deleteSub(sub Sub) {
	val, found := subList[sub.Key]
	if found && val.ID.String() == sub.ID.String() {
		delete(subList, sub.Key)
	}
}

func deletePSub(sub Sub) {
	psubs, found := psubList[sub.Key]
	if found {
		for i := 0; i < len(psubs); i++ {
			psub := psubs[i]
			if psub.ID.String() == sub.ID.String() {
				psubList[sub.Key] = append(
					psubList[sub.Key][:i],
					psubList[sub.Key][i+1:]...,
				)
				delete(psubPings[sub.Key], sub.ID.String())
				psub.Cancel()
				break
			}
			if len(psubList[sub.Key]) == 0 {
				delete(psubList, sub.Key)
				delete(psubIndex, sub.Key)
				delete(psubPings, sub.Key)
			} else if psubIndex[sub.Key] >= len(psubList[sub.Key]) {
				psubIndex[sub.Key] = 0
			}
		}
	}
}

func listSubs() (keys []string) {
	keys = subKeys(subList)
	keys = append(keys, psubKeys(psubList)...)
	return keys
}

func listPSubs() (psubs []Sub) {
	for _, v := range psubList {
		psubs = append(psubs, v...)
	}
	return psubs
}

func updatePsubPing(key, id string) {
	subPings, found := psubPings[key]
	if found {
		subPings[id] = time.Now().UTC()
	} else {
		psubPings[key] = map[string]time.Time{id: time.Now().UTC()}
	}
}

// manage Sub channels
func goSubWriter() {
	for {
		select {

		case cn := <-subChan:
			addSub(cn)

		case cq := <-subQueryChan:
			cq.channel <- queryForSub(cq)

		case cc := <-subCountChan:
			cc <- countSubs()

		case cd := <-subDeleteChan:
			deleteSub(cd)

		case cl := <-subListChan:
			cl <- listSubs()

		}
	}
}

// manage PSub channels
func goPSubWriter() {
	for {
		select {

		case pn := <-psubChan:
			addPSub(pn)

		case pq := <-psubQueryChan:
			pq.channel <- queryForSpecificPSub(pq)

		case pd := <-psubDeleteChan:
			deletePSub(pd)

		case pl := <-psubListChan:
			pl <- listPSubs()

		case ru := <-psubLastRepUpdateChan:
			psubLastReps[ru.psub.ID.String()] = ru.rep

		case lr := <-psubLastRepChan:
			lr.lastRep <- psubLastReps[lr.psub.ID.String()]

		case pv := <-psubValidityChan:
			pv.valid <- validPSub(pv.key, pv.token, pv.shared)

		}
	}
}

// manage ping channels
func goPingWriter() {
	for {
		select {

		case pp := <-psubPingChan:
			updatePsubPing(pp.key, pp.id)

		case ppl := <-psubPingListChan:
			ppl <- psubPings

		}
	}
}

func queryForSub(q subQueryStruct) Sub {
	if !q.psub {
		val, found := subList[q.Key]
		if found {
			return val
		}
	}
	return queryForPSub(q)
}

func queryForPSub(q subQueryStruct) Sub {
	psubs, found := psubList[q.Key]
	if found && len(psubs) > 0 {
		index := psubIndex[q.Key]
		if index >= len(psubs) {
			index = 0
		}
		sub := psubs[index]
		index++
		psubIndex[q.Key] = index
		return sub
	}
	return Sub{}
}

func queryForSpecificPSub(q psubQueryStruct) Sub {
	psubs, found := psubList[q.Key]
	if found {
		for _, sub := range psubs {
			if sub.Key == q.Key && sub.ID.String() == q.ID {
				return sub
			}
		}
	}
	return Sub{}
}

// RegisterSub -- create and register a new Sub object
func RegisterSub(
	cancel context.CancelFunc,
	key string,
	token string,
) Sub {
	sub := Sub{
		Key:     key,
		Auth:    token,
		ID:      uuid.NewV4(),
		RepChan: make(chan SubReply),
		Cancel:  cancel,
		Valid:   make(chan bool),
	}
	subChan <- sub
	return sub
}

// RegisterPSub -- create and register a new persistent Sub object
func RegisterPSub(
	cancel context.CancelFunc,
	key string,
	token string,
	shared bool,
	conn *websocket.Conn,
) Sub {
	sub := Sub{
		Key:       key,
		Auth:      token,
		ID:        uuid.NewV4(),
		RepChan:   make(chan SubReply),
		Cancel:    cancel,
		Websocket: conn,
		Shared:    shared,
		Valid:     make(chan bool),
		Mutex:     &sync.Mutex{},
	}
	if !CONFIRM {
		updateKeepalive(sub.Key, sub.ID.String())
	}
	psubChan <- sub
	return sub
}

// DeregisterPSub -- removes a sub from the psub list
func DeregisterPSub(sub Sub) {
	psubDeleteChan <- sub
}

// SpecificPSub -- return a psub by uuid, do not increase the index
func SpecificPSub(ctx context.Context, key, id string) (Sub, error) {
	reply := make(chan Sub)

	psubQueryChan <- psubQueryStruct{key, id, reply}

	for {
		select {
		case <-ctx.Done():
			return Sub{}, ctx.Err()

		case sub := <-reply:
			return sub, nil
		}
	}
}

// SubQuery -- query for the Sub
func SubQuery(key string, psub bool) Sub {
	reply := make(chan Sub)
	subQueryChan <- subQueryStruct{key, psub, reply}
	return <-reply
}

// GetLastRep returns the last rep for the psub
func GetLastRep(psub Sub) SubReply {
	reply := psubLastRepStruct{psub, make(chan SubReply)}
	psubLastRepChan <- reply
	return <-reply.lastRep
}

// UpdateLastRep stores the rep as the psub's last rep
func UpdateLastRep(psub Sub, reply SubReply) {
	lastRepUpdate := psubLastRepUpdateStruct{psub, reply}
	psubLastRepUpdateChan <- lastRepUpdate
}

// SubCount -- returns a count of active Sub clients
func SubCount() float64 {
	reply := make(chan float64)
	subCountChan <- reply
	return <-reply
}

// SubList -- returns a list of active Sub keys
func SubList() []string {
	reply := make(chan []string)
	subListChan <- reply
	return <-reply
}

// Get -- make a Sub wait for the Rep channel
func (s Sub) Get(ctx context.Context) (SubReply, error) {
	for {
		select {
		case <-ctx.Done():
			return SubReply{}, ctx.Err()
		case rep := <-s.RepChan:
			return rep, nil
		}
	}
}

func updateKeepalive(key, id string) {
	psubPingChan <- psubPingStruct{key, id}
}

// PongHandler accepts psub keepalive pongs
func (s Sub) PongHandler(_ string) error {
	updateKeepalive(s.Key, s.ID.String())
	return nil
}

// ReadLoop will process PONG replies by being the sole reader
// ReadLoop is only used when read reciepts are not used
func (s Sub) ReadLoop() {
	for {
		if _, _, err := s.Websocket.NextReader(); err != nil {
			DeregisterPSub(s)
			s.Cancel()
			break
		}
	}
}

// ValidPSub -- check if the key/token is valid for a new psub
func ValidPSub(key, token string, shared bool) bool {
	pv := psubValidityStruct{key, token, shared, make(chan bool)}
	psubValidityChan <- pv
	return <-pv.valid
}

// WriteToPSub -- send a message to a Sub's websocket
func WriteToPSub(conn *websocket.Conn, reply SubReply, sub Sub) (success bool) {
	sub.Mutex.Lock()

	err := conn.WriteMessage(websocket.TextMessage, reply.data)
	if err != nil {
		sub.Mutex.Unlock()
		return false
	}

	// You can request a read receipt with each message.
	// you /may/ want to do this, for low traffic implementations.
	// higher volume setups may wish to disable this.
	// This setting /must/ be consistent on all psubs and the server.
	//
	// this has side effects for shared psubs only:
	//
	// Enabled:
	// - each message will redirected as soon as it fails to write to a sub
	// Disabled:
	// - the "last successful" rep is saved per psub. the 2nd write to a failed
	//   sub will trigger the redirect to the next shared sub, where it will
	//   receive both failed messages
	//
	// in both cases, no messages are ever dropped by esub.

	if CONFIRM {
		setReadErr := conn.SetReadDeadline(time.Now().UTC().Add(1 * time.Second))

		if setReadErr != nil {
			sub.Mutex.Unlock()
			return false
		}

		_, _, err := conn.ReadMessage()
		if err != nil {
			sub.Mutex.Unlock()
			return false
		}
	} else {
		UpdateLastRep(sub, reply)
	}

	sub.Mutex.Unlock()

	// send a write metric
	SendMetric(
		Metric{
			Name:      "psub_reply",
			Route:     "sub",
			Key:       sub.Key,
			Auth:      sub.Auth != "",
			Success:   true,
			Confirmed: CONFIRM,
		},
		1,
		reply.repTime,
	)

	return true
}

// RedirectPSub -- send the rep to the next sub in the psub
// returns a boolean of the message(s) being successfully redirected
func RedirectPSub(sub Sub, reply SubReply) bool {
	if !sub.Shared {
		return false
	}
	return redirectPSub(sub, reply)
}

// redirectPSub -- redirect until a shared psub receives the rep
func redirectPSub(sub Sub, replies ...SubReply) bool {
	lastRep := GetLastRep(sub)
	if !lastRep.repTime.IsZero() {
		// without confirming receipt with a read from the client,
		// we can only tell the ws connection is dead after the second
		// unsuccessful write. so we add our last "successful" rep to
		// the array of messages that should be redirected here. this
		// ensures no messages are ever dropped unless all members die
		//
		// if read reciepts are in use, lastRep is nil
		replies = append(replies, lastRep)
	}

	psub := SubQuery(sub.Key, true)
	if psub.ID == nil {
		log.Printf("shared psub '%s' has no connections, dropping %d message(s)",
			sub.Key, len(replies))
		return false
	}

	for i := 0; i < len(replies); i++ {
		if !WriteToPSub(psub.Websocket, replies[i], psub) {
			DeregisterPSub(psub)
			return redirectPSub(psub, replies[i:]...)
		}
	}

	return true
}

// ensures that we have recent pings from all connected psubs
// used only if not using read reciepts
func goTimeoutPSubs(ctx context.Context) {
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
		now := time.Now().UTC()
		reply := make(chan map[string]map[string]time.Time)
		psubPingListChan <- reply
		pings := <-reply

		for subKey, subs := range pings {
			for subID, lastPing := range subs {
				if now.After(lastPing.Add(pingTime)) {

					sub, err := SpecificPSub(ctx, subKey, subID)
					if err != nil || sub.Key == "" {
						return
					}
					log.Printf("psub %s for %s has timed out", sub.ID, sub.Key)
					DeregisterPSub(sub)
					sub.Cancel()
				}
			}
		}
		time.Sleep(pingTime)
	}
}
