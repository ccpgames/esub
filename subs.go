package main

import (
	"context"
	"time"

	"github.com/twinj/uuid"
)

// ZeroTime -- because golang
var ZeroTime = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)

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
	// Bytes object stored after we receive our Rep
	Rep []byte
	// Time of receiving the Rep
	RepTime time.Time
	// Channel to receive the Rep into
	RepChan chan SubReply
}

type subChanStruct struct {
	Sub Sub
}

type subQueryStruct struct {
	Key     string
	channel chan Sub
}

type subDeleteStruct struct {
	Sub Sub
}

var subList map[string]Sub
var subChan chan subChanStruct
var subQueryChan chan subQueryStruct
var subDeleteChan chan subDeleteStruct
var subCountChan chan chan float64
var subListChan chan chan []string

// Initialize -- starts the sub channels dispatcher
func Initialize() {
	subList = make(map[string]Sub)
	subChan = make(chan subChanStruct)
	subQueryChan = make(chan subQueryStruct)
	subDeleteChan = make(chan subDeleteStruct, 100)
	subCountChan = make(chan chan float64, 100)
	subListChan = make(chan chan []string, 100)
	go goSubWriter()
}

// manage Sub channels
func goSubWriter() {
	for {
		select {

		case cn := <-subChan:
			val, found := subList[cn.Sub.Key]
			// cancel the previous sub on overwrite
			if found {
				val.Cancel()
			}
			subList[cn.Sub.Key] = cn.Sub

		case cq := <-subQueryChan:
			cq.channel <- queryForSub(cq)

		case cc := <-subCountChan:
			cc <- float64(len(subList))

		case cd := <-subDeleteChan:
			val, found := subList[cd.Sub.Key]
			if found && val.ID.String() == cd.Sub.ID.String() {
				delete(subList, cd.Sub.Key)
			}

		case cl := <-subListChan:
			cl <- subKeys(subList)

		}
	}
}

func queryForSub(q subQueryStruct) Sub {
	val, found := subList[q.Key]
	if found {
		return val
	}
	return Sub{"", "", nil, nil, nil, ZeroTime, nil}
}

// RegisterSub -- create and register a new Sub object
func RegisterSub(cancel context.CancelFunc, key string, token string) Sub {
	sub := Sub{
		Key:     key,
		Auth:    token,
		ID:      uuid.NewV4(),
		RepChan: make(chan SubReply),
		Cancel:  cancel,
	}
	subChan <- subChanStruct{sub}
	return sub
}

// SubQuery -- uses the key from ctx to query for the Sub
func SubQuery(ctx context.Context, key string) (Sub, error) {
	reply := make(chan Sub)
	subQueryChan <- subQueryStruct{key, reply}

	for {
		select {
		case <-ctx.Done():
			return Sub{"", "", nil, nil, nil, ZeroTime, nil}, ctx.Err()

		case sub := <-reply:
			return sub, nil
		}
	}
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
func (s Sub) Get(ctx context.Context) (Sub, error) {
	for {
		select {
		case <-ctx.Done():
			return s, ctx.Err()
		case rep := <-s.RepChan:
			s.Rep = rep.data
			s.RepTime = rep.repTime
			return s, nil
		}
	}
}
