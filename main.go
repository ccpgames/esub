// esub, phase 1 implementation:
// PING, INFO, KEYS, SUB and REP

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rs/xhandler"
	"github.com/rs/xmux"
	"golang.org/x/net/context"
)

type key int

const (
	keyKey    key = iota
	keyToken  key = iota
	keyMetric key = iota
	keyStart  key = iota
)

// this shouldn't change for our lifetime
var infoJSON = getInfoJSON()

func getInfoJSON() []byte {
	type info struct {
		Name string `json:"name"`
		IP   string `json:"ip"`
	}

	nodeIP := os.Getenv("ESUB_NODE_IP")
	if nodeIP == "" {
		nodeIP = LocalIP()
	}

	infoStruct := &info{Name: Hostname(), IP: nodeIP}
	infoStr, err := json.Marshal(infoStruct)

	if err != nil {
		log.Panic(err)
	}

	return infoStr
}

// set the status header, write the content, log errors
func logWrite(w http.ResponseWriter, status int, content []byte) {
	w.WriteHeader(status)
	if content != nil {
		_, err := w.Write(content)
		if err != nil {
			log.Println(err)
		}
	}
}

// pulls the metric struct from context, maybe sets success then sends it
func shipMetric(ctx context.Context, success bool) {
	metric := ctx.Value(keyMetric).(*Metric)
	start := ctx.Value(keyStart).(time.Time)
	metric.Success = success
	SendMetric(*metric, 1, start)
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

	// we could be waiting here a while
	sub, err := sub.Get(ctx)

	// could be canceled, could be done. either way clean up if we're the owner
	subDeleteChan <- subDeleteStruct{sub}

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
	logWrite(w, http.StatusOK, sub.Rep)

	// send our metric
	shipMetric(ctx, true)
}

// HandleRep -- POST /rep/:sub
func HandleRep(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	// fetch our chain-created sub key
	key := ctx.Value(keyKey).(string)

	// fetch the sub object, can be null
	sub, err := SubQuery(ctx, key)

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

	// fulfill the sub
	sub.RepChan <- body

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

func main() {
	c := xhandler.Chain{}

	// adds a close context to handlers for disconnects
	c.UseC(xhandler.CloseHandler)

	// optionally close all http connections
	if os.Getenv("ESUB_CLOSE_CONNECTIONS") != "" {
		c.UseC(CloseConnectionHandler)
	}

	router := xmux.New()

	router.GET("/ping", xhandler.HandlerFuncC(HandlePing))
	router.GET(
		"/info",
		MetricHandler("info", xhandler.HandlerFuncC(HandleInfo)),
	)
	router.GET(
		"/keys",
		MetricHandler("keys", xhandler.HandlerFuncC(HandleKeys)),
	)
	router.GET(
		"/sub/:sub",
		MetricHandler("sub", xhandler.HandlerFuncC(HandleSub)),
	)
	router.POST(
		"/rep/:sub",
		MetricHandler("rep", xhandler.HandlerFuncC(HandleRep)),
	)

	Initialize()
	CounterInitialize()
	go DisplayStats()

	log.Println("Listening on :8090")
	if err := http.ListenAndServe(":8090", c.Handler(router)); err != nil {
		log.Fatal(err)
	}
}
