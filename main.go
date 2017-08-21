// esub, phase 1.5 implementation:
// PING, INFO, KEYS, SUB and REP
// phase 1.5 adds PSUB

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rs/xhandler"
	"github.com/rs/xmux"
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
func shipMetric(ctx context.Context, success bool) Metric {
	metric := *ctx.Value(keyMetric).(*Metric)
	start := ctx.Value(keyStart).(time.Time)
	metric.Success = success
	SendMetric(metric, 1, start)
	return metric
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
	router.GET(
		"/psub/:sub",
		MetricHandler("psub", xhandler.HandlerFuncC(HandlePSub)),
	)
	router.POST(
		"/rep/:sub",
		MetricHandler("rep", xhandler.HandlerFuncC(HandleRep)),
	)

	Initialize(context.Background())
	CounterInitialize()
	go DisplayStats()

	port := os.Getenv("ESUB_PORT")
	if port == "" {
		port = "8090"
	}
	port = fmt.Sprintf(":%s", port)
	log.Printf("Listening on %s", port)
	if err := http.ListenAndServe(port, c.Handler(router)); err != nil {
		log.Fatal(err)
	}
}
