// esub, phase 1.5 implementation:
// PING, INFO, KEYS, SUB and REP
// phase 1.5 adds PSUB

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/xhandler"
	"github.com/rs/xmux"
)

type key int

const (
	keyKey   key = iota
	keyToken key = iota
	keyStart key = iota
)

var (
	// this shouldn't change for our lifetime
	infoJSON = getInfoJSON()

	metricPrefix = MetricPrefix()

	// exposed prometheus Metrics
	httpRequest = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricPrefix,
			Subsystem: "http",
			Name:      "request",
			Help:      "http requests received",
		},
		[]string{"route", "method", "success", "auth", "confirmed"},
	)

	psubClients = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Subsystem: "psub",
			Name:      "connected",
			Help:      "psub clients connected",
		},
		[]string{},
	)

	prepClients = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Subsystem: "prep",
			Name:      "connected",
			Help:      "prep clients connected",
		},
		[]string{},
	)

	subClients = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Subsystem: "sub",
			Name:      "connected",
			Help:      "sub clients connected",
		},
		[]string{},
	)

	metrics = []prometheus.Collector{
		httpRequest,
		psubClients,
		prepClients,
		subClients,
	}
)

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

func main() {
	c := xhandler.Chain{}

	// adds a close context to handlers for disconnects
	c.UseC(xhandler.CloseHandler)

	// optionally close all http connections
	if os.Getenv("ESUB_CLOSE_CONNECTIONS") != "" {
		c.UseC(CloseConnectionHandler)
	}

	router := xmux.New()

	router.Handle(http.MethodGet, "/metrics", promhttp.Handler())

	router.GET("/ping", xhandler.HandlerFuncC(HandlePing))
	router.GET(
		"/info",
		MetricHandler(xhandler.HandlerFuncC(HandleInfo)),
	)
	router.GET(
		"/keys",
		MetricHandler(xhandler.HandlerFuncC(HandleKeys)),
	)
	router.GET(
		"/sub/:sub",
		MetricHandler(xhandler.HandlerFuncC(HandleSub)),
	)
	router.GET(
		"/psub/:sub",
		MetricHandler(xhandler.HandlerFuncC(HandlePSub)),
	)
	router.POST(
		"/rep/:sub",
		MetricHandler(xhandler.HandlerFuncC(HandleRep)),
	)
	router.GET(
		"/prep",
		MetricHandler(xhandler.HandlerFuncC(HandlePRep)),
	)

	prometheus.MustRegister(metrics...)

	go PeriodicMetrics()
	go TimeoutPSubs()

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
