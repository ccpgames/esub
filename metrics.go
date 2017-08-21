package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/PagerDuty/godspeed"
)

// Metric -- groups request metrics, route is the only required value
type Metric struct {
	Name      string
	Route     string
	Key       string
	Auth      bool
	Success   bool
	Confirmed bool
}

// MetricValue -- queued metric values
type MetricValue struct {
	Requests  int
	Durations []float64
}

type metricEnv struct {
	Debug   bool
	Verbose bool
	TagKeys bool
	Prefix  string
}

// per metric
type counterIncrementStruct struct {
	bucket   Metric  // key name
	value    int     // number of messages to increment/decrement by
	duration float64 // response duration (or -1 for decrements)
}

type counterQueryStruct struct {
	bucket  Metric
	channel chan MetricValue
}

type counterDeleteStruct struct {
	bucket Metric
}

var counter map[Metric]MetricValue
var counterQueryChan chan counterQueryStruct
var counterDeleteChan chan counterDeleteStruct
var counterListChan chan chan map[Metric]MetricValue
var counterIncrementChan chan counterIncrementStruct

// CounterInitialize -- starts the metric receiving/writing loop
func CounterInitialize() {
	counter = make(map[Metric]MetricValue)
	counterQueryChan = make(chan counterQueryStruct, 100)
	counterDeleteChan = make(chan counterDeleteStruct, 100)
	counterListChan = make(chan chan map[Metric]MetricValue, 100)
	counterIncrementChan = make(chan counterIncrementStruct)

	go goCounterWriter()
}

func goCounterWriter() {
	for {
		select {

		case ci := <-counterIncrementChan:
			if len(ci.bucket.Route) == 0 {
				return
			}
			v := counter[ci.bucket]
			v.Requests += ci.value
			if ci.value > 0 {
				v.Durations = append(v.Durations, ci.duration)
			} else {
				// drop the reported durations from the front of the slice
				v.Durations = append(v.Durations[0:0], v.Durations[ci.value*-1:]...)
			}

			counter[ci.bucket] = v

		case cq := <-counterQueryChan:
			val, found := counter[cq.bucket]
			if found {
				cq.channel <- val
			} else {
				cq.channel <- MetricValue{-1, nil}
			}

		case cl := <-counterListChan:
			nm := make(map[Metric]MetricValue)
			for k, v := range counter {
				nm[k] = v
			}
			cl <- nm

		case cd := <-counterDeleteChan:
			delete(counter, cd.bucket)

		}
	}
}

// SendMetric -- main export for sending a metric
// doesn't actually send the metric, just queues it to be sent (non-blocking)
func SendMetric(bucket Metric, counter int, start time.Time) {
	if len(bucket.Route) == 0 || counter == 0 {
		return
	}
	counterIncrementChan <- counterIncrementStruct{
		bucket,
		counter,
		time.Since(start).Seconds(),
	}
}

// CounterList -- get a map of all known Metrics and their MetricValues
func CounterList() map[Metric]MetricValue {
	reply := make(chan map[Metric]MetricValue)
	counterListChan <- reply
	return <-reply
}

// CounterDelete -- remove a Metric from knowledge
func CounterDelete(bucket Metric) {
	counterDeleteChan <- counterDeleteStruct{bucket}
}

func average(nums []float64) float64 {
	total := 0.0
	for _, n := range nums {
		total += n
	}
	return total / float64(len(nums))
}

func max(nums []float64) float64 {
	currentMax := 0.0
	for _, n := range nums {
		if n > currentMax {
			currentMax = n
		}
	}
	return currentMax
}

// Hostname returns the system hostname
func Hostname() string {
	host, err := os.Hostname()
	if err != nil {
		host = "localhost"
	}
	return host
}

// LocalIP returns the local non-loopback IP
func LocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

func loggedClose(g *godspeed.Godspeed) {
	err := g.Conn.Close()
	if err != nil {
		log.Println(err)
	}
}

func metricInit() (g *godspeed.Godspeed, settings metricEnv) {
	debug := os.Getenv("ESUB_DEBUG") != ""
	verbose := os.Getenv("ESUB_VERBOSE_DEBUG") != ""
	tagKeys := os.Getenv("ESUB_TAG_KEYS") != ""

	datadog := os.Getenv("DATADOG_SERVICE_HOST")
	if datadog == "" {
		datadog = "localhost"
	}
	g, err := godspeed.New(datadog, godspeed.DefaultPort, false)
	if err != nil {
		log.Fatal("STATS: failed to create godspeed client")
	}

	environment := os.Getenv("ESUB_ENVIRONMENT_NAME")
	if environment != "" {
		g.AddTag(fmt.Sprintf("environment:%s", environment))
	}

	prefix := os.Getenv("ESUB_METRIC_PREFIX")
	if prefix == "" {
		prefix = "esub"
	}

	host := Hostname()

	ipaddr := os.Getenv("ESUB_NODE_IP")
	if ipaddr == "" {
		ipaddr = LocalIP()
	}

	g.AddTag(fmt.Sprintf("esub_ip:%+v", ipaddr))
	g.AddTag(fmt.Sprintf("esub_node:%s", host))

	log.Printf("default metric tags: %+v", g.Tags)

	return g, metricEnv{debug, verbose, tagKeys, prefix}
}

func metricTags(metric Metric, tagKeys bool) []string {
	tags := []string{fmt.Sprintf("route:%s", metric.Route)}
	if tagKeys && metric.Key != "" {
		tags = append(tags, fmt.Sprintf("key:%s", metric.Key))
	}
	if metric.Auth {
		tags = append(tags, fmt.Sprintf("auth:%+v", metric.Auth))
	}
	if metric.Confirmed {
		tags = append(tags, fmt.Sprintf("confirmed:%+v", metric.Confirmed))
	}

	tags = append(tags, fmt.Sprintf("success:%+v", metric.Success))
	return tags
}

// DisplayStats -- main loop to send metrics to either datadog and/or log
func DisplayStats() {
	// setup our environment, default tags...
	g, e := metricInit()

	// close our client when we die
	defer loggedClose(g)

	// start looping forever
	lastSent := time.Now().UTC()
	for {
		timeSince := time.Since(lastSent)

		for k, v := range CounterList() {
			if v.Requests <= 0 {
				CounterDelete(k)
				continue
			}

			var metricName string
			if k.Name == "" {
				metricName = e.Prefix
			} else {
				metricName = fmt.Sprintf("%s.%s", e.Prefix, k.Name)
			}

			displayStat(g, k, v, metricName, metricTags(k, e.TagKeys), timeSince, e)

			// decrements the metric
			SendMetric(k, -v.Requests, time.Now())
		}

		// display outstanding sub count
		subs := SubCount()
		err := g.Count(fmt.Sprintf("%s.sub.waiting", e.Prefix), subs, []string{})
		if err != nil && e.Verbose && subs > 0 {
			log.Printf("STATS: failed to add metric: %+v waiting subs(s)", subs)
		} else if e.Debug {
			log.Printf("STATS: %+v waiting sub(s)", subs)
		}

		// capture run finish time
		lastSent = time.Now().UTC()

		// sleepy time
		time.Sleep(10 * time.Second)
	}
}

func displayStat(
	g *godspeed.Godspeed,
	key Metric,
	metric MetricValue,
	name string,
	tags []string,
	timeSince time.Duration,
	e metricEnv,
) {
	metricName := fmt.Sprintf("%s.requests", name)
	err := g.Count(metricName, float64(metric.Requests), tags)
	logStat(
		err,
		metricName,
		metric,
		tags,
		fmt.Sprintf("%.2f RPS", float64(metric.Requests)/timeSince.Seconds()),
		e,
	)

	if metric.Durations != nil {
		metricName = fmt.Sprintf("%s.duration", name)
		err = g.Timing(metricName, average(metric.Durations), tags)
		logStat(err, metricName, metric, tags, metric.Durations, e)

		metricName = fmt.Sprintf("%s.duration_max", name)
		err = g.Timing(metricName, max(metric.Durations), tags)
		logStat(err, metricName, metric, tags, max(metric.Durations), e)
	}
}

func logStat(
	err error,
	name string,
	metric MetricValue,
	tags []string,
	value interface{},
	e metricEnv,
) {
	if err != nil && e.Verbose && metric.Requests > 0 {
		log.Printf(
			"STATS: failed to add metric for %d %s %+v: %+v: %+v",
			metric.Requests,
			name,
			tags,
			value,
			err.Error(),
		)
	} else if e.Debug {
		log.Printf("STATS: %d %s %+v: %+v", metric.Requests, name, tags, value)
	}
}
