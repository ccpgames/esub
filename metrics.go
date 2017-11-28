package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SendHistogram sends a histogram observation with tags
func SendHistogram(m *prometheus.HistogramVec, value float64, tags ...string) {
	m.WithLabelValues(tags...).Observe(value)
}

// SendGauge sets a gauge value with tags
func SendGauge(m *prometheus.GaugeVec, value float64, tags ...string) {
	m.WithLabelValues(tags...).Set(value)
}

// SendTiming measures the request time from context and sends the histogram
func SendTiming(ctx context.Context, m *prometheus.HistogramVec, t ...string) {
	for {
		if len(t) >= 5 {
			break
		}
		t = append(t, "false")
	}
	SendHistogram(m, time.Since(ctx.Value(keyStart).(time.Time)).Seconds(), t...)
}

// PeriodicMetrics -- occasionally sends concurrency gauge metrics
func PeriodicMetrics() {
	metricDelay := os.Getenv("ESUB_PERIODIC_METRIC_FREQUENCY")
	if metricDelay == "" {
		metricDelay = "20"
	}
	metricFreq, err := strconv.ParseInt(metricDelay, 10, 32)
	if err != nil {
		log.Panicf(
			"Failed to parse ESUB_PERIODIC_METRIC_FREQUENCY as int: %s",
			err.Error(),
		)
	}
	for {
		SendGauge(psubClients, PSubCount())
		SendGauge(prepClients, PRepCount())
		SendGauge(subClients, SubCount())
		time.Sleep(time.Duration(metricFreq) * time.Second)
	}
}

// MetricPrefix returns the preferred metrics prefix string
func MetricPrefix() string {
	prefix := os.Getenv("ESUB_METRIC_PREFIX")
	if prefix == "" {
		prefix = "esub"
	}
	return prefix
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
