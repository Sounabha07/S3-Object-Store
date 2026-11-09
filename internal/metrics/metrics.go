// Package metrics defines the Prometheus metrics exported at /_metrics.
// Each process builds its own registry, so tests can run several gateways
// in one process without collisions.
package metrics

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Gateway holds the gateway's metrics.
type Gateway struct {
	Registry *prometheus.Registry

	Requests          *prometheus.CounterVec   // s3_requests_total{op,code}
	Latency           *prometheus.HistogramVec // s3_request_duration_seconds{op}
	BytesIn           prometheus.Counter       // s3_bytes_in_total
	BytesOut          prometheus.Counter       // s3_bytes_out_total
	ShardReadFailures *prometheus.CounterVec   // storage_shard_read_failures_total{node,reason}
	RepairShards      prometheus.Counter       // repair_shards_rebuilt_total
	RepairUnrecover   prometheus.Counter       // repair_chunks_unrecoverable_total
	RepairPasses      prometheus.Counter       // repair_passes_total
	GCDeleted         prometheus.Counter       // gc_shards_deleted_total
	GCBytesFreed      prometheus.Counter       // gc_bytes_freed_total
}

// NewGateway builds and registers the gateway metrics, plus the Go runtime
// and process collectors.
func NewGateway() *Gateway {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Gateway{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "s3_requests_total", Help: "S3 API requests by operation and HTTP status code.",
		}, []string{"op", "code"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "s3_request_duration_seconds", Help: "S3 API request latency by operation.",
			// 1ms .. ~65s; covers small-object metadata ops up to multi-GB transfers.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 17),
		}, []string{"op"}),
		BytesIn:  prometheus.NewCounter(prometheus.CounterOpts{Name: "s3_bytes_in_total", Help: "Request body bytes received."}),
		BytesOut: prometheus.NewCounter(prometheus.CounterOpts{Name: "s3_bytes_out_total", Help: "Response body bytes sent."}),
		ShardReadFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_shard_read_failures_total",
			Help: "Shard reads that failed and fell back to another replica or to reconstruction.",
		}, []string{"node", "reason"}),
		RepairShards:    prometheus.NewCounter(prometheus.CounterOpts{Name: "repair_shards_rebuilt_total", Help: "Shards rebuilt by repair."}),
		RepairUnrecover: prometheus.NewCounter(prometheus.CounterOpts{Name: "repair_chunks_unrecoverable_total", Help: "Chunks repair found with too few shards to rebuild."}),
		RepairPasses:    prometheus.NewCounter(prometheus.CounterOpts{Name: "repair_passes_total", Help: "Completed repair passes."}),
		GCDeleted:       prometheus.NewCounter(prometheus.CounterOpts{Name: "gc_shards_deleted_total", Help: "Unreferenced shards deleted by GC."}),
		GCBytesFreed:    prometheus.NewCounter(prometheus.CounterOpts{Name: "gc_bytes_freed_total", Help: "Bytes freed by GC."}),
	}
	for _, c := range []prometheus.Collector{m.Requests, m.Latency, m.BytesIn, m.BytesOut, m.ShardReadFailures,
		m.RepairShards, m.RepairUnrecover, m.RepairPasses, m.GCDeleted, m.GCBytesFreed} {
		f(c)
	}
	return m
}

// Gauge registers a gauge whose value is read from fn at scrape time.
func Gauge(reg *prometheus.Registry, name, help string, fn func() float64) {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, fn))
}

// Handler serves a registry in the Prometheus text format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// ShardFailureReason classifies a shard read error for the reason label.
func ShardFailureReason(err error) string {
	switch {
	case errors.Is(err, storage.ErrCorrupt):
		return "corrupt"
	case errors.Is(err, storage.ErrNotFound):
		return "not_found"
	case errors.Is(err, storage.ErrUnavailable):
		return "unavailable"
	default:
		return "other"
	}
}

// NewProcess returns a registry with only the Go runtime and process
// collectors, for processes that add their own gauges.
func NewProcess() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// InstrumentHTTP wraps an internal API handler with request count and
// latency metrics, labelled by method and status code.
func InstrumentHTTP(reg *prometheus.Registry, namespace string, h http.Handler) http.Handler {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "http_requests_total", Help: "Internal API requests.",
	}, []string{"method", "code"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "http_request_duration_seconds", Help: "Internal API latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	}, []string{"method", "code"})
	reg.MustRegister(requests, latency)
	return promhttp.InstrumentHandlerCounter(requests, promhttp.InstrumentHandlerDuration(latency, h))
}
