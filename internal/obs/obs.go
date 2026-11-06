// Package obs provides Prometheus metrics and OpenTelemetry tracing for the
// Liffo gateway control plane (request lifecycle, routing, model state).
package obs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "liffo/gateway"

// Telemetry holds Prometheus registry + OTel tracer provider.
type Telemetry struct {
	Registry *prometheus.Registry
	Tracer   trace.Tracer
	tp       *sdktrace.TracerProvider
	traceOut io.WriteCloser

	// Request lifecycle
	RequestsTotal *prometheus.CounterVec
	ErrorsTotal   *prometheus.CounterVec
	DurationSec   *prometheus.HistogramVec
	TTFTSec       *prometheus.HistogramVec
	QueueWaitSec  *prometheus.HistogramVec
	Inflight      prometheus.Gauge
	QueueWaiting  prometheus.Gauge

	// Routing
	RouteTotal    *prometheus.CounterVec
	FallbackTotal *prometheus.CounterVec

	// Workers
	WorkerInflight *prometheus.GaugeVec
	WorkerHealthy  *prometheus.GaugeVec
	WorkerServed   *prometheus.CounterVec

	// Model lifecycle
	ModelState       *prometheus.GaugeVec
	ModelTransitions *prometheus.CounterVec
	ModelInflight    prometheus.Gauge
	ModelQueueDepth  prometheus.Gauge

	// Admission / lifecycle rejects
	RejectTotal *prometheus.CounterVec

	mu          sync.Mutex
	lastModelSt string
	spanSamples []map[string]any
	maxSamples  int
}

// Config for telemetry setup.
type Config struct {
	// TraceJSONL path: when set, OTel spans export as JSON lines for evidence.
	TraceJSONL string
	// ServiceName for OTel resource.
	ServiceName string
}

// New builds a dedicated registry (safe for multiple gateways in tests).
func New(cfg Config) (*Telemetry, error) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	t := &Telemetry{
		Registry:   reg,
		maxSamples: 500,
	}

	t.RequestsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_requests_total",
		Help: "Completed gateway chat completion requests",
	}, []string{"status", "error_code", "worker", "route_method", "fallback", "model"})

	t.ErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_errors_total",
		Help: "Gateway errors by class and code",
	}, []string{"class", "error_code", "worker"})

	t.DurationSec = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "liffo_request_duration_seconds",
		Help:    "End-to-end request latency (queue + upstream stream)",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"status", "worker", "route_method"})

	t.TTFTSec = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "liffo_ttft_seconds",
		Help:    "Time to first token (backend-oriented estimate when available)",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"worker", "route_method"})

	t.QueueWaitSec = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "liffo_queue_wait_seconds",
		Help:    "Admission queue wait before worker execute",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"result"}) // admitted | timeout | canceled

	t.Inflight = factory.NewGauge(prometheus.GaugeOpts{
		Name: "liffo_inflight_requests",
		Help: "Currently admitted in-flight requests at gateway",
	})
	t.QueueWaiting = factory.NewGauge(prometheus.GaugeOpts{
		Name: "liffo_queue_waiting",
		Help: "Requests waiting for an admission slot",
	})

	t.RouteTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_route_decisions_total",
		Help: "Routing decisions",
	}, []string{"worker", "route_method", "fallback", "reason"})

	t.FallbackTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_route_fallback_total",
		Help: "Routing fallbacks (overload/unhealthy/cold/etc)",
	}, []string{"fallback", "from_reason", "worker"})

	t.WorkerInflight = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "liffo_worker_inflight",
		Help: "Per-worker in-flight count",
	}, []string{"worker"})
	t.WorkerHealthy = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "liffo_worker_healthy",
		Help: "1 if worker healthy, 0 otherwise",
	}, []string{"worker"})
	t.WorkerServed = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_worker_served_total",
		Help: "Requests completed on worker (success or fail)",
	}, []string{"worker", "success"})

	t.ModelState = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "liffo_model_state",
		Help: "Model lifecycle state (1 for current state)",
	}, []string{"state", "model"})
	t.ModelTransitions = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_model_transitions_total",
		Help: "Model lifecycle transitions",
	}, []string{"from", "to", "model"})
	t.ModelInflight = factory.NewGauge(prometheus.GaugeOpts{
		Name: "liffo_model_inflight",
		Help: "Model lifecycle lease inflight",
	})
	t.ModelQueueDepth = factory.NewGauge(prometheus.GaugeOpts{
		Name: "liffo_model_job_queue_depth",
		Help: "Queued model load/evict jobs",
	})

	t.RejectTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "liffo_rejects_total",
		Help: "Rejected requests (admission, model lifecycle, no worker)",
	}, []string{"reason"})

	// OpenTelemetry tracer → JSONL file for evidence
	svc := cfg.ServiceName
	if svc == "" {
		svc = "liffo-gateway"
	}
	var exp sdktrace.SpanExporter
	var err error
	if cfg.TraceJSONL != "" {
		_ = os.MkdirAll(dirOf(cfg.TraceJSONL), 0o755)
		f, ferr := os.OpenFile(cfg.TraceJSONL, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if ferr != nil {
			return nil, ferr
		}
		t.traceOut = f
		exp, err = stdouttrace.New(
			stdouttrace.WithWriter(f),
			stdouttrace.WithPrettyPrint(),
		)
	} else {
		exp, err = stdouttrace.New(stdouttrace.WithWriter(io.Discard))
	}
	if err != nil {
		return nil, err
	}
	// Avoid resource.Default() schema URL clash across otel versions.
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(svc),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.tp = tp
	otel.SetTracerProvider(tp)
	t.Tracer = tp.Tracer(tracerName)
	return t, nil
}

func dirOf(path string) string {
	i := strings.LastIndexAny(path, `/\`)
	if i < 0 {
		return "."
	}
	return path[:i]
}

// Handler returns Prometheus scrape handler for this registry.
func (t *Telemetry) Handler() http.Handler {
	return promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{Registry: t.Registry})
}

// Shutdown flushes OTel spans.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.tp != nil {
		_ = t.tp.Shutdown(ctx)
	}
	if t.traceOut != nil {
		return t.traceOut.Close()
	}
	return nil
}

// RequestAttrs are labels recorded for one chat completion.
type RequestAttrs struct {
	Worker      string
	RouteMethod string
	Fallback    string
	Reason      string
	Model       string
	ErrorCode   string
	ErrorClass  string
	Status      string // ok | error | reject
	QueueWaitMs float64
	TTFTMs      float64
	LatencyMs   float64
	HTTPStatus  int
	// RoutingDecisionID correlates a public response to server-side OTel evidence.
	RoutingDecisionID string
	// RoutingJSON is a privacy-safe structured decision (hashes/scores only, no prompt text).
	RoutingJSON     string
	EstimatedTTFTMs float64
	BackendTTFTMs   float64
	GatewayTTFTMs   float64
}

// StartRequest begins an OTel span for the request lifecycle.
func (t *Telemetry) StartRequest(ctx context.Context, route string) (context.Context, trace.Span) {
	if t == nil || t.Tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.Tracer.Start(ctx, "gateway.chat_completions",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("http.route", route)),
	)
}

// ObserveQueueWait records admission wait.
func (t *Telemetry) ObserveQueueWait(result string, waitSec float64) {
	if t == nil {
		return
	}
	t.QueueWaitSec.WithLabelValues(result).Observe(waitSec)
}

// IncQueueWaiting adjusts waiting gauge.
func (t *Telemetry) IncQueueWaiting(delta float64) {
	if t == nil {
		return
	}
	t.QueueWaiting.Add(delta)
}

// SetInflight sets admitted inflight gauge.
func (t *Telemetry) SetInflight(n float64) {
	if t == nil {
		return
	}
	t.Inflight.Set(n)
}

// RecordRoute records a routing decision.
func (t *Telemetry) RecordRoute(worker, method, fallback, reason string) {
	if t == nil {
		return
	}
	if fallback == "" {
		fallback = "none"
	}
	rshort := reasonClass(reason)
	t.RouteTotal.WithLabelValues(worker, method, fallback, rshort).Inc()
	if fallback != "none" {
		t.FallbackTotal.WithLabelValues(fallback, rshort, worker).Inc()
	}
}

// RecordReject increments reject counter.
func (t *Telemetry) RecordReject(reason string) {
	if t == nil {
		return
	}
	t.RejectTotal.WithLabelValues(reason).Inc()
}

// FinishRequest records counters/histograms and ends the span.
func (t *Telemetry) FinishRequest(span trace.Span, a RequestAttrs) {
	if t == nil {
		return
	}
	if a.Status == "" {
		if a.ErrorCode != "" {
			a.Status = "error"
		} else {
			a.Status = "ok"
		}
	}
	if a.Worker == "" {
		a.Worker = "none"
	}
	if a.RouteMethod == "" {
		a.RouteMethod = "unknown"
	}
	if a.Fallback == "" {
		a.Fallback = "none"
	}
	if a.Model == "" {
		a.Model = "unknown"
	}
	if a.ErrorCode == "" {
		a.ErrorCode = "none"
	}
	t.RequestsTotal.WithLabelValues(a.Status, a.ErrorCode, a.Worker, a.RouteMethod, a.Fallback, a.Model).Inc()
	if a.Status != "ok" {
		class := a.ErrorClass
		if class == "" {
			class = a.Status
		}
		t.ErrorsTotal.WithLabelValues(class, a.ErrorCode, a.Worker).Inc()
	}
	if a.LatencyMs > 0 {
		t.DurationSec.WithLabelValues(a.Status, a.Worker, a.RouteMethod).Observe(a.LatencyMs / 1000.0)
	}
	if a.TTFTMs > 0 && a.Status == "ok" {
		t.TTFTSec.WithLabelValues(a.Worker, a.RouteMethod).Observe(a.TTFTMs / 1000.0)
	}
	if a.QueueWaitMs >= 0 {
		res := "admitted"
		if a.Status == "reject" {
			res = a.ErrorCode
			if res == "" {
				res = "reject"
			}
		}
		t.QueueWaitSec.WithLabelValues(res).Observe(a.QueueWaitMs / 1000.0)
	}
	ok := "false"
	if a.Status == "ok" {
		ok = "true"
	}
	if a.Worker != "none" {
		t.WorkerServed.WithLabelValues(a.Worker, ok).Inc()
	}

	if span != nil && span.IsRecording() {
		span.SetAttributes(
			attribute.String("liffo.worker", a.Worker),
			attribute.String("liffo.route_method", a.RouteMethod),
			attribute.String("liffo.fallback", a.Fallback),
			attribute.String("liffo.reason", a.Reason),
			attribute.String("liffo.model", a.Model),
			attribute.String("liffo.status", a.Status),
			attribute.String("liffo.error_code", a.ErrorCode),
			attribute.Float64("liffo.queue_wait_ms", a.QueueWaitMs),
			attribute.Float64("liffo.ttft_ms", a.TTFTMs),
			attribute.Float64("liffo.latency_ms", a.LatencyMs),
			attribute.String("liffo.routing.decision_id", a.RoutingDecisionID),
			attribute.String("liffo.routing.decision", a.RoutingJSON),
			attribute.Float64("liffo.routing.estimated_ttft_ms", a.EstimatedTTFTMs),
			attribute.Float64("liffo.backend_ttft_ms", a.BackendTTFTMs),
			attribute.Float64("liffo.gateway_ttft_ms", a.GatewayTTFTMs),
			attribute.Int("http.status_code", a.HTTPStatus),
		)
		if a.Status != "ok" {
			span.SetStatus(codes.Error, a.ErrorCode)
			if a.ErrorCode != "" {
				span.RecordError(fmt.Errorf("%s", a.ErrorCode))
			}
		} else {
			span.SetStatus(codes.Ok, "ok")
		}
		// lightweight in-memory sample for evidence (no prompt text)
		t.mu.Lock()
		if len(t.spanSamples) < t.maxSamples {
			t.spanSamples = append(t.spanSamples, map[string]any{
				"ts":                time.Now().UTC().Format(time.RFC3339Nano),
				"worker":            a.Worker,
				"route_method":      a.RouteMethod,
				"fallback":          a.Fallback,
				"reason":            a.Reason,
				"model":             a.Model,
				"status":            a.Status,
				"error_code":        a.ErrorCode,
				"queue_ms":          a.QueueWaitMs,
				"ttft_ms":           a.TTFTMs,
				"latency_ms":        a.LatencyMs,
				"http_status":       a.HTTPStatus,
				"decision_id":       a.RoutingDecisionID,
				"estimated_ttft_ms": a.EstimatedTTFTMs,
				"backend_ttft_ms":   a.BackendTTFTMs,
				"gateway_ttft_ms":   a.GatewayTTFTMs,
			})
		}
		t.mu.Unlock()
		span.End()
	}
}

// SpanSamples returns privacy-safe span attribute samples.
func (t *Telemetry) SpanSamples() []map[string]any {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]map[string]any, len(t.spanSamples))
	copy(out, t.spanSamples)
	return out
}

// SetModelState updates model lifecycle gauges.
func (t *Telemetry) SetModelState(state, model string, inflight, queueDepth int) {
	if t == nil {
		return
	}
	// zero all known states for this model then set current
	for _, st := range []string{"loading", "ready", "draining", "evicting", "failed", "stopped"} {
		v := 0.0
		if st == state {
			v = 1.0
		}
		t.ModelState.WithLabelValues(st, model).Set(v)
	}
	t.ModelInflight.Set(float64(inflight))
	t.ModelQueueDepth.Set(float64(queueDepth))
	t.mu.Lock()
	prev := t.lastModelSt
	if prev != "" && prev != state {
		t.ModelTransitions.WithLabelValues(prev, state, model).Inc()
	}
	t.lastModelSt = state
	t.mu.Unlock()
}

// ObserveModelTransition records an explicit transition.
func (t *Telemetry) ObserveModelTransition(from, to, model string) {
	if t == nil {
		return
	}
	t.ModelTransitions.WithLabelValues(from, to, model).Inc()
	t.SetModelState(to, model, 0, 0)
}

// SetWorkerGauges updates per-worker health/inflight.
func (t *Telemetry) SetWorkerGauges(worker string, inflight float64, healthy bool) {
	if t == nil {
		return
	}
	t.WorkerInflight.WithLabelValues(worker).Set(inflight)
	h := 0.0
	if healthy {
		h = 1.0
	}
	t.WorkerHealthy.WithLabelValues(worker).Set(h)
}

// GatherText scrapes metrics in Prometheus text format (for evidence files).
func (t *Telemetry) GatherText() (string, error) {
	if t == nil {
		return "", fmt.Errorf("nil telemetry")
	}
	mfs, err := t.Registry.Gather()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, mf := range mfs {
		_, _ = fmt.Fprintf(&b, "# HELP %s %s\n", mf.GetName(), mf.GetHelp())
		_, _ = fmt.Fprintf(&b, "# TYPE %s %s\n", mf.GetName(), strings.ToLower(mf.GetType().String()))
		for _, m := range mf.GetMetric() {
			// Use expfmt would be better but avoid extra dep surface — write simplified
			_ = m
		}
	}
	// Prefer promhttp encode via temporary gatherer write
	return encodeGather(t.Registry)
}

func encodeGather(reg *prometheus.Registry) (string, error) {
	// Use prometheus/common expfmt via Handler round-trip is heavy; use internal Write
	// client_golang Gather + expfmt
	return gatherPrometheusText(reg)
}

func reasonClass(reason string) string {
	r := strings.ToLower(reason)
	switch {
	case r == "":
		return "none"
	case strings.Contains(r, "overload"):
		return "overload"
	case strings.Contains(r, "unhealthy"):
		return "unhealthy"
	case strings.Contains(r, "least"):
		return "least_inflight"
	case strings.Contains(r, "sticky"):
		return "sticky"
	case strings.Contains(r, "multi_slo") || strings.Contains(r, "cost"):
		return "multi_slo"
	case strings.Contains(r, "force"):
		return "forced"
	case strings.Contains(r, "fallback"):
		return "fallback"
	default:
		if len(r) > 40 {
			return r[:40]
		}
		return strings.ReplaceAll(r, " ", "_")
	}
}
