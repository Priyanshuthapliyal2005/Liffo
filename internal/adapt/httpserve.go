package adapt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/obs"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
	"go.opentelemetry.io/otel/trace"
)

// WorkerReg is a registered backend with a stable label (never stores secrets).
type WorkerReg struct {
	ID    int    `json:"id"`
	URL   string `json:"url"`
	Label string `json:"label"`
}

// ServeConfig for the real HTTP control-plane gateway.
type ServeConfig struct {
	Listen  string
	Gateway GatewayConfig
	Labels  []string
	// Telemetry optional Prometheus + OpenTelemetry. When nil, NewHTTPServer creates defaults.
	Telemetry *obs.Telemetry
	// TraceJSONL used only when Telemetry is nil (path for OTel span export).
	TraceJSONL string
	// AdminToken authenticates lifecycle mutation/event endpoints.
	AdminToken string
}

// WorkerProbe is live readiness for one registered worker.
type WorkerProbe struct {
	ID              int    `json:"id"`
	Label           string `json:"label"`
	URL             string `json:"url"`
	Registered      bool   `json:"registered"`
	RouterHealthy   bool   `json:"router_healthy"`
	Ready           bool   `json:"ready"`
	ProbeHTTPStatus int    `json:"probe_http_status,omitempty"`
	ProbeError      string `json:"probe_error,omitempty"`
	Inflight        int64  `json:"inflight"`
	Served          int64  `json:"served"`
}

// HTTPServer is the real Liffo gateway (not a temporary proxy).
type HTTPServer struct {
	gw           *Gateway
	labels       []string
	client       *http.Client
	srv          *http.Server
	obs          *obs.Telemetry
	limiter      *rateLimiter
	sec          SecurityConfig
	shuttingDown int32
	adminToken   string
	mu           sync.Mutex
}

// NewHTTPServer builds gateway + HTTP handlers.
func NewHTTPServer(cfg ServeConfig) (*HTTPServer, error) {
	if ((cfg.Gateway.Coordinator != nil && cfg.Gateway.Coordinator.HasManaged()) || cfg.Gateway.Life != nil) && strings.TrimSpace(cfg.AdminToken) == "" {
		return nil, fmt.Errorf("adapt: LIFFO_ADMIN_TOKEN is required for managed lifecycle")
	}
	// Preserve pre-unification HTTP admit wait: when AdmitTimeout was unset,
	// handleChatCompletions used DefaultTimeout as the admit deadline.
	// Shared BeginRequest only applies AdmitTimeout (canonical Handle semantics),
	// so map that historical HTTP default here without forking admit code.
	if cfg.Gateway.AdmitTimeout <= 0 && cfg.Gateway.DefaultTimeout > 0 {
		cfg.Gateway.AdmitTimeout = cfg.Gateway.DefaultTimeout
	}
	gw, err := NewGateway(cfg.Gateway)
	if err != nil {
		return nil, err
	}
	labels := append([]string(nil), cfg.Labels...)
	if len(labels) < len(cfg.Gateway.Workers) {
		for i := len(labels); i < len(cfg.Gateway.Workers); i++ {
			labels = append(labels, fmt.Sprintf("worker-%d", i))
		}
	}
	tel := cfg.Telemetry
	if tel == nil {
		tel, err = obs.New(obs.Config{ServiceName: "liffo-gateway", TraceJSONL: cfg.TraceJSONL})
		if err != nil {
			return nil, fmt.Errorf("obs: %w", err)
		}
	}
	sec := defaultSecurity()
	hs := &HTTPServer{
		gw:         gw,
		labels:     labels,
		obs:        tel,
		limiter:    newRateLimiter(sec.RateRPS, sec.RateBurst),
		sec:        sec,
		adminToken: cfg.AdminToken,
		// Keep-alives ON: the gateway opened a fresh upstream TCP connection per
		// request, which added a handshake to every TTFT and burned ephemeral
		// ports under load. Workers are long-lived, so connections are reused.
		client: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				DisableKeepAlives:   false,
				MaxIdleConns:        0,
				MaxIdleConnsPerHost: 512,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   false,
			},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/ready", hs.handleReady)
	mux.HandleFunc("/v1/workers", hs.handleWorkers)
	mux.HandleFunc("/v1/chat/completions", hs.handleChatCompletions)
	mux.HandleFunc("/v1/route-method", hs.handleRouteMethod)
	mux.HandleFunc("/v1/quality-feedback", hs.handleQualityFeedback)
	mux.HandleFunc("/v1/workers/reset-health", hs.handleResetHealth)
	mux.HandleFunc("/v1/models", hs.handleModelsDispatch)
	mux.HandleFunc("/v1/models/load", hs.handleModelsLoad)
	mux.HandleFunc("/v1/models/evict", hs.handleModelsEvict)
	mux.HandleFunc("/v1/models/events", hs.handleModelsEvents)
	mux.Handle("/metrics", tel.Handler())
	mux.HandleFunc("/v1/obs/samples", hs.handleObsSamples)
	listen := cfg.Listen
	if listen == "" {
		listen = "127.0.0.1:8088"
	}
	hs.srv = &http.Server{
		Addr:              listen,
		Handler:           hs.applySecurityMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		// Hardened timeouts (idle connections)
		ReadTimeout:  0, // streaming; use context
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}
	hs.syncModelObs()
	hs.syncWorkerObs()
	return hs, nil
}

// handleModelsDispatch: GET = simple model list; other methods use lifecycle snapshot.
func (h *HTTPServer) handleModelsDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if coord := h.gw.Coordinator(); coord != nil {
			writeJSON(w, http.StatusOK, map[string]any{"object": "list", "workers": coord.Status()})
			return
		}
		data := []map[string]any{
			{"id": "local", "object": "model", "owned_by": "liffo-mesh"},
		}
		ms := h.gw.ModelSnapshot()
		if ms.Model != "" && ms.Model != "local" {
			data = append(data, map[string]any{
				"id": ms.Model, "object": "model", "owned_by": "liffo-lifecycle",
				"lifecycle_state": ms.State,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
		return
	}
	h.handleModels(w, r)
}

// Obs returns telemetry (never nil after NewHTTPServer).
func (h *HTTPServer) Obs() *obs.Telemetry { return h.obs }

func (h *HTTPServer) syncModelObs() {
	if h.obs == nil {
		return
	}
	ms := h.gw.ModelSnapshot()
	h.obs.SetModelState(string(ms.State), ms.Model, ms.Inflight, ms.QueueDepth)
}

func (h *HTTPServer) syncWorkerObs() {
	if h.obs == nil {
		return
	}
	for _, w := range h.gw.Router().Workers() {
		h.obs.SetWorkerGauges(h.labelOf(w.ID), float64(w.InFlight()), w.Healthy())
	}
	h.obs.SetInflight(float64(atomic.LoadInt32(&h.gw.inflight)))
}

func (h *HTTPServer) Gateway() *Gateway { return h.gw }

func (h *HTTPServer) Addr() string {
	if h.srv == nil {
		return ""
	}
	return h.srv.Addr
}

func (h *HTTPServer) ListenAndServe() error { return h.srv.ListenAndServe() }

func (h *HTTPServer) Serve(ln net.Listener) error { return h.srv.Serve(ln) }

func (h *HTTPServer) Shutdown(ctx context.Context) error {
	// Prefer graceful drain + obs flush when available.
	if h.sec.ShutdownGrace > 0 {
		return h.GracefulShutdown(ctx)
	}
	return h.srv.Shutdown(ctx)
}

func (h *HTTPServer) ProbeWorkers(ctx context.Context) []WorkerProbe {
	ws := h.gw.Router().Workers()
	out := make([]WorkerProbe, 0, len(ws))
	for _, w := range ws {
		p := WorkerProbe{
			ID:            w.ID,
			Label:         h.labelOf(w.ID),
			URL:           w.BaseURL,
			Registered:    true,
			RouterHealthy: w.Healthy(),
			Inflight:      w.InFlight(),
			Served:        w.Served(),
		}
		u := strings.TrimRight(w.BaseURL, "/") + "/health"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			p.ProbeError = err.Error()
			out = append(out, p)
			continue
		}
		resp, err := h.client.Do(req)
		if err != nil {
			p.ProbeError = err.Error()
			out = append(out, p)
			continue
		}
		p.ProbeHTTPStatus = resp.StatusCode
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			p.Ready = true
		} else {
			p.ProbeError = fmt.Sprintf("http %d: %s", resp.StatusCode, string(b))
		}
		out = append(out, p)
	}
	return out
}

func (h *HTTPServer) labelOf(id int) string {
	if id >= 0 && id < len(h.labels) {
		return h.labels[id]
	}
	return fmt.Sprintf("worker-%d", id)
}

func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	probes := h.ProbeWorkers(r.Context())
	readyN := 0
	for _, p := range probes {
		if p.Ready {
			readyN++
		}
	}
	healthy, total := h.gw.HealthyCount()
	pol := h.gw.applied
	ms := h.gw.ModelSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                 "ok",
		"gateway":                "liffo-adapt",
		"temporary_proxy":        false,
		"listen":                 h.srv.Addr,
		"mode":                   string(h.gw.cfg.Mode),
		"route_method":           string(pol.RouteMethod),
		"max_inflight":           pol.MaxInflight,
		"retry_enabled":          pol.RetryEnabled,
		"workers_registered":     total,
		"workers_router_healthy": healthy,
		"workers_ready":          readyN,
		"workers":                probes,
		"multi_slo":              h.gw.Router().MultiSnapshot(),
		"model":                  ms.Model,
		"model_state":            ms.State,
		"model_inflight":         ms.Inflight,
		"model_queue_depth":      ms.QueueDepth,
		"lifecycle_wired":        h.gw.Life() != nil || h.gw.Coordinator() != nil,
	})
}

func (h *HTTPServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	probes := h.ProbeWorkers(r.Context())
	allReady := len(probes) > 0
	for _, p := range probes {
		if !p.Ready {
			allReady = false
			break
		}
	}
	code := http.StatusOK
	status := "ready"
	if !allReady {
		code = http.StatusServiceUnavailable
		status = "not_ready"
	}
	writeJSON(w, code, map[string]any{
		"status":          status,
		"gateway":         "liffo-adapt",
		"temporary_proxy": false,
		"workers":         probes,
	})
}

func (h *HTTPServer) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"gateway":         "liffo-adapt",
		"temporary_proxy": false,
		"workers":         h.ProbeWorkers(r.Context()),
	})
}

type chatBody struct {
	Model       string           `json:"model"`
	Messages    []map[string]any `json:"messages"`
	Stream      bool             `json:"stream"`
	MaxTokens   int              `json:"max_tokens"`
	Temperature float64          `json:"temperature"`
	LiffoRoute  *struct {
		MinQuality    float64 `json:"min_quality"`
		MaxCostPerReq float64 `json:"max_cost_per_req"`
		MaxLatencyMs  float64 `json:"max_latency_ms"`
		PreferQuality bool    `json:"prefer_quality"`
		PreferCost    bool    `json:"prefer_cost"`
	} `json:"liffo_route,omitempty"`
}

func (h *HTTPServer) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reqStart := time.Now()
	ctx, span := h.obs.StartRequest(r.Context(), "/v1/chat/completions")
	attrs := obs.RequestAttrs{Status: "error", ErrorCode: "unknown", Model: h.gw.ActiveModel()}
	var routingDecisionID, routingJSON string
	var estimatedTTFTMs, backendTTFTMs, gatewayTTFTMs float64
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		if attrs.LatencyMs <= 0 {
			attrs.LatencyMs = float64(time.Since(reqStart).Seconds() * 1000)
		}
		attrs.RoutingDecisionID = routingDecisionID
		attrs.RoutingJSON = routingJSON
		attrs.EstimatedTTFTMs = estimatedTTFTMs
		attrs.BackendTTFTMs = backendTTFTMs
		attrs.GatewayTTFTMs = gatewayTTFTMs
		h.obs.FinishRequest(span, attrs)
		h.syncWorkerObs()
		h.syncModelObs()
	}
	defer finish()

	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		attrs = obs.RequestAttrs{Status: "error", ErrorCode: "bad_body", ErrorClass: "http", HTTPStatus: 400, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var body chatBody
	if err := json.Unmarshal(raw, &body); err != nil {
		attrs = obs.RequestAttrs{Status: "error", ErrorCode: "bad_json", ErrorClass: "http", HTTPStatus: 400, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	var constraints *route.RouteConstraints
	if limits := body.LiffoRoute; limits != nil {
		if limits.MinQuality < 0 || limits.MinQuality > 1 || limits.MaxCostPerReq < 0 || limits.MaxLatencyMs < 0 || (limits.PreferQuality && limits.PreferCost) || h.gw.Router().Method() != route.MethodMultiSLO || r.Header.Get("X-Liffo-Force-Worker-Id") != "" {
			http.Error(w, "invalid liffo_route or unsupported routing method", http.StatusBadRequest)
			return
		}
		constraints = &route.RouteConstraints{MinQuality: limits.MinQuality, MaxCostPerReq: limits.MaxCostPerReq, MaxLatencyMs: limits.MaxLatencyMs, PreferQuality: limits.PreferQuality, PreferCost: limits.PreferCost}
	}
	content := extractUserContent(body.Messages)

	// Optional bench-only artificial delay (simulates added network latency).
	artMs := 0
	if v := r.Header.Get("X-Liffo-Artificial-Delay-Ms"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
			artMs = n
		}
	}

	targetModel := strings.TrimSpace(body.Model)
	if targetModel == "" {
		targetModel = h.gw.ActiveModel()
	}

	// Resolve a model-capable worker before taking its per-worker lifecycle lease.
	var eligible map[int]bool
	if coord := h.gw.Coordinator(); coord != nil {
		eligible = coord.ReadyWorkers(targetModel)
		if len(eligible) == 0 {
			attrs = obs.RequestAttrs{Status: "reject", ErrorCode: "model_not_loaded", ErrorClass: "lifecycle", HTTPStatus: http.StatusConflict, Model: targetModel}
			writeJSON(w, http.StatusConflict, map[string]any{"error": "requested model is not loaded", "error_code": "model_not_loaded", "model": targetModel})
			return
		}
	}

	// Pick worker (optional force for fair local vs remote cells).
	routeStart := time.Now()
	var dec route.Decision
	// Admission BEFORE worker reservation (see Gateway.BeginAdmission): a
	// queued request must not inflate the router's inflight signal.
	maxInf := h.gw.appliedMaxInflight()
	queuedObs := false
	if int(atomic.LoadInt32(&h.gw.inflight)) >= maxInf {
		h.obs.IncQueueWaiting(1)
		queuedObs = true
	}
	sess, serr := h.gw.BeginAdmission(ctx)
	if queuedObs {
		h.obs.IncQueueWaiting(-1)
	}
	if serr != nil {
		h.writeSessionReject(w, serr, &attrs, reqStart)
		return
	}
	defer func() {
		sess.Release()
		h.obs.SetInflight(float64(atomic.LoadInt32(&h.gw.inflight)))
	}()
	h.obs.SetInflight(float64(atomic.LoadInt32(&h.gw.inflight)))
	queueMs := sess.QueueWaitMs
	attrs.QueueWaitMs = queueMs

	reserved := false
	if v := r.Header.Get("X-Liffo-Force-Worker-Id"); v != "" {
		if h.adminToken == "" || !modellife.BearerAuthorized(r.Header.Get("Authorization"), h.adminToken) {
			attrs = obs.RequestAttrs{Status: "error", ErrorCode: "force_worker_unauthorized", ErrorClass: "auth", HTTPStatus: http.StatusUnauthorized, Model: targetModel}
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "benchmark authorization required", "error_code": "force_worker_unauthorized"})
			return
		}
		id, err := strconv.Atoi(v)
		if err != nil || id < 0 || id >= len(h.gw.Router().Workers()) || (eligible != nil && !eligible[id]) {
			attrs = obs.RequestAttrs{Status: "error", ErrorCode: "bad_force_worker", ErrorClass: "http", HTTPStatus: 400, Model: targetModel, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
			http.Error(w, "invalid or model-ineligible force worker id", http.StatusBadRequest)
			return
		}
		wkr := h.gw.Router().Workers()[id]
		dec = route.Decision{
			WorkerID: id,
			BaseURL:  wkr.BaseURL,
			Method:   "force_worker",
			Reason:   fmt.Sprintf("forced worker %d (%s)", id, h.labelOf(id)),
		}
		h.gw.Router().Acquire(id)
		reserved = true
	} else if eligible != nil {
		dec = h.gw.Router().PickEligibleWithConstraintsAndAcquire(content, eligible, constraints)
		reserved = dec.WorkerID >= 0 && dec.BaseURL != ""
	} else {
		dec = h.gw.Router().PickWithConstraintsAndAcquire(content, constraints)
		reserved = dec.WorkerID >= 0 && dec.BaseURL != ""
	}
	routeMs := float64(time.Since(routeStart).Microseconds()) / 1000.0
	attachRoutingTelemetry(w, span, &attrs, dec)
	routingDecisionID = attrs.RoutingDecisionID
	routingJSON = attrs.RoutingJSON
	estimatedTTFTMs = attrs.EstimatedTTFTMs
	if dec.WorkerID < 0 || dec.BaseURL == "" {
		if dec.Fallback == "constraints_unavailable" {
			h.obs.RecordReject("constraints_unavailable")
			attrs = obs.RequestAttrs{Status: "reject", ErrorCode: "constraints_unavailable", ErrorClass: "route", HTTPStatus: 503, Model: targetModel}
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": dec.Reason, "error_code": "constraints_unavailable"})
			return
		}
		h.obs.RecordReject("no_model_worker")
		attrs = obs.RequestAttrs{Status: "reject", ErrorCode: "no_model_worker", ErrorClass: "route", HTTPStatus: 503, Model: targetModel, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no healthy worker has the requested model", "error_code": "no_model_worker", "model": targetModel})
		return
	}

	// Lifecycle lease for the selected worker, after admission + reservation.
	if lerr := sess.AttachLifecycle(ctx, dec.WorkerID, targetModel); lerr != nil {
		if reserved {
			h.gw.Router().CancelAcquire(dec.WorkerID)
		}
		h.writeSessionReject(w, lerr, &attrs, reqStart)
		return
	}
	attrs.Model = sess.Model
	// Preserve the shared admission identifier in the response.
	w.Header().Set("X-Liffo-Shared-Path", sess.PathID)

	wLabel := h.labelOf(dec.WorkerID)
	h.obs.RecordRoute(wLabel, dec.Method, dec.Fallback, dec.Reason)
	attrs.Worker = wLabel
	attrs.RouteMethod = dec.Method
	attrs.Fallback = dec.Fallback
	attrs.Reason = dec.Reason

	healthAtPick := h.gw.Router().SnapshotHealth()
	h.syncWorkerObs()

	if artMs > 0 {
		select {
		case <-ctx.Done():
			h.gw.Router().CancelAcquire(dec.WorkerID)
			attrs = obs.RequestAttrs{Status: "reject", ErrorCode: "art_delay_canceled", ErrorClass: "cancel", Worker: wLabel, RouteMethod: dec.Method, Fallback: dec.Fallback, Reason: dec.Reason, Model: attrs.Model, QueueWaitMs: queueMs, HTTPStatus: 408, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
			http.Error(w, "canceled during artificial delay", http.StatusRequestTimeout)
			return
		case <-time.After(time.Duration(artMs) * time.Millisecond):
		}
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		h.gw.Router().CancelAcquire(dec.WorkerID)
		attrs = obs.RequestAttrs{Status: "error", ErrorCode: "bad_payload", ErrorClass: "http", Worker: wLabel, HTTPStatus: 400, Model: attrs.Model, QueueWaitMs: queueMs, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	delete(payload, "liffo_route") // gateway-only controls must not leak to OpenAI-compatible workers
	if body.Model == "" {
		if sess.LifecycleHold() && sess.Model != "" {
			payload["model"] = sess.Model
		} else {
			payload["model"] = h.gw.ActiveModel()
		}
	}
	if m, ok := payload["model"].(string); ok && m != "" {
		attrs.Model = m
	}
	if sess.LifecycleHold() {
		w.Header().Set("X-Liffo-Model", sess.Model)
		w.Header().Set("X-Liffo-Model-State", "ready")
		w.Header().Set("X-Liffo-Lifecycle-Hold", "1")
	}
	// The health probe runs before inference. RTT/2 estimates the return-path delay.
	upStart := time.Now()
	var timing UpstreamTiming
	rtt, perr := probeNetworkRTT(ctx, h.client, dec.BaseURL)
	timing.NetworkRTTMs = rtt
	timing.NetworkOneWayMs = rtt / 2
	if perr != nil {
		timing.ProbeError = perr.Error()
	}

	upURL := strings.TrimRight(dec.BaseURL, "/") + "/v1/chat/completions"
	upRaw, _ := json.Marshal(payload)
	tt := &traceTimers{}
	traceCtx := httptrace.WithClientTrace(ctx, tt.clientTrace())
	upReq, err := http.NewRequestWithContext(traceCtx, http.MethodPost, upURL, bytes.NewReader(upRaw))
	if err != nil {
		h.gw.Router().CancelAcquire(dec.WorkerID)
		attrs = obs.RequestAttrs{Status: "error", ErrorCode: "upstream_build", ErrorClass: "http", Worker: wLabel, RouteMethod: dec.Method, Fallback: dec.Fallback, Reason: dec.Reason, Model: attrs.Model, QueueWaitMs: queueMs, HTTPStatus: 500, LatencyMs: float64(time.Since(reqStart).Seconds() * 1000)}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	upReq.Header.Set("Content-Type", "application/json")
	if body.Stream {
		upReq.Header.Set("Accept", "text/event-stream")
	}

	upResp, err := h.client.Do(upReq)
	if err != nil {
		dialMs, ttfbW, _ := tt.snapshot()
		timing.NetworkDialMs = dialMs
		timing.TTFBAfterWriteMs = ttfbW
		timing.UpstreamTotalHeaderMs = float64(time.Since(upStart).Seconds() * 1000)
		h.gw.Router().Release(dec.WorkerID, false)
		h.gw.Router().ObserveLatency(dec.WorkerID, 0, false)
		setIdentityHeaders(w, h, dec, queueMs, routeMs, artMs, timing, healthAtPick)
		w.Header().Set("X-Liffo-Error-Code", "upstream_error")
		attrs = obs.RequestAttrs{
			Status: "error", ErrorCode: "upstream_error", ErrorClass: "upstream",
			Worker: wLabel, RouteMethod: dec.Method, Fallback: dec.Fallback, Reason: dec.Reason,
			Model: attrs.Model, QueueWaitMs: queueMs, HTTPStatus: 502,
			LatencyMs: float64(time.Since(reqStart).Seconds() * 1000),
		}
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upResp.Body.Close()

	ok := upResp.StatusCode >= 200 && upResp.StatusCode < 300
	dialMs, ttfbAfterWrite, wroteAt := tt.snapshot()
	timing.NetworkDialMs = dialMs
	timing.TTFBAfterWriteMs = ttfbAfterWrite

	var prebuf [][]byte
	var firstContent time.Time
	br := bufio.NewReader(upResp.Body)
	if body.Stream && ok {
		prebuf, firstContent, _ = readUntilFirstContent(br, wroteAt)
		if !firstContent.IsZero() && !wroteAt.IsZero() {
			timing.FirstContentAfterWriteMs = float64(firstContent.Sub(wroteAt).Seconds() * 1000)
		} else if ttfbAfterWrite > 0 {
			timing.FirstContentAfterWriteMs = ttfbAfterWrite
		}
	} else if ttfbAfterWrite > 0 {
		timing.FirstContentAfterWriteMs = ttfbAfterWrite
	}
	rawServer := timing.FirstContentAfterWriteMs
	if rawServer <= 0 {
		rawServer = timing.TTFBAfterWriteMs
	}
	backend := rawServer - timing.NetworkOneWayMs
	if backend < 0 {
		backend = 0
	}
	timing.BackendTTFTMs = backend
	backendTTFTMs = timing.BackendTTFTMs
	gatewayTTFTMs = timing.FirstContentAfterWriteMs
	timing.UpstreamTotalHeaderMs = float64(time.Since(upStart).Seconds() * 1000)

	setIdentityHeaders(w, h, dec, queueMs, routeMs, artMs, timing, healthAtPick)
	if ct := upResp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else if body.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(upResp.StatusCode)

	// Do not ignore stream R/W errors: HTTP 2xx alone was a false-success trap for
	// adaptive Release/RecordSample when the body was truncated or the client left.
	pump := pumpUpstreamToClient(w, br, prebuf, body.Stream)
	reqOK, workerOK, errCode := classifyStreamSuccess(ok, body.Stream, ctx.Err(), pump)
	if !ok && errCode == "upstream_http" {
		errCode = fmt.Sprintf("upstream_%d", upResp.StatusCode)
	}

	h.gw.Router().Release(dec.WorkerID, workerOK)
	latObs := timing.BackendTTFTMs
	if latObs <= 0 {
		latObs = timing.FirstContentAfterWriteMs
	}
	h.gw.Router().ObserveLatency(dec.WorkerID, latObs, workerOK)
	h.gw.served.Add(1)

	// Same adaptive controller as Gateway.Handle (shared path feedback).
	healthy, total := h.gw.HealthyCount()
	_ = h.gw.RecordSample(Sample{
		TTFTMs:         timing.BackendTTFTMs,
		LatencyMs:      float64(time.Since(reqStart).Seconds() * 1000),
		Error:          !reqOK,
		InflightSeen:   sess.InflightSeen,
		HealthyWorkers: healthy,
		TotalWorkers:   total,
	})

	status := "ok"
	if !reqOK {
		status = "error"
	}
	attrs = obs.RequestAttrs{
		Status: status, ErrorCode: errCode, ErrorClass: status,
		Worker: wLabel, RouteMethod: dec.Method, Fallback: dec.Fallback, Reason: dec.Reason,
		Model: attrs.Model, QueueWaitMs: queueMs, TTFTMs: timing.BackendTTFTMs,
		LatencyMs: float64(time.Since(reqStart).Seconds() * 1000), HTTPStatus: upResp.StatusCode,
	}
}

// writeSessionReject maps shared BeginRequest SessionError to HTTP status/body (pre-unification codes preserved).
func (h *HTTPServer) writeSessionReject(w http.ResponseWriter, serr *SessionError, attrs *obs.RequestAttrs, reqStart time.Time) {
	lat := float64(time.Since(reqStart).Seconds() * 1000)
	h.obs.RecordReject(serr.Code)
	if serr.Kind == "lifecycle" {
		h.syncModelObs()
		ms := serr.Life
		*attrs = obs.RequestAttrs{
			Status: "reject", ErrorCode: serr.Code, ErrorClass: "lifecycle",
			Model: ms.Model, HTTPStatus: 503, LatencyMs: lat,
		}
		w.Header().Set("X-Liffo-Error-Code", serr.Code)
		w.Header().Set("X-Liffo-Model-State", string(ms.State))
		w.Header().Set("X-Liffo-Model", ms.Model)
		w.Header().Set("X-Liffo-Shared-Path", SharedPathID)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       serr.Error(),
			"error_code":  serr.Code,
			"model_state": ms.State,
			"model":       ms.Model,
			"inflight":    ms.Inflight,
			"queue_depth": ms.QueueDepth,
		})
		return
	}
	// admit
	httpStatus := http.StatusTooManyRequests
	msg := "admission timeout"
	if serr.Code == "admit_canceled" {
		httpStatus = http.StatusRequestTimeout
		msg = "admit canceled"
	}
	*attrs = obs.RequestAttrs{
		Status: "reject", ErrorCode: serr.Code, ErrorClass: "reject",
		Model: attrs.Model, QueueWaitMs: serr.QueueWaitMs,
		HTTPStatus: httpStatus, LatencyMs: lat,
	}
	w.Header().Set("X-Liffo-Error-Code", serr.Code)
	w.Header().Set("X-Liffo-Shared-Path", SharedPathID)
	http.Error(w, msg, httpStatus)
}

func (h *HTTPServer) handleObsSamples(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.syncWorkerObs()
	h.syncModelObs()
	writeJSON(w, http.StatusOK, map[string]any{
		"span_samples": h.obs.SpanSamples(),
		"metrics_keys": h.obs.SnapshotKeyValues(),
		"model":        h.gw.ModelSnapshot(),
		"inflight":     atomic.LoadInt32(&h.gw.inflight),
	})
}

func setIdentityHeaders(w http.ResponseWriter, h *HTTPServer, dec route.Decision, queueMs, routeMs float64, artMs int, t UpstreamTiming, healthAtPick []route.HealthSnapshot) {
	w.Header().Set("X-Liffo-Gateway", "liffo-adapt")
	w.Header().Set("X-Liffo-Temporary-Proxy", "false")
	w.Header().Set("X-Liffo-Worker-Id", fmt.Sprintf("%d", dec.WorkerID))
	w.Header().Set("X-Liffo-Worker-Label", h.labelOf(dec.WorkerID))
	w.Header().Set("X-Liffo-Route-Method", dec.Method)
	if dec.ChosenScore != 0 {
		w.Header().Set("X-Liffo-Chosen-Score", fmt.Sprintf("%.6f", dec.ChosenScore))
	}
	if dec.Reason != "" {
		w.Header().Set("X-Liffo-Route-Reason", dec.Reason)
	}
	if dec.Fallback != "" {
		w.Header().Set("X-Liffo-Fallback", dec.Fallback)
	}
	_ = healthAtPick // detailed worker health is retained only in server-side traces.
	w.Header().Set("X-Liffo-Timing-Queue-Ms", fmt.Sprintf("%.3f", queueMs))
	w.Header().Set("X-Liffo-Timing-Route-Ms", fmt.Sprintf("%.3f", routeMs))
	w.Header().Set("X-Liffo-Timing-Artificial-Ms", fmt.Sprintf("%d", artMs))
	// SEPARATED metrics
	w.Header().Set("X-Liffo-Timing-Network-Rtt-Ms", fmt.Sprintf("%.3f", t.NetworkRTTMs))
	w.Header().Set("X-Liffo-Timing-Network-One-Way-Ms", fmt.Sprintf("%.3f", t.NetworkOneWayMs))
	w.Header().Set("X-Liffo-Timing-Network-Dial-Ms", fmt.Sprintf("%.3f", t.NetworkDialMs))
	w.Header().Set("X-Liffo-Timing-Backend-Ttft-Ms", fmt.Sprintf("%.3f", t.BackendTTFTMs))
	w.Header().Set("X-Liffo-Timing-Ttfb-After-Write-Ms", fmt.Sprintf("%.3f", t.TTFBAfterWriteMs))
	w.Header().Set("X-Liffo-Timing-First-Content-After-Write-Ms", fmt.Sprintf("%.3f", t.FirstContentAfterWriteMs))
	w.Header().Set("X-Liffo-Timing-Upstream-Header-Ms", fmt.Sprintf("%.3f", t.UpstreamTotalHeaderMs))
	// Back-compat aliases (explicit names, not mixed)
	w.Header().Set("X-Liffo-Timing-Network-Ms", fmt.Sprintf("%.3f", t.NetworkRTTMs))
	w.Header().Set("X-Liffo-Timing-Backend-Ttfb-Ms", fmt.Sprintf("%.3f", t.BackendTTFTMs))
	if t.ProbeError != "" {
		w.Header().Set("X-Liffo-Timing-Probe-Error", t.ProbeError)
	}
}

func extractUserContent(msgs []map[string]any) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if role, _ := msgs[i]["role"].(string); role == "user" {
			if c, ok := msgs[i]["content"].(string); ok {
				return c
			}
		}
	}
	if len(msgs) > 0 {
		if c, ok := msgs[len(msgs)-1]["content"].(string); ok {
			return c
		}
	}
	return ""
}

func (h *HTTPServer) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ms := h.gw.ModelSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"lifecycle_wired": h.gw.Life() != nil,
		"model":           ms.Model,
		"state":           ms.State,
		"inflight":        ms.Inflight,
		"queue_depth":     ms.QueueDepth,
		"last_error":      ms.LastError,
		"load_gen":        ms.LoadGen,
	})
}

func (h *HTTPServer) handleModelsLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	coord := h.gw.Coordinator()
	life := h.gw.Life()
	if coord == nil && life == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error": "lifecycle not wired (no modellife.Manager on gateway)",
		})
		return
	}
	var body struct {
		Model  string `json:"model"`
		Worker string `json:"worker"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if body.Model == "" {
		http.Error(w, "model required", http.StatusBadRequest)
		return
	}
	if body.Worker != "" {
		if coord == nil {
			http.Error(w, "explicit worker placement requires the coordinator", http.StatusBadRequest)
			return
		}
		if err := coord.ValidateLoadTarget(body.Worker, body.Model); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"ok": false, "error": err.Error(), "model": body.Model, "worker": body.Worker,
			})
			return
		}
	}
	// Async-style: kick load; client can poll /v1/models. Also support sync via ?wait=1.
	wait := r.URL.Query().Get("wait") == "1" || r.URL.Query().Get("wait") == "true"
	if !wait {
		if coord != nil {
			jobID := fmt.Sprintf("load-%d", time.Now().UTC().UnixNano())
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				if body.Worker != "" {
					_, _ = coord.LoadOnWorker(ctx, body.Worker, body.Model)
				} else {
					_, _ = coord.Load(ctx, body.Model)
				}
			}()
			writeJSON(w, http.StatusAccepted, map[string]any{
				"accepted": true, "job_id": jobID, "model": body.Model, "worker": body.Worker,
			})
			return
		}
		// Non-blocking: queue job and return immediately with snapshot
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			_ = life.EnqueueLoad(ctx, body.Model)
			// Keep gateway config model label in sync on success
			if life.Snapshot().State == modellife.StateReady {
				h.gw.cfg.Model = life.Snapshot().Model
			}
		}()
		ms := life.Snapshot()
		writeJSON(w, http.StatusAccepted, map[string]any{
			"accepted":    true,
			"queued":      body.Model,
			"model_state": ms.State,
			"inflight":    ms.Inflight,
			"queue_depth": ms.QueueDepth,
			// Async acceptance can precede the transition to draining.
			"note": "async queue; drain while leases held. Proof path: hold stream + ?wait=1",
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	if coord != nil {
		var placed modellife.PlacementResult
		var err error
		if body.Worker != "" {
			placed, err = coord.LoadOnWorker(ctx, body.Worker, body.Model)
		} else {
			placed, err = coord.Load(ctx, body.Model)
		}
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, modellife.ErrUnknownModel) || errors.Is(err, modellife.ErrUnknownWorker) {
				status = http.StatusBadRequest
			}
			writeJSON(w, status, map[string]any{"ok": false, "error": err.Error(), "model": body.Model})
			return
		}
		h.gw.cfg.Model = body.Model
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "placement": placed})
		return
	}
	err := life.EnqueueLoad(ctx, body.Model)
	ms := life.Snapshot()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok": false, "error": err.Error(), "model_state": ms.State, "model": ms.Model,
		})
		return
	}
	h.gw.cfg.Model = ms.Model
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "model": ms.Model, "model_state": ms.State, "load_gen": ms.LoadGen,
	})
}

func (h *HTTPServer) handleModelsEvict(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	coord := h.gw.Coordinator()
	life := h.gw.Life()
	if coord == nil && life == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "lifecycle not wired"})
		return
	}
	wait := r.URL.Query().Get("wait") != "0"
	if coord != nil {
		var body struct {
			Worker string `json:"worker"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil || strings.TrimSpace(body.Worker) == "" {
			http.Error(w, "worker required", http.StatusBadRequest)
			return
		}
		if !wait {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				_ = coord.Evict(ctx, body.Worker)
			}()
			writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "worker": body.Worker})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
		defer cancel()
		if err := coord.Evict(ctx, body.Worker); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "worker": body.Worker})
		return
	}
	if !wait {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_ = life.EnqueueEvict(ctx)
		}()
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "model_state": life.Snapshot().State})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	err := life.EnqueueEvict(ctx)
	ms := life.Snapshot()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error(), "model_state": ms.State})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "model_state": ms.State})
}

func (h *HTTPServer) handleModelsEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if coord := h.gw.Coordinator(); coord != nil {
		writeJSON(w, http.StatusOK, map[string]any{"lifecycle_wired": true, "workers": coord.Status(), "events": coord.Events()})
		return
	}
	life := h.gw.Life()
	if life == nil {
		writeJSON(w, http.StatusOK, map[string]any{"lifecycle_wired": false, "events": []any{}})
		return
	}
	ev := life.Events()
	// optional tail
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 && n < len(ev) {
			ev = ev[len(ev)-n:]
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"lifecycle_wired": true,
		"count":           len(life.Events()),
		"events":          ev,
		"snapshot":        life.Snapshot(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		payload = []byte(`{"error":"response serialization failed","error_code":"internal_error"}`)
		code = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(payload, '\n'))
}

type routingTrace struct {
	DecisionID      string              `json:"decision_id"`
	WorkerID        int                 `json:"worker_id"`
	Method          string              `json:"method"`
	Fallback        string              `json:"fallback,omitempty"`
	Reason          string              `json:"reason,omitempty"`
	EstimatedTTFTMs float64             `json:"estimated_ttft_ms,omitempty"`
	EligibleWorkers []int               `json:"eligible_workers,omitempty"`
	RejectionCode   string              `json:"rejection_code,omitempty"`
	Scores          []route.WorkerScore `json:"scores,omitempty"`
}

func attachRoutingTelemetry(w http.ResponseWriter, span trace.Span, attrs *obs.RequestAttrs, dec route.Decision) {
	id := ""
	if span != nil {
		id = span.SpanContext().TraceID().String()
	}
	if id == "" || id == "00000000000000000000000000000000" {
		id = fmt.Sprintf("local-%d", time.Now().UnixNano())
	}
	attrs.RoutingDecisionID = id
	attrs.EstimatedTTFTMs = dec.EstimatedTTFTMs
	tracePayload := routingTrace{DecisionID: id, WorkerID: dec.WorkerID, Method: dec.Method, Fallback: dec.Fallback,
		Reason: dec.Reason, EstimatedTTFTMs: dec.EstimatedTTFTMs, EligibleWorkers: dec.EligibleWorkers,
		RejectionCode: dec.RejectionCode, Scores: dec.Scores}
	if b, err := json.Marshal(tracePayload); err == nil {
		attrs.RoutingJSON = string(b)
	} else {
		attrs.RoutingJSON = `{"serialization_error":true}`
	}
	w.Header().Set("X-Liffo-Decision-Id", id)
	if dec.EstimatedTTFTMs > 0 {
		w.Header().Set("X-Liffo-Estimated-Ttft-Ms", fmt.Sprintf("%.3f", dec.EstimatedTTFTMs))
	}
	if dec.RejectionCode != "" {
		w.Header().Set("X-Liffo-Rejection-Code", dec.RejectionCode)
	}
}

var _ = route.MethodHybrid

func (h *HTTPServer) handleRouteMethod(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"route_method": string(h.gw.Router().Method()),
			"multi_slo":    h.gw.Router().MultiSnapshot(),
		})
	case http.MethodPost:
		var body struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		m := route.Method(body.Method)
		h.gw.Router().SetMethod(m)
		// ensure multi state if switching to multi_slo
		if m == route.MethodMultiSLO && h.gw.Router().MultiSnapshot() == nil {
			h.gw.Router().EnableMulti(h.gw.cfg.MultiSLO, h.gw.cfg.WorkerStatic)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"route_method": string(h.gw.Router().Method()),
			"multi_slo":    h.gw.Router().MultiSnapshot(),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
func (h *HTTPServer) handleResetHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.gw.Router().ResetAllHealth()
	if h.gw.Router().MultiSnapshot() != nil {
		h.gw.Router().EnableMulti(h.gw.cfg.MultiSLO, h.gw.cfg.WorkerStatic)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"workers": h.gw.Router().SnapshotHealth(),
	})
}

func (h *HTTPServer) handleQualityFeedback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		WorkerID int      `json:"worker_id"`
		Quality  *float64 `json:"quality"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if body.Quality == nil || math.IsNaN(*body.Quality) || math.IsInf(*body.Quality, 0) || *body.Quality < 0 || *body.Quality > 1 {
		http.Error(w, "quality must be a float in [0..1]", http.StatusBadRequest)
		return
	}
	ws := h.gw.Router().Workers()
	if body.WorkerID < 0 || body.WorkerID >= len(ws) {
		http.Error(w, "worker_id out of range", http.StatusBadRequest)
		return
	}
	ok := h.gw.Router().ObserveQuality(body.WorkerID, *body.Quality)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "multi_slo routing state not enabled on router",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"worker_id": body.WorkerID,
		"quality":   *body.Quality,
		"multi_slo": h.gw.Router().MultiSnapshot(),
	})
}
