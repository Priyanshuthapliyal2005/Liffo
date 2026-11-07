package obs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewTelemetryAndLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	jsonlPath := filepath.Join(tmpDir, "traces.jsonl")

	tel, err := New(Config{
		TraceJSONL:  jsonlPath,
		ServiceName: "test-gateway",
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tel.Shutdown(ctx)
	}()

	if tel.Registry == nil {
		t.Fatal("expected non-nil Registry")
	}
	if tel.Tracer == nil {
		t.Fatal("expected non-nil Tracer")
	}

	// Start and finish a request
	ctx, span := tel.StartRequest(context.Background(), "/v1/chat/completions")
	if ctx == nil || span == nil {
		t.Fatal("StartRequest returned nil ctx or span")
	}

	tel.IncQueueWaiting(1)
	tel.ObserveQueueWait("admitted", 0.015)
	tel.IncQueueWaiting(-1)
	tel.SetInflight(1)

	tel.RecordRoute("worker-1", "round_robin", "", "least_inflight")
	tel.RecordRoute("worker-2", "round_robin", "cold_fallback", "overload")
	tel.RecordReject("queue_full")

	tel.FinishRequest(span, RequestAttrs{
		Worker:      "worker-1",
		RouteMethod: "round_robin",
		Fallback:    "none",
		Reason:      "least_inflight",
		Model:       "test-model",
		ErrorCode:   "",
		ErrorClass:  "",
		Status:      "ok",
		QueueWaitMs: 15.0,
		TTFTMs:      45.0,
		LatencyMs:   120.0,
		HTTPStatus:  http.StatusOK,
	})

	tel.SetInflight(0)

	// Test error request finish
	_, errSpan := tel.StartRequest(context.Background(), "/v1/chat/completions")
	tel.FinishRequest(errSpan, RequestAttrs{
		Worker:      "worker-2",
		RouteMethod: "round_robin",
		Fallback:    "fallback",
		Reason:      "worker_timeout",
		Model:       "test-model",
		ErrorCode:   "upstream_timeout",
		ErrorClass:  "timeout",
		Status:      "error",
		QueueWaitMs: 5.0,
		TTFTMs:      0,
		LatencyMs:   500.0,
		HTTPStatus:  http.StatusGatewayTimeout,
	})

	// Check SpanSamples
	samples := tel.SpanSamples()
	if len(samples) != 2 {
		t.Errorf("expected 2 span samples, got %d", len(samples))
	}

	// Check model state
	tel.SetModelState("loading", "test-model", 0, 1)
	tel.SetModelState("ready", "test-model", 0, 0)
	tel.ObserveModelTransition("ready", "draining", "test-model")

	// Check worker gauges
	tel.SetWorkerGauges("worker-1", 2.0, true)
	tel.SetWorkerGauges("worker-2", 0.0, false)

	// Check GatherText
	text, err := tel.GatherText()
	if err != nil {
		t.Fatalf("GatherText() error: %v", err)
	}
	if !strings.Contains(text, "liffo_requests_total") {
		t.Errorf("GatherText output missing liffo_requests_total: %s", text)
	}

	// Check SnapshotKeyValues
	kv := tel.SnapshotKeyValues()
	if len(kv) == 0 {
		t.Error("expected non-empty SnapshotKeyValues")
	}

	// Check HTTP handler
	handler := tel.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("metrics handler returned %d, want 200", rr.Code)
	}
}

func TestNilTelemetryGuards(t *testing.T) {
	var tel *Telemetry
	// Should not panic on nil receiver
	ctx, span := tel.StartRequest(context.Background(), "/test")
	if ctx == nil {
		t.Error("expected non-nil ctx from nil Telemetry")
	}
	tel.ObserveQueueWait("admitted", 1.0)
	tel.IncQueueWaiting(1)
	tel.SetInflight(1)
	tel.RecordRoute("w1", "rr", "", "")
	tel.RecordReject("full")
	tel.FinishRequest(span, RequestAttrs{})
	if samples := tel.SpanSamples(); samples != nil {
		t.Error("expected nil samples from nil Telemetry")
	}
	tel.SetModelState("ready", "m1", 0, 0)
	tel.ObserveModelTransition("a", "b", "m1")
	tel.SetWorkerGauges("w1", 1.0, true)
	if _, err := tel.GatherText(); err == nil {
		t.Error("expected error from GatherText on nil Telemetry")
	}
	if kv := tel.SnapshotKeyValues(); len(kv) != 0 {
		t.Error("expected empty kv from nil Telemetry")
	}
	_ = tel.Shutdown(context.Background())
}

func TestReasonClass(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"", "none"},
		{"queue overload detected", "overload"},
		{"unhealthy backend", "unhealthy"},
		{"least inflight pick", "least_inflight"},
		{"sticky session match", "sticky"},
		{"multi_slo balanced", "multi_slo"},
		{"cost target", "multi_slo"},
		{"force worker", "forced"},
		{"fallback to secondary", "fallback"},
		{"custom reason code", "custom_reason_code"},
	}

	for _, tc := range cases {
		got := reasonClass(tc.input)
		if got != tc.want {
			t.Errorf("reasonClass(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestDirOf(t *testing.T) {
	if got := dirOf("a/b/c.json"); got != "a/b" {
		t.Errorf("dirOf(a/b/c.json) = %q, want a/b", got)
	}
	if got := dirOf("foo.json"); got != "." {
		t.Errorf("dirOf(foo.json) = %q, want .", got)
	}
}
