package adapt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

func TestHTTPQualityCostConstraintsAndUpstreamCompatibility(t *testing.T) {
	var calls [2]atomic.Int32
	upstream := make([]*httptest.Server, 2)
	for i := range upstream {
		n := i
		upstream[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path != "/v1/chat/completions" {
				http.NotFound(w, r)
				return
			}
			calls[n].Add(1)
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("upstream JSON: %v", err)
			}
			if _, ok := payload["liffo_route"]; ok {
				t.Error("gateway routing controls leaked upstream")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
		}))
		defer upstream[i].Close()
	}
	sc := testServeConfig([]string{upstream[0].URL, upstream[1].URL}, 4, 0, nil)
	sc.Gateway.Initial.RouteMethod = route.MethodMultiSLO
	sc.Gateway.WorkerStatic = []route.WorkerStatic{{CostPerReq: 0.2, QualityTier: 0.4}, {CostPerReq: 1, QualityTier: 1}}
	sc.Gateway.MultiSLO = route.MultiSLOConfig{WeightCost: 1, SLOTargetMs: 150}
	hs, err := NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hs.srv.Handler)
	defer srv.Close()
	submit := func(limits string) (*http.Response, string) {
		t.Helper()
		body := `{"model":"local","messages":[{"role":"user","content":"ping"}],"stream":false,"liffo_route":` + limits + `}`
		resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp, string(raw)
	}
	response, _ := submit(`{"min_quality":0.9}`)
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Liffo-Worker-Id") != "1" {
		t.Fatalf("quality route: status=%d worker=%s", response.StatusCode, response.Header.Get("X-Liffo-Worker-Id"))
	}
	response, _ = submit(`{"max_cost_per_req":0.3}`)
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Liffo-Worker-Id") != "0" {
		t.Fatalf("cost route: status=%d worker=%s", response.StatusCode, response.Header.Get("X-Liffo-Worker-Id"))
	}
	response, raw := submit(`{"min_quality":0.9,"max_cost_per_req":0.3}`)
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(raw, "constraints_unavailable") {
		t.Fatalf("infeasible route: status=%d body=%s", response.StatusCode, raw)
	}
	response, _ = submit(`{"min_quality":1.5}`)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid quality: status=%d", response.StatusCode)
	}
	if calls[0].Load() != 1 || calls[1].Load() != 1 {
		t.Fatalf("unexpected upstream calls %d,%d", calls[0].Load(), calls[1].Load())
	}
}

func TestHTTPResetHealthAlsoResetsMultiSLOState(t *testing.T) {
	sc := testServeConfig([]string{"http://127.0.0.1:1", "http://127.0.0.1:2"}, 4, 0, nil)
	sc.Gateway.Initial.RouteMethod = route.MethodMultiSLO
	sc.Gateway.WorkerStatic = []route.WorkerStatic{{QualityTier: 0.4}, {QualityTier: 0.9}}
	sc.Gateway.MultiSLO = route.MultiSLOConfig{WeightQuality: 1, SLOTargetMs: 150, LatencyEMAAlpha: 0.5}
	hs, err := NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hs.srv.Handler)
	defer srv.Close()

	feedback, err := http.Post(srv.URL+"/v1/quality-feedback", "application/json", strings.NewReader(`{"worker_id":0,"quality":0.0}`))
	if err != nil {
		t.Fatal(err)
	}
	feedback.Body.Close()
	if feedback.StatusCode != http.StatusOK {
		t.Fatalf("quality feedback status=%d", feedback.StatusCode)
	}

	before, err := http.Get(srv.URL + "/v1/route-method")
	if err != nil {
		t.Fatal(err)
	}
	var beforeState struct {
		Multi []map[string]any `json:"multi_slo"`
	}
	if err := json.NewDecoder(before.Body).Decode(&beforeState); err != nil {
		t.Fatal(err)
	}
	before.Body.Close()
	if got := beforeState.Multi[0]["n_quality_obs"].(float64); got != 1 {
		t.Fatalf("feedback did not enter multi state: n_quality_obs=%v", got)
	}

	reset, err := http.Post(srv.URL+"/v1/workers/reset-health", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	reset.Body.Close()
	if reset.StatusCode != http.StatusOK {
		t.Fatalf("reset status=%d", reset.StatusCode)
	}

	after, err := http.Get(srv.URL + "/v1/route-method")
	if err != nil {
		t.Fatal(err)
	}
	var afterState struct {
		Multi []map[string]any `json:"multi_slo"`
	}
	if err := json.NewDecoder(after.Body).Decode(&afterState); err != nil {
		t.Fatal(err)
	}
	after.Body.Close()
	if got := afterState.Multi[0]["n_quality_obs"].(float64); got != 0 {
		t.Fatalf("reset retained quality feedback: n_quality_obs=%v", got)
	}
	if got := afterState.Multi[0]["ema_quality"].(float64); got != 0.4 {
		t.Fatalf("reset did not restore configured quality tier: ema_quality=%v", got)
	}
}

func TestHTTPQualityFeedbackEndpointAndDynamicAdaptation(t *testing.T) {
	upstream := make([]*httptest.Server, 2)
	for i := range upstream {
		upstream[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"response"}}]}`)
		}))
		defer upstream[i].Close()
	}
	sc := testServeConfig([]string{upstream[0].URL, upstream[1].URL}, 4, 0, nil)
	sc.Gateway.Initial.RouteMethod = route.MethodMultiSLO
	// Worker 0: high prior (0.95), Worker 1: lower prior (0.80)
	sc.Gateway.WorkerStatic = []route.WorkerStatic{
		{CostPerReq: 0.5, QualityTier: 0.95, LatencyPriorMs: 40},
		{CostPerReq: 0.5, QualityTier: 0.80, LatencyPriorMs: 40},
	}
	sc.Gateway.MultiSLO = route.MultiSLOConfig{WeightQuality: 0.8, WeightLatency: 0.1, WeightCost: 0.1, SLOTargetMs: 150, LatencyEMAAlpha: 0.5}
	hs, err := NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hs.srv.Handler)
	defer srv.Close()

	// Initial request picks worker 0 due to 0.95 prior
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"local","messages":[{"role":"user","content":"test"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Liffo-Worker-Id") != "0" {
		t.Fatalf("initial route should pick worker 0, got status=%d worker=%s", resp.StatusCode, resp.Header.Get("X-Liffo-Worker-Id"))
	}
	resp.Body.Close()

	// Send feedback: worker 0 failed verification (quality 0.0)
	fbResp, err := http.Post(srv.URL+"/v1/quality-feedback", "application/json", strings.NewReader(`{"worker_id":0,"quality":0.0}`))
	if err != nil {
		t.Fatal(err)
	}
	if fbResp.StatusCode != http.StatusOK {
		t.Fatalf("feedback status: %d", fbResp.StatusCode)
	}
	var fbData map[string]any
	json.NewDecoder(fbResp.Body).Decode(&fbData)
	fbResp.Body.Close()
	if fbData["status"] != "ok" {
		t.Fatalf("feedback response not ok: %+v", fbData)
	}

	// Send feedback: worker 1 passed verification (quality 1.0)
	fbResp2, _ := http.Post(srv.URL+"/v1/quality-feedback", "application/json", strings.NewReader(`{"worker_id":1,"quality":1.0}`))
	fbResp2.Body.Close()

	// Send more failure feedback for worker 0 to solidify degradation
	fbResp3, _ := http.Post(srv.URL+"/v1/quality-feedback", "application/json", strings.NewReader(`{"worker_id":0,"quality":0.0}`))
	fbResp3.Body.Close()

	// Subsequent request must now dynamically route to worker 1
	resp2, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"local","messages":[{"role":"user","content":"test"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusOK || resp2.Header.Get("X-Liffo-Worker-Id") != "1" {
		t.Fatalf("adapted route should pick worker 1, got status=%d worker=%s", resp2.StatusCode, resp2.Header.Get("X-Liffo-Worker-Id"))
	}
	resp2.Body.Close()

	// Validation tests on feedback endpoint
	invalidCases := []string{
		`{"worker_id":-1,"quality":0.5}`,
		`{"worker_id":2,"quality":0.5}`,
		`{"worker_id":0,"quality":-0.5}`,
		`{"worker_id":0,"quality":1.5}`,
		`{"worker_id":0}`,
		`not json`,
	}
	for _, ic := range invalidCases {
		r, _ := http.Post(srv.URL+"/v1/quality-feedback", "application/json", strings.NewReader(ic))
		if r.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid payload %s, got %d", ic, r.StatusCode)
		}
		r.Body.Close()
	}

	// GET method not allowed
	getResp, _ := http.Get(srv.URL + "/v1/quality-feedback")
	if getResp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET feedback, got %d", getResp.StatusCode)
	}
	getResp.Body.Close()
}

func TestRoutingTelemetryIsServerSideAndPublicHeadersAreBounded(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer upstream.Close()

	tracePath := t.TempDir() + "/routing-trace.json"
	sc := testServeConfig([]string{upstream.URL}, 4, 0, nil)
	sc.Gateway.Initial.RouteMethod = route.MethodMultiSLO
	sc.Gateway.WorkerStatic = []route.WorkerStatic{{CostPerReq: 1, LatencyPriorMs: 80, QualityTier: 1}}
	sc.Gateway.MultiSLO = route.MultiSLOConfig{WeightLatency: 1, SLOTargetMs: 100, LatencyEMAAlpha: 0.3}
	sc.TraceJSONL = tracePath
	hs, err := NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hs.srv.Handler)
	defer srv.Close()

	secret := "private prompt must never enter routing telemetry"
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"local","messages":[{"role":"user","content":"`+secret+`"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Liffo-Decision-Id") == "" {
		t.Fatal("missing correlation decision id")
	}
	if resp.Header.Get("X-Liffo-Estimated-Ttft-Ms") == "" {
		t.Fatal("missing EMA latency estimate")
	}
	if routeMs, err := strconv.ParseFloat(resp.Header.Get("X-Liffo-Timing-Route-Ms"), 64); err != nil || routeMs < 0 {
		t.Fatalf("missing or invalid route timing header: %q err=%v", resp.Header.Get("X-Liffo-Timing-Route-Ms"), err)
	}
	for _, leaked := range []string{"X-Liffo-Scores", "X-Liffo-Workers-Health", "X-Liffo-Worker-URL"} {
		if got := resp.Header.Get(leaked); got != "" {
			t.Fatalf("public response leaked internal routing telemetry %s=%q", leaked, got)
		}
	}
	obsResp, err := http.Get(srv.URL + "/v1/obs/samples")
	if err != nil {
		t.Fatal(err)
	}
	obsBody, _ := io.ReadAll(obsResp.Body)
	obsResp.Body.Close()
	if strings.Contains(string(obsBody), `"routing"`) || strings.Contains(string(obsBody), secret) {
		t.Fatalf("public samples leaked detailed routing telemetry: %s", obsBody)
	}

	if err := hs.obs.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(trace), "liffo.routing.decision") || !strings.Contains(string(trace), "estimated_ttft_ms") {
		t.Fatalf("trace lacks structured routing telemetry: %s", trace)
	}
	if strings.Contains(string(trace), secret) || strings.Contains(string(trace), "prompt_sha256") || strings.Contains(string(trace), "prefix_sha256") || strings.Contains(string(trace), upstream.URL) {
		t.Fatalf("server-side routing trace leaked prompt hashes or upstream URL: %s", trace)
	}

	// Unauthenticated forced-worker header must be rejected
	reqForce, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"local","messages":[{"role":"user","content":"ping"}]}`))
	reqForce.Header.Set("Content-Type", "application/json")
	reqForce.Header.Set("X-Liffo-Force-Worker-Id", "0")
	respForce, err := http.DefaultClient.Do(reqForce)
	if err != nil {
		t.Fatal(err)
	}
	respForce.Body.Close()
	if respForce.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated force worker header, got %d", respForce.StatusCode)
	}
}
