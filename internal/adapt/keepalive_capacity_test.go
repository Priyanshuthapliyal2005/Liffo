package adapt

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// TestGatewayReusesUpstreamConnections proves keep-alives are enabled on the
// gateway's upstream client: N sequential requests must not open N upstream TCP
// connections. With DisableKeepAlives:true this test sees newConns == requests.
func TestGatewayReusesUpstreamConnections(t *testing.T) {
	var newConns int64
	w := slowSSEWorker(1*time.Millisecond, &newConns)
	defer w.Close()

	hs, err := NewHTTPServer(ServeConfig{
		Listen: "127.0.0.1:0",
		Labels: []string{"w0"},
		Gateway: GatewayConfig{
			Mode:         ModeStatic,
			Workers:      []string{w.URL},
			Initial:      StaticPolicy(route.MethodLeastInflight, 1, false),
			AdmitTimeout: 3 * time.Second,
			MaxTokens:    2,
			Model:        "mock",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs.srv.Handler)
	defer ts.Close()

	const n = 8
	body := `{"model":"mock","stream":true,"max_tokens":2,"messages":[{"role":"user","content":"hi"}]}`
	for i := 0; i < n; i++ {
		resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}
	got := atomic.LoadInt64(&newConns)
	if got >= n {
		t.Fatalf("upstream opened %d new connections for %d sequential requests: keep-alives are disabled", got, n)
	}
}

// TestCapacityAwareMethodEnablesMultiState proves the gateway wires multi state
// for capacity-aware policies. Without it, capacityAndServiceEstimate returns
// (1,1) for every worker and the policy silently degrades to least_inflight.
func TestCapacityAwareMethodEnablesMultiState(t *testing.T) {
	w := slowSSEWorker(1*time.Millisecond, nil)
	defer w.Close()
	g, err := NewGateway(GatewayConfig{
		Mode:    ModeStatic,
		Workers: []string{w.URL, w.URL},
		Initial: StaticPolicy(route.MethodCapacityNormalizedLeastInflight, 4, false),
		Model:   "mock",
		WorkerStatic: []route.WorkerStatic{
			{Capacity: 1, LatencyPriorMs: 50, CostPerReq: 1, QualityTier: 1},
			{Capacity: 8, LatencyPriorMs: 50, CostPerReq: 1, QualityTier: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := g.Router()
	r.Acquire(0)
	r.Acquire(1)
	d := r.Pick("p")
	if d.WorkerID != 1 {
		t.Fatalf("chose worker %d; capacity metadata was not wired into the router", d.WorkerID)
	}
}
