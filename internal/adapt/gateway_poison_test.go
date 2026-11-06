package adapt

import (
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// TestPoisonWorkerURL_DoesNotPreMarkUnhealthy ensures soft disconnect leaves the
// worker Healthy until real dial/stream failures call Release(false).
func TestPoisonWorkerURL_DoesNotPreMarkUnhealthy(t *testing.T) {
	gw, err := NewGateway(GatewayConfig{
		Mode:    ModeStatic,
		Workers: []string{"http://127.0.0.1:18081", "http://127.0.0.1:18082"},
		Initial: StaticPolicy(route.MethodRoundRobin, 2, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gw.WorkerStillHealthy(0) {
		t.Fatal("worker0 should start healthy")
	}
	orig := gw.Router().Workers()[0].BaseURL
	gw.PoisonWorkerURL(0)
	if gw.Router().Workers()[0].BaseURL == orig {
		t.Fatal("expected BaseURL rewritten to dead address")
	}
	if !gw.WorkerStillHealthy(0) {
		t.Fatal("PoisonWorkerURL must not pre-MarkFail (injected worker failure)")
	}
	// MarkFail remains available for real Release(false) / explicit cooldown.
	gw.Router().Workers()[0].MarkFail(2 * time.Second)
	if gw.WorkerStillHealthy(0) {
		t.Fatal("MarkFail should make unhealthy")
	}
	gw.RestoreWorkerURL(0, orig)
	if !gw.WorkerStillHealthy(0) {
		t.Fatal("RestoreWorkerURL should clear unhealthy")
	}
	if gw.Router().Workers()[0].BaseURL != orig {
		t.Fatalf("url=%s want %s", gw.Router().Workers()[0].BaseURL, orig)
	}
}
