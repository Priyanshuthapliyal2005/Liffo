package route

import (
	"sync"
	"testing"
)

// Control policy changes must be safe while the gateway is routing requests.
func TestConcurrentMultiConfigAndObservations(t *testing.T) {
	workers := []*Worker{{ID: 0, BaseURL: "http://a"}, {ID: 1, BaseURL: "http://b"}}
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, workers, defaultMultiSLO(), nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			newCfg := defaultMultiSLO()
			newCfg.WeightCost = float64(i%3) / 3
			r.SetMultiSLOConfig(newCfg)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			r.ObserveLatency(i%2, float64(i+1), true)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			r.ObserveQuality(i%2, float64(i%11)/10.0)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			d := r.Pick("prompt")
			if d.WorkerID < 0 || d.WorkerID > 1 {
				t.Errorf("invalid decision %+v", d)
			}
		}
	}()
	wg.Wait()
}

func TestConcurrentMethodSwitchAndRouting(t *testing.T) {
	workers := []*Worker{{ID: 0, BaseURL: "http://a"}, {ID: 1, BaseURL: "http://b"}}
	r, err := NewWithMulti(Defaults(), workers, defaultMultiSLO(), nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			r.SetMethod(MethodMultiSLO)
			r.SetMethod(MethodLeastInflight)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			d := r.PickWithConstraints("prompt", nil)
			if d.WorkerID < 0 || d.WorkerID > 1 {
				t.Errorf("invalid decision %+v", d)
			}
		}
	}()
	wg.Wait()
}
