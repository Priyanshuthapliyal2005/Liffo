package modellife

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestBearerAuthorized(t *testing.T) {
	if !BearerAuthorized("Bearer correct-token", "correct-token") {
		t.Fatal("correct bearer token rejected")
	}
	for _, header := range []string{"", "correct-token", "Bearer wrong-token", "Basic correct-token"} {
		if BearerAuthorized(header, "correct-token") {
			t.Fatalf("authorized invalid header %q", header)
		}
	}
	if BearerAuthorized("Bearer anything", "") {
		t.Fatal("empty expected token must fail closed")
	}
}

func TestLlamaLoaderRejectsUnknownAndPathShapedModelIDs(t *testing.T) {
	loader := &LlamaServerLoader{Models: map[string]string{"safe": "safe.gguf"}}
	for _, model := range []string{"unknown", "../safe.gguf", `C:\models\safe.gguf`, `\\server\share\safe.gguf`} {
		_, err := loader.resolvePath(model)
		if !errors.Is(err, ErrUnknownModel) {
			t.Fatalf("model=%q err=%v want ErrUnknownModel", model, err)
		}
	}
}

func TestValidateModelPathConfinedToRoot(t *testing.T) {
	root := t.TempDir()
	model := filepath.Join(root, "safe.gguf")
	if err := os.WriteFile(model, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ValidateModelPath(root, "safe.gguf")
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(model)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	outside := filepath.Join(filepath.Dir(root), "outside.gguf")
	if err := os.WriteFile(outside, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(outside) })
	if _, err := ValidateModelPath(root, filepath.Join("..", "outside.gguf")); err == nil {
		t.Fatal("traversal path accepted")
	}
	if runtime.GOOS == "windows" {
		if _, err := ValidateModelPath(root, `\\server\share\model.gguf`); err == nil {
			t.Fatal("UNC path accepted")
		}
	}
}

func TestCoordinatorPlacesByHealthLoadLatencyAndCost(t *testing.T) {
	makeWorker := func(id int, label string, rt RuntimeState, cost float64) (*WorkerController, *FakeLoader) {
		loader := NewFakeLoader()
		manager, err := New(Config{LoadTimeout: time.Second, EvictTimeout: time.Second}, loader)
		if err != nil {
			t.Fatal(err)
		}
		return &WorkerController{
			ID: id, Label: label, Models: map[string]bool{"model-a": true},
			Manager: manager, Cost: cost, Runtime: func() RuntimeState { return rt },
		}, loader
	}
	slow, slowLoader := makeWorker(0, "slow", RuntimeState{Healthy: true, Inflight: 3, LatencyMs: 90}, 1)
	fast, fastLoader := makeWorker(1, "fast", RuntimeState{Healthy: true, Inflight: 0, LatencyMs: 20}, 0.5)
	coord, err := NewCoordinator([]*WorkerController{slow, fast})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = coord.Shutdown(ctx)
	})
	placed, err := coord.Load(context.Background(), "model-a")
	if err != nil {
		t.Fatal(err)
	}
	if placed.WorkerID != 1 || fastLoader.Loads.Load() != 1 {
		t.Fatalf("placement=%+v fast_loads=%d", placed, fastLoader.Loads.Load())
	}
	targeted, err := coord.LoadOnWorker(context.Background(), "slow", "model-a")
	if err != nil {
		t.Fatal(err)
	}
	if targeted.WorkerID != 0 || slowLoader.Loads.Load() != 1 {
		t.Fatalf("targeted=%+v slow_loads=%d", targeted, slowLoader.Loads.Load())
	}
	if _, err := coord.LoadOnWorker(context.Background(), "missing", "model-a"); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("unknown worker err=%v want ErrUnknownWorker", err)
	}
	if _, err := coord.Load(context.Background(), "../model.gguf"); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("path-shaped id err=%v", err)
	}
}

func TestManagerShutdownUnloadsResidentModel(t *testing.T) {
	loader := NewFakeLoader()
	manager, err := New(Config{}, loader)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.EnqueueLoad(context.Background(), "model-a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if loader.Unloads.Load() != 1 || loader.Resident() != "" {
		t.Fatalf("unloads=%d resident=%q", loader.Unloads.Load(), loader.Resident())
	}
}
