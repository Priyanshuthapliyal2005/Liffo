package adapt

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

func TestLifecycleMutationRequiresBearerToken(t *testing.T) {
	loader := modellife.NewFakeLoader()
	manager, err := modellife.New(modellife.Config{}, loader)
	if err != nil {
		t.Fatal(err)
	}
	coord, err := modellife.NewCoordinator([]*modellife.WorkerController{{
		ID: 0, Label: "managed", Models: map[string]bool{"model-a": true}, Manager: manager,
	}})
	if err != nil {
		t.Fatal(err)
	}
	hs, err := NewHTTPServer(ServeConfig{
		AdminToken: "test-secret",
		Gateway: GatewayConfig{
			Workers: []string{"http://127.0.0.1:1"},
			Initial: StaticPolicy(route.MethodRoundRobin, 1, false),
			Model:   "model-a", Coordinator: coord,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hs.srv.Handler)
	defer server.Close()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = coord.Shutdown(ctx)
	})

	post := func(token string) int {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/models/load?wait=1", bytes.NewBufferString(`{"model":"model-a"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := post(""); got != http.StatusUnauthorized {
		t.Fatalf("missing token status=%d", got)
	}
	if got := post("wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d", got)
	}
	if got := post("test-secret"); got != http.StatusOK {
		t.Fatalf("correct token status=%d", got)
	}

	req, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/models/load",
		bytes.NewBufferString(`{"model":"model-a","worker":"missing"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("async unknown worker status=%d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestManagedLifecycleFailsClosedWithoutToken(t *testing.T) {
	manager, _ := modellife.New(modellife.Config{}, modellife.NewFakeLoader())
	coord, _ := modellife.NewCoordinator([]*modellife.WorkerController{{
		ID: 0, Label: "managed", Models: map[string]bool{"model-a": true}, Manager: manager,
	}})
	_, err := NewHTTPServer(ServeConfig{
		Gateway: GatewayConfig{Workers: []string{"http://127.0.0.1:1"}, Coordinator: coord},
	})
	if err == nil {
		t.Fatal("expected missing admin token error")
	}
}
