package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEndpointJoin(t *testing.T) {
	cases := []struct {
		base, path, want string
	}{
		{"http://127.0.0.1:8080", "/v1/chat/completions", "http://127.0.0.1:8080/v1/chat/completions"},
		{"http://127.0.0.1:8080/", "/v1/chat/completions", "http://127.0.0.1:8080/v1/chat/completions"},
		{"http://127.0.0.1:8080", "v1/chat/completions", "http://127.0.0.1:8080/v1/chat/completions"},
		{"http://127.0.0.1:8080/", "", "http://127.0.0.1:8080/v1/chat/completions"},
	}
	for _, tc := range cases {
		c := Config{BaseURL: tc.base, EndpointPath: tc.path}
		if got := c.Endpoint(); got != tc.want {
			t.Fatalf("base=%q path=%q got=%q want=%q", tc.base, tc.path, got, tc.want)
		}
	}
}

func TestDefaultsTimeout(t *testing.T) {
	c := Defaults()
	if c.Timeout().Seconds() != 120 {
		t.Fatalf("timeout=%v", c.Timeout())
	}
	if c.SampleInterval() != 200*time.Millisecond {
		t.Fatalf("sample interval=%v, want 200ms", c.SampleInterval())
	}
	c.SampleIntervalMs = 500
	if c.SampleInterval() != 500*time.Millisecond {
		t.Fatalf("sample interval=%v, want 500ms", c.SampleInterval())
	}
}

func TestTotals(t *testing.T) {
	c := Config{
		Iterations: 3,
		Prompts:    []Prompt{{ID: "p1"}, {ID: "p2"}},
		Warmup:     2,
	}
	if got := c.MeasuredTotal(); got != 6 {
		t.Errorf("MeasuredTotal = %d, want 6", got)
	}
	c.TotalRequests = 10
	if got := c.MeasuredTotal(); got != 10 {
		t.Errorf("MeasuredTotal = %d, want 10", got)
	}

	c.Concurrency = 1
	if got := c.WarmupTotal(); got != 4 {
		t.Errorf("WarmupTotal (concurrency=1) = %d, want 4", got)
	}
	c.Concurrency = 4
	if got := c.WarmupTotal(); got != 8 {
		t.Errorf("WarmupTotal (concurrency=4) = %d, want 8", got)
	}
	c.Warmup = 0
	if got := c.WarmupTotal(); got != 0 {
		t.Errorf("WarmupTotal (warmup=0) = %d, want 0", got)
	}
}

func TestLoadYAMLAndApplyRunFlags(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test-bench.yaml")
	yamlContent := `
base_url: "http://127.0.0.1:9000"
model: "test-model"
mode: "cpu"
iterations: 5
warmup: 1
max_tokens: 32
concurrency: 2
prompts:
  - id: "p1"
    messages:
      - role: "user"
        content: "test"
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	loaded, err := LoadYAML(cfgPath)
	if err != nil {
		t.Fatalf("LoadYAML failed: %v", err)
	}
	if loaded.BaseURL != "http://127.0.0.1:9000" || loaded.Model != "test-model" {
		t.Errorf("unexpected loaded config: %+v", loaded)
	}

	empty, err := LoadYAML("")
	if err != nil || empty.BaseURL == "" {
		t.Errorf("LoadYAML('') failed or empty: %+v", empty)
	}

	var cfg Config
	args := []string{
		"-config", cfgPath,
		"-base-url", "http://override:8000",
		"-mode", "gpu",
		"-iterations", "10",
		"-concurrency", "4",
		"-max-tokens", "128",
		"-timeout-sec", "60",
		"-total-requests", "20",
		"-server-pid", "1234",
		"-run-id", "run-42",
		"-out", filepath.Join(tmpDir, "out"),
	}
	if err := ApplyRunFlags(&cfg, args); err != nil {
		t.Fatalf("ApplyRunFlags failed: %v", err)
	}
	if cfg.BaseURL != "http://override:8000" || cfg.Mode != "gpu" || cfg.Iterations != 10 {
		t.Errorf("ApplyRunFlags did not apply overrides properly: %+v", cfg)
	}
	if cfg.ServerPID != 1234 || cfg.RunID != "run-42" {
		t.Errorf("ApplyRunFlags metadata mismatch: %+v", cfg)
	}
}
