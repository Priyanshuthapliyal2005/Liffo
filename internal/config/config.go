package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Message struct {
	Role    string `yaml:"role" json:"role"`
	Content string `yaml:"content" json:"content"`
}

type Prompt struct {
	ID       string    `yaml:"id" json:"id"`
	Messages []Message `yaml:"messages" json:"messages"`
}

type Config struct {
	BaseURL      string  `yaml:"base_url"`
	EndpointPath string  `yaml:"endpoint_path"`
	Model        string  `yaml:"model"`
	Mode         string  `yaml:"mode"`
	Stream       bool    `yaml:"stream"`
	Iterations   int     `yaml:"iterations"`
	Warmup       int     `yaml:"warmup"`
	MaxTokens    int     `yaml:"max_tokens"`
	Temperature  float64 `yaml:"temperature"`
	TimeoutSec   int     `yaml:"timeout_sec"`
	Concurrency  int     `yaml:"concurrency"`
	// TotalRequests is measured request count for load runs. 0 = iterations * len(prompts).
	TotalRequests       int      `yaml:"total_requests"`
	SampleIntervalMs    int      `yaml:"sample_interval_ms"`
	StorePromptText     bool     `yaml:"store_prompt_text"`
	StoreCompletionText bool     `yaml:"store_completion_text"`
	Prompts             []Prompt `yaml:"prompts"`
	OutDir              string   `yaml:"-"`
	ServerPID           int      `yaml:"-"`
	RunID               string   `yaml:"-"`
}

func Defaults() Config {
	return Config{
		BaseURL:          "http://127.0.0.1:8080",
		EndpointPath:     "/v1/chat/completions",
		Model:            "local",
		Mode:             "gpu",
		Stream:           true,
		Iterations:       3,
		Warmup:           1,
		MaxTokens:        64,
		Temperature:      0,
		TimeoutSec:       120,
		Concurrency:      1,
		SampleIntervalMs: 200,
		OutDir:           "results/raw",
	}
}

func LoadYAML(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) Endpoint() string {
	base := strings.TrimRight(c.BaseURL, "/")
	path := c.EndpointPath
	if path == "" {
		path = "/v1/chat/completions"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}

func (c *Config) Timeout() time.Duration {
	return time.Duration(c.TimeoutSec) * time.Second
}

func (c *Config) SampleInterval() time.Duration {
	ms := c.SampleIntervalMs
	if ms <= 0 {
		ms = 200
	}
	return time.Duration(ms) * time.Millisecond
}

// ApplyRunFlags merges CLI flags for the "run" subcommand.
func ApplyRunFlags(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "configs/bench.yaml", "path to bench YAML")
	baseURL := fs.String("base-url", "", "override base URL")
	mode := fs.String("mode", "", "cpu|gpu label")
	out := fs.String("out", "", "raw results directory")
	model := fs.String("model", "", "model name sent to API")
	iters := fs.Int("iterations", -1, "iterations per prompt")
	warmup := fs.Int("warmup", -1, "warmup iterations (not recorded)")
	maxTokens := fs.Int("max-tokens", -1, "max_tokens")
	timeout := fs.Int("timeout-sec", -1, "HTTP timeout seconds")
	concurrency := fs.Int("concurrency", -1, "parallel in-flight requests")
	totalReq := fs.Int("total-requests", -1, "total measured requests (0=iterations*prompts)")
	serverPID := fs.Int("server-pid", 0, "optional llama-server PID for RAM sampling")
	runID := fs.String("run-id", "", "optional run id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	loaded, err := LoadYAML(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	*cfg = loaded
	if *baseURL != "" {
		cfg.BaseURL = *baseURL
	}
	if *mode != "" {
		cfg.Mode = *mode
	}
	if *out != "" {
		cfg.OutDir = *out
	} else if cfg.OutDir == "" {
		cfg.OutDir = "results/raw"
	}
	if *model != "" {
		cfg.Model = *model
	}
	if *iters >= 0 {
		cfg.Iterations = *iters
	}
	if *warmup >= 0 {
		cfg.Warmup = *warmup
	}
	if *maxTokens >= 0 {
		cfg.MaxTokens = *maxTokens
	}
	if *timeout >= 0 {
		cfg.TimeoutSec = *timeout
	}
	if *concurrency >= 0 {
		cfg.Concurrency = *concurrency
	}
	if *totalReq >= 0 {
		cfg.TotalRequests = *totalReq
	}
	cfg.ServerPID = *serverPID
	cfg.RunID = *runID
	if cfg.Iterations < 1 {
		cfg.Iterations = 1
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if len(cfg.Prompts) == 0 {
		cfg.Prompts = []Prompt{{
			ID: "default",
			Messages: []Message{
				{Role: "user", Content: "Say hello in five words."},
			},
		}}
	}
	return nil
}

// MeasuredTotal returns how many measured requests to schedule.
func (c *Config) MeasuredTotal() int {
	if c.TotalRequests > 0 {
		return c.TotalRequests
	}
	n := c.Iterations * len(c.Prompts)
	if n < 1 {
		n = 1
	}
	return n
}

// WarmupTotal returns how many warmup requests (not recorded).
// When concurrency > 1, warmup is at least one full wave of workers.
func (c *Config) WarmupTotal() int {
	if c.Warmup <= 0 {
		return 0
	}
	if c.Concurrency <= 1 {
		return c.Warmup * len(c.Prompts)
	}
	// Interpret warmup as waves of concurrent requests.
	waves := c.Warmup
	if waves < 1 {
		waves = 1
	}
	return waves * c.Concurrency
}
