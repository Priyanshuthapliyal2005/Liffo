package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/adapt"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/client"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/report"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
	"gopkg.in/yaml.v3"
)

const version = "0.1.0-rc1"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "gateway":
		err = runGateway(os.Args[2:])
	case "bench":
		err = runBench(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	case "status":
		err = runStatus(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Println(version)
		return
	case "help", "--help", "-h":
		usage()
		return
	default:
		usage()
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Liffo local inference gateway and benchmark tool

Usage:
  liffo gateway --config configs/gateway.yaml
  liffo bench [flags]
  liffo report [flags]
  liffo status [--gateway http://127.0.0.1:8088]
  liffo version`)
}

type gatewayFile struct {
	Listen          string        `yaml:"listen"`
	Mode            string        `yaml:"mode"`
	RouteMethod     route.Method  `yaml:"route_method"`
	MaxInflight     int           `yaml:"max_inflight"`
	MaxAdmitWaiters int           `yaml:"max_admit_waiters"`
	AdmitTimeoutMS  int           `yaml:"admit_timeout_ms"`
	RetryEnabled    *bool         `yaml:"retry_enabled"`
	Model           string        `yaml:"model"`
	TimeoutSec      int           `yaml:"timeout_sec"`
	TraceJSONL      string        `yaml:"trace_jsonl"`
	Workers         []workerFile  `yaml:"workers"`
	Lifecycle       lifecycleFile `yaml:"lifecycle"`
	MultiSLO        multiSLOFile  `yaml:"multi_slo"`
}

type workerFile struct {
	URL            string  `yaml:"url"`
	Label          string  `yaml:"label"`
	CostPerReq     float64 `yaml:"cost_per_req"`
	LatencyPriorMS float64 `yaml:"latency_prior_ms"`
	QualityTier    float64 `yaml:"quality_tier"`
	Capacity       float64 `yaml:"capacity"`
}

type multiSLOFile struct {
	WeightCost      float64 `yaml:"weight_cost"`
	WeightLatency   float64 `yaml:"weight_latency"`
	WeightLoad      float64 `yaml:"weight_load"`
	WeightSLO       float64 `yaml:"weight_slo"`
	WeightQuality   float64 `yaml:"weight_quality"`
	MinQuality      float64 `yaml:"min_quality"`
	MaxCostPerReq   float64 `yaml:"max_cost_per_req"`
	SLOTargetMS     float64 `yaml:"slo_target_ms"`
	LatencyEMAAlpha float64 `yaml:"latency_ema_alpha"`
}

type lifecycleFile struct {
	Enabled         bool              `yaml:"enabled"`
	Bin             string            `yaml:"bin"`
	WorkDir         string            `yaml:"work_dir"`
	ModelRoot       string            `yaml:"model_root"`
	Host            string            `yaml:"host"`
	Port            int               `yaml:"port"`
	NGL             *int              `yaml:"ngl"`
	CtxSize         int               `yaml:"ctx_size"`
	ReadyTimeoutSec int               `yaml:"ready_timeout_sec"`
	LoadTimeoutSec  int               `yaml:"load_timeout_sec"`
	EvictTimeoutSec int               `yaml:"evict_timeout_sec"`
	DrainTimeoutSec int               `yaml:"drain_timeout_sec"`
	LogPath         string            `yaml:"log_path"`
	BootstrapModel  string            `yaml:"bootstrap_model"`
	Models          map[string]string `yaml:"models"`
}

func runGateway(args []string) error {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	path := fs.String("config", "configs/gateway.yaml", "gateway YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var f gatewayFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	sc, cleanup, err := buildGatewayConfig(f)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	hs, err := adapt.NewHTTPServer(sc)
	if err != nil {
		return err
	}
	fmt.Printf("liffo gateway listen=%s workers=%d mode=%s route=%s lifecycle=%t\n",
		hs.Addr(), len(sc.Gateway.Workers), sc.Gateway.Mode, sc.Gateway.Initial.RouteMethod, sc.Gateway.Life != nil)

	errCh := make(chan error, 1)
	go func() { errCh <- hs.ListenAndServe() }()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return hs.GracefulShutdown(ctx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func buildGatewayConfig(f gatewayFile) (adapt.ServeConfig, func(), error) {
	if len(f.Workers) == 0 {
		return adapt.ServeConfig{}, nil, errors.New("gateway config requires at least one worker")
	}
	if f.Listen == "" {
		f.Listen = "127.0.0.1:8088"
	}
	if f.Mode == "" {
		f.Mode = string(adapt.ModeStatic)
	}
	if f.RouteMethod == "" {
		f.RouteMethod = route.MethodLeastInflight
	}
	if f.MaxInflight < 1 {
		f.MaxInflight = 4
	}
	if f.TimeoutSec < 1 {
		f.TimeoutSec = 120
	}
	if f.Model == "" {
		f.Model = "local"
	}
	retry := true
	if f.RetryEnabled != nil {
		retry = *f.RetryEnabled
	}
	mode := adapt.ModeStatic
	if strings.EqualFold(f.Mode, string(adapt.ModeAdaptive)) {
		mode = adapt.ModeAdaptive
	}
	urls := make([]string, 0, len(f.Workers))
	labels := make([]string, 0, len(f.Workers))
	static := make([]route.WorkerStatic, 0, len(f.Workers))
	for i, w := range f.Workers {
		if strings.TrimSpace(w.URL) == "" {
			return adapt.ServeConfig{}, nil, fmt.Errorf("worker %d has no URL", i)
		}
		urls = append(urls, strings.TrimRight(w.URL, "/"))
		label := w.Label
		if label == "" {
			label = fmt.Sprintf("worker-%d", i)
		}
		labels = append(labels, label)
		static = append(static, route.WorkerStatic{CostPerReq: w.CostPerReq, LatencyPriorMs: w.LatencyPriorMS, QualityTier: w.QualityTier, Capacity: w.Capacity})
	}
	multi := route.MultiSLOConfig{
		WeightCost: f.MultiSLO.WeightCost, WeightLatency: f.MultiSLO.WeightLatency,
		WeightLoad: f.MultiSLO.WeightLoad, WeightSLO: f.MultiSLO.WeightSLO,
		WeightQuality: f.MultiSLO.WeightQuality, MinQuality: f.MultiSLO.MinQuality,
		MaxCostPerReq: f.MultiSLO.MaxCostPerReq, SLOTargetMs: f.MultiSLO.SLOTargetMS,
		LatencyEMAAlpha: f.MultiSLO.LatencyEMAAlpha,
	}
	gc := adapt.GatewayConfig{
		Mode: mode, Workers: urls, Initial: adapt.StaticPolicy(f.RouteMethod, f.MaxInflight, retry),
		DefaultTimeout:  time.Duration(f.TimeoutSec) * time.Second,
		AdmitTimeout:    time.Duration(f.AdmitTimeoutMS) * time.Millisecond,
		MaxAdmitWaiters: f.MaxAdmitWaiters, MaxTokens: 64, Model: f.Model,
		MultiSLO: multi, WorkerStatic: static,
	}
	var cleanup func()
	if f.Lifecycle.Enabled {
		if len(f.Workers) != 1 {
			return adapt.ServeConfig{}, nil, errors.New("local lifecycle currently requires exactly one worker")
		}
		ngl := -1
		if f.Lifecycle.NGL != nil {
			ngl = *f.Lifecycle.NGL
		}
		loader := &modellife.LlamaServerLoader{
			Bin: f.Lifecycle.Bin, WorkDir: f.Lifecycle.WorkDir, ModelRoot: f.Lifecycle.ModelRoot,
			Host: f.Lifecycle.Host, Port: f.Lifecycle.Port, Ngl: ngl, CtxSize: f.Lifecycle.CtxSize,
			Models: f.Lifecycle.Models, ReadyTimeout: seconds(f.Lifecycle.ReadyTimeoutSec), LogPath: f.Lifecycle.LogPath,
		}
		if strings.TrimRight(f.Workers[0].URL, "/") != loader.BaseURL() {
			return adapt.ServeConfig{}, nil, fmt.Errorf("managed worker URL %q must match lifecycle endpoint %q", f.Workers[0].URL, loader.BaseURL())
		}
		life, err := modellife.New(modellife.Config{
			LoadTimeout: seconds(f.Lifecycle.LoadTimeoutSec), EvictTimeout: seconds(f.Lifecycle.EvictTimeoutSec),
			DrainTimeout: seconds(f.Lifecycle.DrainTimeoutSec),
		}, loader)
		if err != nil {
			return adapt.ServeConfig{}, nil, err
		}
		gc.Life = life
		gc.BootstrapModel = f.Lifecycle.BootstrapModel
		cleanup = life.Close
	}
	return adapt.ServeConfig{
		Listen: f.Listen, Gateway: gc, Labels: labels, TraceJSONL: f.TraceJSONL,
		AdminToken: strings.TrimSpace(os.Getenv("LIFFO_ADMIN_TOKEN")),
	}, cleanup, nil
}

func seconds(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

type workItem struct {
	prompt config.Prompt
	index  int
	warmup bool
}

func runBench(args []string) error {
	var cfg config.Config
	if err := config.ApplyRunFlags(&cfg, args); err != nil {
		return err
	}
	if cfg.RunID == "" {
		cfg.RunID = time.Now().UTC().Format("20060102T150405Z")
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	runner := client.NewRunner(cfg)
	defer runner.Close()
	if n := cfg.WarmupTotal(); n > 0 {
		_ = runPool(runner, cfg, makeJobs(cfg, n, true))
	}
	start := time.Now()
	results := runPool(runner, cfg, makeJobs(cfg, cfg.MeasuredTotal(), false))
	agg := metrics.Summarize(results)
	tokens := 0
	for _, r := range results {
		if metrics.IsSuccess(r) {
			tokens += r.TokensOut
		}
	}
	metrics.AttachWallClock(&agg, time.Since(start), tokens)
	path := filepath.Join(cfg.OutDir, fmt.Sprintf("bench-%s-c%d-%s.jsonl", cfg.Mode, cfg.Concurrency, cfg.RunID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, r := range results {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Printf("raw=%s n=%d errors=%d wall=%.2fs rps=%.2f ttft_p50=%.1fms latency_p50=%.1fms tokens_per_sec_p50=%.1f\n",
		path, agg.Count, agg.Errors, agg.WallSec, agg.ThroughputRPS, agg.TTFTMs.P50, agg.LatencyMs.P50, agg.TokensPerSec.P50)
	return nil
}

func makeJobs(cfg config.Config, n int, warmup bool) []workItem {
	jobs := make([]workItem, 0, n)
	for i := 0; i < n; i++ {
		jobs = append(jobs, workItem{prompt: cfg.Prompts[i%len(cfg.Prompts)], index: i + 1, warmup: warmup})
	}
	return jobs
}

func runPool(runner *client.Runner, cfg config.Config, jobs []workItem) []metrics.Result {
	if len(jobs) == 0 {
		return nil
	}
	n := cfg.Concurrency
	if n > len(jobs) {
		n = len(jobs)
	}
	in := make(chan workItem)
	out := make(chan metrics.Result, len(jobs))
	var wg sync.WaitGroup
	for worker := 1; worker <= n; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range in {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout())
				out <- runner.RunOneWorker(ctx, job.prompt, job.index, job.warmup, id)
				cancel()
			}
		}(worker)
	}
	go func() {
		for _, job := range jobs {
			in <- job
		}
		close(in)
		wg.Wait()
		close(out)
	}()
	results := make([]metrics.Result, 0, len(jobs))
	for result := range out {
		results = append(results, result)
	}
	return results
}

func runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	raw := fs.String("raw", "results/raw", "JSONL file or directory")
	out := fs.String("out", "reports", "output directory")
	runID := fs.String("run-id", time.Now().UTC().Format("20060102T150405Z"), "report identifier")
	target := fs.String("target", "unspecified", "hardware description")
	stack := fs.String("stack", "llama.cpp OpenAI-compatible API", "inference stack")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rows, err := report.LoadJSONL(*raw)
	if err != nil {
		return err
	}
	doc := report.Build(*raw, rows, map[string]string{"target": *target, "stack": *stack})
	md := filepath.Join(*out, "report-"+*runID+".md")
	js := filepath.Join(*out, "report-"+*runID+".json")
	if err := report.WriteMarkdown(md, doc); err != nil {
		return err
	}
	if err := report.WriteJSON(js, doc); err != nil {
		return err
	}
	fmt.Printf("markdown=%s json=%s rows=%d errors=%d\n", md, js, doc.Aggregate.Count, doc.Aggregate.Errors)
	return nil
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	base := fs.String("gateway", "http://127.0.0.1:8088", "gateway base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c := &http.Client{Timeout: 10 * time.Second}
	for _, path := range []string{"/health", "/ready", "/v1/workers"} {
		resp, err := c.Get(strings.TrimRight(*base, "/") + path)
		if err != nil {
			return err
		}
		var value any
		err = json.NewDecoder(resp.Body).Decode(&value)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		pretty, _ := json.MarshalIndent(value, "", "  ")
		fmt.Printf("%s %d\n%s\n", path, resp.StatusCode, pretty)
	}
	return nil
}
