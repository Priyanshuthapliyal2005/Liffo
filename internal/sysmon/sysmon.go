package sysmon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
)

// Sample is one point-in-time observation.
type Sample struct {
	TS         time.Time
	RAMMb      float64
	GPUMemMb   float64
	GPUUtilPct float64
	HasGPU     bool
}

// Monitor periodically samples RAM and GPU.
// Not safe to call Start concurrently; Start/Stop must be paired.
type Monitor struct {
	interval  time.Duration
	serverPID int

	mu      sync.Mutex
	samples []Sample
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	hasGPU  bool
}

// New creates a monitor. serverPID 0 means sample current process only for RAM.
func New(interval time.Duration, serverPID int) *Monitor {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Monitor{interval: interval, serverPID: serverPID, hasGPU: nvidiaAvailable()}
}

func nvidiaAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nvidia-smi", "-L")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// Start begins background sampling until Stop.
// Takes an immediate sample on the calling goroutine so short requests still get data.
// If already running, Stop is called first to avoid goroutine leaks.
func (m *Monitor) Start() {
	m.mu.Lock()
	already := m.running
	m.mu.Unlock()
	if already {
		m.Stop()
	}

	m.tick()
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancel = cancel
	m.running = true
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		t := time.NewTicker(m.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.tick()
			}
		}
	}()
}

// Stop ends sampling, takes a final sample, and waits for the loop.
// Safe to call when not started.
func (m *Monitor) Stop() {
	m.mu.Lock()
	cancel := m.cancel
	running := m.running
	m.cancel = nil
	m.running = false
	m.mu.Unlock()

	if running {
		m.tick()
	}
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

func (m *Monitor) tick() {
	s := Sample{TS: time.Now()}
	s.RAMMb = readRAMMb(m.serverPID)
	if m.hasGPU {
		mem, util, ok := readGPU()
		if ok {
			s.GPUMemMb = mem
			s.GPUUtilPct = util
			s.HasGPU = true
		}
	}
	m.mu.Lock()
	m.samples = append(m.samples, s)
	m.mu.Unlock()
}

// Snapshot returns range stats for samples collected so far.
func (m *Monitor) Snapshot() (ram, gpuMem, gpuUtil metrics.RangeStat, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rams, gmems, gutils []float64
	for _, s := range m.samples {
		if s.RAMMb > 0 {
			rams = append(rams, s.RAMMb)
		}
		if s.HasGPU {
			gmems = append(gmems, s.GPUMemMb)
			gutils = append(gutils, s.GPUUtilPct)
		}
	}
	return metrics.FromSamples(rams), metrics.FromSamples(gmems), metrics.FromSamples(gutils), len(m.samples)
}

// Reset clears samples (e.g. between requests). Does not stop the monitor.
func (m *Monitor) Reset() {
	m.mu.Lock()
	m.samples = nil
	m.mu.Unlock()
}

func readRAMMb(pid int) float64 {
	if pid <= 0 {
		if healthURL := strings.TrimSpace(os.Getenv("LIFFO_SYSMON_HEALTH_URL")); healthURL != "" {
			if rss := readRemoteHealthRSSMb(healthURL); rss > 0 {
				return rss
			}
		}
	}
	if runtime.GOOS == "windows" {
		return readWindowsWorkingSetMb(pid)
	}
	if runtime.GOOS == "linux" {
		rss := readLinuxRSSMb(pid)
		if rss > 0 {
			return rss
		}
		if pid > 0 && pid != getpid() {
			return 0
		}
	}
	if pid > 0 && pid != getpid() {
		return 0
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Alloc) / (1024 * 1024)
}

func readRemoteHealthRSSMb(healthURL string) float64 {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Get(healthURL)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0
	}
	var payload struct {
		RSSMb float64 `json:"rss_mb"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0
	}
	return payload.RSSMb
}

func readLinuxRSSMb(pid int) float64 {
	if pid <= 0 {
		pid = getpid()
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 2 {
			if pages, err := strconv.ParseFloat(fields[1], 64); err == nil && pages > 0 {
				pageSize := float64(os.Getpagesize())
				return (pages * pageSize) / (1024 * 1024)
			}
		}
	}
	// Fallback to /proc/[pid]/status VmRSS
	data, err = os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil && kb > 0 {
						return kb / 1024.0
					}
				}
			}
		}
	}
	return 0
}

func readWindowsWorkingSetMb(pid int) float64 {
	if pid <= 0 {
		pid = getpid()
	}
	// Prefer tasklist (faster than spawning PowerShell each sample).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH")
	out, err := cmd.Output()
	if err == nil {
		line := strings.TrimSpace(string(out))
		// INFO: No tasks...
		if line != "" && !strings.HasPrefix(strings.ToLower(line), "info:") {
			parts := parseCSVLine(line)
			if len(parts) >= 5 {
				memField := parts[len(parts)-1]
				memField = strings.TrimSpace(memField)
				// Locales: "12,345 K", "8,01,532 K", "12345 K"
				memField = strings.TrimSuffix(memField, " K")
				memField = strings.TrimSuffix(memField, " k")
				memField = strings.ReplaceAll(memField, ",", "")
				memField = strings.ReplaceAll(memField, " ", "")
				memField = strings.ReplaceAll(memField, "\u00a0", "")
				if v, err := strconv.ParseFloat(memField, 64); err == nil && v > 0 {
					return v / 1024.0 // KB -> MB
				}
			}
		}
	}
	// Fallback: PowerShell WorkingSet64
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	ps := fmt.Sprintf("(Get-Process -Id %d -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty WorkingSet64)", pid)
	cmd2 := exec.CommandContext(ctx2, "powershell", "-NoProfile", "-Command", ps)
	out2, err2 := cmd2.Output()
	if err2 != nil {
		return 0
	}
	s := strings.TrimSpace(string(out2))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v / (1024 * 1024)
}

func parseCSVLine(line string) []string {
	if line == "" {
		return nil
	}
	var fields []string
	var cur strings.Builder
	inQ := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '"':
			inQ = !inQ
		case c == ',' && !inQ:
			fields = append(fields, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	fields = append(fields, cur.String())
	return fields
}

func readGPU() (memMb, utilPct float64, ok bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=memory.used,utilization.gpu",
		"--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, false
	}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	if !sc.Scan() {
		return 0, 0, false
	}
	line := strings.TrimSpace(sc.Text())
	parts := strings.Split(line, ",")
	if len(parts) < 2 {
		return 0, 0, false
	}
	mem, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	util, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return mem, util, true
}
