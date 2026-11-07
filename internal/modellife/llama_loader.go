package modellife

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var ErrUnknownModel = errors.New("modellife: unknown model id")

// LlamaServerLoader starts/stops a real llama-server.exe process per model path.
// Model names resolve exclusively through Models. Request-provided paths are never
// accepted.
type LlamaServerLoader struct {
	Bin     string // path to llama-server.exe
	WorkDir string // cwd for process (often bin dir for CUDA DLLs)
	// ModelRoot is the only directory from which GGUF files may be loaded.
	ModelRoot string
	Host      string // default 127.0.0.1
	Port      int    // default 8080
	Ngl       int    // GPU layers; -1 = omit (CPU)
	CtxSize   int    // default 2048
	// Models maps logical id → GGUF path. It must not be empty.
	Models map[string]string
	// ReadyTimeout waits for /health after start. Default 90s.
	ReadyTimeout time.Duration
	// ExtraArgs appended to llama-server.
	ExtraArgs []string
	// LogPath optional combined stdout/stderr file.
	LogPath string

	mu      sync.Mutex
	cmd     *exec.Cmd
	current string // last loaded model id/path
	logFile *os.File
	managed managedProcess
}

// BaseURL returns the worker base URL this loader owns.
func (l *LlamaServerLoader) BaseURL() string {
	host := l.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := l.Port
	if port <= 0 {
		port = 8080
	}
	return fmt.Sprintf("http://%s:%d", host, port)
}

func (l *LlamaServerLoader) resolvePath(model string) (string, error) {
	if model == "" {
		return "", ErrInvalidModel
	}
	path, ok := l.Models[model]
	if !ok || path == "" {
		return "", fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	return ValidateModelPath(l.ModelRoot, path)
}

// ValidateModelPath ensures an allowlisted GGUF remains under modelRoot even
// after cleaning and symlink evaluation. It is shared by local and agent
// process loaders.
func ValidateModelPath(modelRoot, configuredPath string) (string, error) {
	if stringsLikePath(configuredPath) && modelRoot == "" {
		return "", fmt.Errorf("modellife: model_root is required")
	}
	if runtime.GOOS == "windows" {
		vol := filepath.VolumeName(configuredPath)
		if len(configuredPath) >= 2 && configuredPath[0] == '\\' && configuredPath[1] == '\\' {
			return "", fmt.Errorf("modellife: UNC/device model paths are forbidden")
		}
		if vol != "" && !filepath.IsAbs(configuredPath) {
			return "", fmt.Errorf("modellife: drive-relative model paths are forbidden")
		}
	}
	rootAbs, err := filepath.Abs(modelRoot)
	if err != nil {
		return "", fmt.Errorf("modellife: model_root: %w", err)
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", fmt.Errorf("modellife: model_root: %w", err)
	}
	path := configuredPath
	if !filepath.IsAbs(path) {
		path = filepath.Join(rootReal, path)
	}
	path = filepath.Clean(path)
	pathReal, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("modellife: model file: %w", err)
	}
	rel, err := filepath.Rel(rootReal, pathReal)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("modellife: model path escapes model_root")
	}
	st, err := os.Stat(pathReal)
	if err != nil {
		return "", fmt.Errorf("modellife: model file not found: %w", err)
	}
	if !st.Mode().IsRegular() || !strings.EqualFold(filepath.Ext(pathReal), ".gguf") {
		return "", fmt.Errorf("modellife: model must be a regular .gguf file")
	}
	return pathReal, nil
}

func stringsLikePath(s string) bool {
	return strings.ContainsAny(s, `/\:`)
}

// Load stops any prior process, starts llama-server with the GGUF, waits for /health.
func (l *LlamaServerLoader) Load(ctx context.Context, model string) error {
	path, err := l.resolvePath(model)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stopLocked(); err != nil {
		return fmt.Errorf("modellife: terminate previous llama-server: %w", err)
	}

	if l.Bin == "" {
		return fmt.Errorf("modellife: llama-server bin not set")
	}
	host := l.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := l.Port
	if port <= 0 {
		port = 8080
	}
	ctxSize := l.CtxSize
	if ctxSize <= 0 {
		ctxSize = 2048
	}
	args := []string{
		"-m", path,
		"--host", host,
		"--port", fmt.Sprintf("%d", port),
		"-c", fmt.Sprintf("%d", ctxSize),
	}
	if l.Ngl >= 0 {
		args = append(args, "-ngl", fmt.Sprintf("%d", l.Ngl))
	}
	args = append(args, l.ExtraArgs...)

	cmd := exec.CommandContext(context.Background(), l.Bin, args...)
	prepareManagedCommand(cmd)
	if runtime.GOOS != "windows" {
		binDir := filepath.Dir(l.Bin)
		cmd.Env = append(os.Environ(), "LD_LIBRARY_PATH="+binDir+":"+os.Getenv("LD_LIBRARY_PATH"))
	}
	if l.WorkDir != "" {
		cmd.Dir = l.WorkDir
	}
	if l.LogPath != "" {
		_ = os.MkdirAll(filepath.Dir(l.LogPath), 0o755)
		f, err := os.Create(l.LogPath)
		if err == nil {
			l.logFile = f
			cmd.Stdout = f
			cmd.Stderr = f
		}
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("modellife: start llama-server: %w", err)
	}
	managed, err := attachManagedProcess(cmd)
	if err != nil {
		_ = terminateManagedProcess(cmd, 8*time.Second)
		return fmt.Errorf("modellife: contain llama-server process: %w", err)
	}
	l.cmd = cmd
	l.managed = managed
	l.current = model

	readyTO := l.ReadyTimeout
	if readyTO <= 0 {
		readyTO = 90 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, readyTO)
	defer cancel()
	url := fmt.Sprintf("http://%s:%d/health", host, port)
	if err := waitHealthy(waitCtx, url); err != nil {
		_ = l.stopLocked()
		return fmt.Errorf("modellife: llama-server not ready: %w", err)
	}
	return nil
}

// Unload kills the llama-server process.
func (l *LlamaServerLoader) Unload(ctx context.Context, model string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopLocked()
}

// Current returns the last model id passed to Load (logical name).
func (l *LlamaServerLoader) Current() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

// PID returns the process id if running.
func (l *LlamaServerLoader) PID() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cmd == nil || l.cmd.Process == nil {
		return 0
	}
	return l.cmd.Process.Pid
}

func (l *LlamaServerLoader) stopLocked() error {
	if l.cmd == nil || l.cmd.Process == nil {
		l.cmd = nil
		l.current = ""
		_ = releaseManagedProcess(&l.managed)
		if l.logFile != nil {
			_ = l.logFile.Close()
			l.logFile = nil
		}
		return nil
	}
	err := terminateManagedProcess(l.cmd, 8*time.Second)
	releaseErr := releaseManagedProcess(&l.managed)
	l.cmd = nil
	l.current = ""
	if l.logFile != nil {
		_ = l.logFile.Close()
		l.logFile = nil
	}
	// brief settle for port release on Windows
	time.Sleep(300 * time.Millisecond)
	return errors.Join(err, releaseErr)
}

func waitHealthy(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}
	}
}
