package modellife

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// AgentLoader controls a private worker agent using logical model IDs only.
type AgentLoader struct {
	BaseURL string
	Token   string
	Client  *http.Client

	mu      sync.Mutex
	current string
}

func (l *AgentLoader) Load(ctx context.Context, model string) error {
	if model == "" {
		return ErrInvalidModel
	}
	var out struct {
		OK    bool   `json:"ok"`
		Model string `json:"model"`
	}
	if err := l.call(ctx, http.MethodPost, "/v1/admin/models/load", map[string]string{"model": model}, &out); err != nil {
		return err
	}
	if !out.OK || out.Model != model {
		return fmt.Errorf("modellife: agent did not confirm model %q", model)
	}
	l.mu.Lock()
	l.current = model
	l.mu.Unlock()
	return nil
}

func (l *AgentLoader) Unload(ctx context.Context, _ string) error {
	var out struct {
		OK bool `json:"ok"`
	}
	if err := l.call(ctx, http.MethodPost, "/v1/admin/models/evict", nil, &out); err != nil {
		return err
	}
	if !out.OK {
		return fmt.Errorf("modellife: agent did not confirm eviction")
	}
	l.mu.Lock()
	l.current = ""
	l.mu.Unlock()
	return nil
}

func (l *AgentLoader) Current() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

func (l *AgentLoader) call(ctx context.Context, method, path string, body any, out any) error {
	if strings.TrimSpace(l.Token) == "" {
		return fmt.Errorf("modellife: agent token is required")
	}
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(l.BaseURL, "/")+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+l.Token)
	req.Header.Set("Content-Type", "application/json")
	client := l.Client
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("modellife: agent request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("modellife: agent http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
		return fmt.Errorf("modellife: agent response: %w", err)
	}
	return nil
}

// BearerAuthorized validates an Authorization header without timing-sensitive
// string comparison. Empty expected tokens never authorize.
func BearerAuthorized(header, expected string) bool {
	if expected == "" || !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
