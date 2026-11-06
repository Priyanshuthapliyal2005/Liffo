package adapt

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
)

// SecurityConfig hardens the mesh gateway surface (no cloud API keys).
type SecurityConfig struct {
	RateRPS       float64
	RateBurst     int
	ShutdownGrace time.Duration
}

func defaultSecurity() SecurityConfig {
	rps := 30.0
	if v := strings.TrimSpace(os.Getenv("LIFFO_RATE_RPS")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			rps = f
		}
	}
	burst := 60
	if v := strings.TrimSpace(os.Getenv("LIFFO_RATE_BURST")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			burst = n
		}
	}
	return SecurityConfig{
		RateRPS:       rps,
		RateBurst:     burst,
		ShutdownGrace: 15 * time.Second,
	}
}

// applySecurityMiddleware: rate limit + shutdown gate (mesh-local only).
func (h *HTTPServer) applySecurityMiddleware(mux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/ready" || r.URL.Path == "/metrics" {
			mux.ServeHTTP(w, r)
			return
		}
		if atomic.LoadInt32(&h.shuttingDown) == 1 {
			w.Header().Set("Connection", "close")
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "gateway shutting down", "error_code": "shutting_down",
			})
			return
		}
		if isLifecycleAdminPath(r.URL.Path) && !modellife.BearerAuthorized(r.Header.Get("Authorization"), h.adminToken) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "admin authentication required", "error_code": "unauthorized",
			})
			return
		}
		key := clientKey(r)
		if h.limiter != nil && !h.limiter.Allow(key) {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": "rate limit exceeded", "error_code": "rate_limited",
			})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func isLifecycleAdminPath(path string) bool {
	switch path {
	case "/v1/models/load", "/v1/models/evict", "/v1/models/events":
		return true
	default:
		return false
	}
}

func clientKey(r *http.Request) string {
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

// GracefulShutdown stops accepting traffic, waits for inflight, shuts HTTP, flushes obs.
func (h *HTTPServer) GracefulShutdown(ctx context.Context) error {
	atomic.StoreInt32(&h.shuttingDown, 1)
	deadline := time.Now().Add(h.sec.ShutdownGrace)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	for time.Now().Before(deadline) {
		if h.gw != nil && h.gw.Inflight() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := h.srv.Shutdown(shCtx)
	var lifeErr error
	if h.gw != nil {
		if coord := h.gw.Coordinator(); coord != nil {
			lifeErr = coord.Shutdown(ctx)
		} else if life := h.gw.Life(); life != nil {
			lifeErr = life.Shutdown(ctx)
		}
	}
	if h.obs != nil {
		c2, c := context.WithTimeout(context.Background(), 3*time.Second)
		_ = h.obs.Shutdown(c2)
		c()
	}
	return errors.Join(err, lifeErr)
}
