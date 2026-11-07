package adapt

import (
	"sync"
	"time"
)

// rateLimiter is a per-key token-bucket (mesh gateway only; no cloud provider keys).
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens map[string]float64
	last   map[string]time.Time
}

func newRateLimiter(rateRPS float64, burst int) *rateLimiter {
	if rateRPS <= 0 {
		rateRPS = 20
	}
	if burst < 1 {
		burst = 40
	}
	return &rateLimiter{
		rate: rateRPS, burst: float64(burst),
		tokens: map[string]float64{}, last: map[string]time.Time{},
	}
}

func (l *rateLimiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	if key == "" {
		key = "anon"
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.tokens[key]
	if !ok {
		t = l.burst
	}
	last, ok := l.last[key]
	if !ok {
		last = now
	}
	elapsed := now.Sub(last).Seconds()
	t += elapsed * l.rate
	if t > l.burst {
		t = l.burst
	}
	if t < 1 {
		l.tokens[key] = t
		l.last[key] = now
		return false
	}
	t -= 1
	l.tokens[key] = t
	l.last[key] = now
	return true
}
