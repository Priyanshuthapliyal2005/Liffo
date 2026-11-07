package modellife

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FakeLoader is a test double with controllable delay, fail, and hang behavior.
type FakeLoader struct {
	mu sync.Mutex

	LoadDelay   time.Duration
	UnloadDelay time.Duration
	// FailModels maps model name → error message for Load
	FailModels map[string]string
	// HangModels: Load blocks until context cancel/timeout
	HangModels map[string]bool
	// FailUnload if true Unload returns error
	FailUnload bool

	Loads   atomic.Int64
	Unloads atomic.Int64
	// LastLoaded is the last successful load model
	LastLoaded atomic.Value // string
	// Current resident model name (empty if unloaded)
	resident string
}

func NewFakeLoader() *FakeLoader {
	return &FakeLoader{
		FailModels: map[string]string{},
		HangModels: map[string]bool{},
	}
}

func (f *FakeLoader) Load(ctx context.Context, model string) error {
	f.Loads.Add(1)
	f.mu.Lock()
	delay := f.LoadDelay
	hang := f.HangModels[model]
	failMsg := f.FailModels[model]
	f.mu.Unlock()

	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if failMsg != "" {
		return fmt.Errorf("%s", failMsg)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.resident = model
	f.mu.Unlock()
	f.LastLoaded.Store(model)
	return nil
}

func (f *FakeLoader) Unload(ctx context.Context, model string) error {
	f.Unloads.Add(1)
	f.mu.Lock()
	delay := f.UnloadDelay
	fail := f.FailUnload
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return fmt.Errorf("unload failed for %s", model)
	}
	f.mu.Lock()
	if f.resident == model {
		f.resident = ""
	}
	f.mu.Unlock()
	return nil
}

func (f *FakeLoader) Resident() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resident
}
