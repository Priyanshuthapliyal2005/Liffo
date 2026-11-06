package client

import (
	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/sysmon"
)

// PhaseMonitor samples RAM/GPU for an entire measured phase (not per request).
type PhaseMonitor struct {
	m *sysmon.Monitor
}

// NewPhaseMonitor creates a phase-scoped resource monitor.
func NewPhaseMonitor(cfg config.Config) *PhaseMonitor {
	return &PhaseMonitor{m: sysmon.New(cfg.SampleInterval(), cfg.ServerPID)}
}

func (p *PhaseMonitor) Start() {
	if p != nil && p.m != nil {
		p.m.Start()
	}
}

func (p *PhaseMonitor) Stop() {
	if p != nil && p.m != nil {
		p.m.Stop()
	}
}

func (p *PhaseMonitor) Snapshot() (ram, gpuMem, gpuUtil metrics.RangeStat, n int) {
	if p == nil || p.m == nil {
		return metrics.RangeStat{}, metrics.RangeStat{}, metrics.RangeStat{}, 0
	}
	return p.m.Snapshot()
}
