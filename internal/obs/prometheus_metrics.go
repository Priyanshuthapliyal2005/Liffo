package obs

import (
	"bytes"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func gatherPrometheusText(reg *prometheus.Registry) (string, error) {
	mfs, err := reg.Gather()
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	// prometheus/common v0.55+: NewFormat + FmtText
	format := expfmt.NewFormat(expfmt.TypeTextPlain)
	enc := expfmt.NewEncoder(&buf, format)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			// fallback older constant
			_ = err
			enc2 := expfmt.NewEncoder(&buf, expfmt.FmtText)
			for _, mf2 := range mfs {
				if e2 := enc2.Encode(mf2); e2 != nil {
					return "", e2
				}
			}
			return buf.String(), nil
		}
	}
	if c, ok := enc.(expfmt.Encoder); ok {
		_ = c
	}
	if closer, ok := enc.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	return buf.String(), nil
}

// SnapshotKeyValues extracts high-signal series for JSON evidence.
func (t *Telemetry) SnapshotKeyValues() map[string]float64 {
	out := map[string]float64{}
	if t == nil {
		return out
	}
	mfs, err := t.Registry.Gather()
	if err != nil {
		return out
	}
	for _, mf := range mfs {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			key := name
			if len(m.GetLabel()) > 0 {
				key += "{"
				for i, l := range m.GetLabel() {
					if i > 0 {
						key += ","
					}
					key += fmt.Sprintf("%s=%s", l.GetName(), l.GetValue())
				}
				key += "}"
			}
			switch {
			case m.Counter != nil:
				out[key] = m.GetCounter().GetValue()
			case m.Gauge != nil:
				out[key] = m.GetGauge().GetValue()
			case m.Histogram != nil:
				out[key+"_count"] = float64(m.GetHistogram().GetSampleCount())
				out[key+"_sum"] = m.GetHistogram().GetSampleSum()
			}
		}
	}
	return out
}
