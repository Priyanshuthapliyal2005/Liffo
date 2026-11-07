# Benchmark reports

> Public reports are curated aggregates. Local request traces remain local.

[Project overview](../README.md) · [Benchmark methodology](../docs/BENCHMARKS.md) · [Run your own](../docs/REAL_INFERENCE.md)

---

## Included reference

| Report | Format | Use it for |
|---|---|---|
| [CPU/GPU matrix — 2026-11-06](reference-cpu-gpu-2026-11-06.md) | Markdown | Human-readable environment, results, evidence, and interpretation |
| [CPU/GPU aggregate — 2026-11-06](reference-cpu-gpu-2026-11-06.json) | JSON | Tooling, chart generation, and independent analysis |

### At a glance

| | CPU | NVIDIA L4 GPU |
|---|---:|---:|
| Requests | 128 | 128 |
| Errors | 0 | 0 |
| Median TTFT | 48.8 ms | 9.0 ms |
| Median decode | 29.7 tok/s | 264.0 tok/s |

The matching charts, metric definitions, comparison rules, and reproduction
commands live in the [benchmark guide](../docs/BENCHMARKS.md).

## Publication boundary

Liffo writes new reports here, but generated output is ignored by default so
machine details and experimental data are not committed accidentally. A public
reference report may contain aggregate metrics and an intentionally reviewed
hardware manifest.

Never commit raw request traces, prompt or completion bodies, credentials,
local paths, model weights, or machine identifiers.
