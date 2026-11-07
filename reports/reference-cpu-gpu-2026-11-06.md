# Liffo CPU/GPU reference benchmark

- **Date:** 2026-11-06
- **Requests:** 128 CPU + 128 GPU
- **Errors:** 0
- **SSE parse errors:** 0
- **Workload:** identical prompts and token limits across modes and concurrency cells

## Environment

| Component | Value |
|---|---|
| OS | Linux x86_64 |
| CPU | AMD EPYC 7R13, 8 logical CPUs |
| Memory | 30 GiB RAM |
| GPU | NVIDIA L4, 23,034 MiB; driver 580.178.04 |
| Runtime | llama.cpp b12177 (`6c01da627`) |
| CUDA | 12.8 |
| Model | Qwen2.5-0.5B-Instruct Q4_K_M |
| Context | 2048 tokens |
| CPU configuration | 8 threads, 8 server slots, `-ngl 0` |
| GPU configuration | 8 server slots, full 25/25-layer offload |

CPU and GPU matrices ran sequentially on the same allocation. Each concurrency
cell contains 32 measured requests.

## Overall

| Mode | N | Errors | TTFT p50 | Latency p50 | Decode p50 | RAM max | VRAM max | GPU util max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| CPU | 128 | 0 | 48.8 ms | 1,619.0 ms | 29.7 tok/s | 694 MiB | 0 MiB | 0% |
| GPU | 128 | 0 | 9.0 ms | 180.7 ms | 264.0 tok/s | 526 MiB | 734 MiB | 88% |

## By concurrency

| Mode | Concurrency | TTFT p50 | Latency p50 | Per-request decode p50 | Wall output rate |
|---|---:|---:|---:|---:|---:|
| CPU | 1 | 21.2 ms | 1,031.8 ms | 55.1 tok/s | 49.4 tok/s |
| CPU | 2 | 36.3 ms | 1,652.7 ms | 33.7 tok/s | 58.9 tok/s |
| CPU | 4 | 117.3 ms | 3,505.9 ms | 16.5 tok/s | 59.7 tok/s |
| CPU | 8 | 293.5 ms | 9,130.8 ms | 5.4 tok/s | 40.4 tok/s |
| GPU | 1 | 5.5 ms | 179.6 ms | 360.3 tok/s | 220.5 tok/s |
| GPU | 2 | 6.7 ms | 201.5 ms | 317.5 tok/s | 396.2 tok/s |
| GPU | 4 | 11.1 ms | 299.3 ms | 207.5 tok/s | 565.2 tok/s |
| GPU | 8 | 16.9 ms | 455.5 ms | 124.8 tok/s | 673.3 tok/s |

## GPU placement evidence

The inference-server log recorded:

```text
offloaded 25/25 layers to GPU
CUDA0 model buffer size = 373.73 MiB
CUDA0 KV buffer size = 96.00 MiB
```

## Interpretation

The GPU path reduces median TTFT and raises per-request and aggregate output
throughput in every tested concurrency cell. CPU aggregate output peaks around
concurrency 2–4, while per-request decode rate and TTFT degrade as concurrency
increases. The L4 continues gaining aggregate output through concurrency 8,
with higher per-request contention than at lower concurrency.

These values describe one small model and one hardware allocation. They are not
universal performance claims and must not be compared directly with results
from different hardware, model hashes, quantizations, context sizes, or
llama.cpp revisions.

## Metric definitions

- **TTFT:** time to the first non-empty streamed content delta.
- **Per-request decode:** output tokens divided by the request's decode window
  after TTFT.
- **Wall output rate:** all successful output tokens divided by matrix elapsed
  time.
- **RAM/VRAM/GPU utilization:** sampled maxima during measured requests.

See [`docs/BENCHMARKS.md`](../docs/BENCHMARKS.md) for reproduction guidance.
