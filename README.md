<div align="center">

<img src="docs/images/liffo-mark.svg" width="112" alt="Liffo mesh logo">

# Liffo

**Reliability, routing, observability, and benchmarks for local LLM inference.**

![Status](https://img.shields.io/badge/status-release%20candidate-f59e0b)
![Go](https://img.shields.io/badge/Go-1.27.3-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-Apache--2.0-2563eb)

[Getting started](#getting-started) · [Benchmarks](#reference-benchmarks) · [Documentation](#documentation) · [Contributing](CONTRIBUTING.md)

</div>

Liffo is a local-first control plane for OpenAI-compatible inference workers.
It adds bounded admission, load-aware routing, worker health tracking, model
lifecycle coordination, telemetry, and reproducible benchmarking without
replacing the model runtime.

## Supported backends and models

- OpenAI-compatible `/v1/chat/completions` workers
- `llama-server` is the validated reference backend
- Model-agnostic routing; GGUF models are managed through allowlisted logical IDs
- Validated with Qwen2.5-0.5B-Instruct Q4_K_M on CPU and NVIDIA L4

## Supported features

- Streaming and non-streaming chat-completion gateway
- Bounded in-flight admission, bounded waiters, timeout, and overload shedding
- Round-robin, least-inflight, prefix-affinity, hybrid, capacity-aware,
  estimated-workload, and multi-SLO routing
- Atomic worker selection and in-flight accounting
- One upstream completion attempt per HTTP request, with worker health updates for subsequent requests
- Load → ready → drain → evict process lifecycle
- Prometheus metrics, OpenTelemetry spans, health, readiness, and worker status
- TTFT, latency, throughput, RAM, VRAM, and GPU-utilization benchmarking
- Privacy-safe results: prompt IDs and hashes, not prompt or completion bodies

## Where Liffo fits

Liffo operates above the inference runtime. The boundary is intentionally
simple:

| Layer | Responsibility |
|---|---|
| Client or deployment edge | Application behavior, TLS, and user authentication |
| Liffo control plane | Admission, worker selection, recovery, lifecycle, telemetry, and benchmarks |
| Inference worker | Model loading, device execution, token generation, and the OpenAI-compatible worker API |

`llama-server` is the reference worker, but Liffo's routing and observability
layers depend on the API contract rather than a specific model architecture.

## Supported environments

- macOS arm64: build, vet, race suite, and streaming integration test passed
- Linux amd64: cross-build passed; CI workflow included
- Windows amd64: cross-build passed; native managed-process support included
- Optional NVIDIA sampling through `nvidia-smi`

Required for a complete deployment but not included in this release:

- An inference worker and model weights
- TLS and ordinary-request authentication for non-loopback exposure
- Model downloads or hosted inference
- A web interface
- A claim that measurements from different hardware are directly comparable

## Getting started

### Prerequisites

- Go 1.27.3
- A recent `llama-server`
- A GGUF model

### Build

```sh
git clone https://github.com/Priyanshuthapliyal2005/Liffo.git
cd Liffo
make check
```

Or build the CLI directly:

```sh
go build -trimpath -o bin/liffo ./cmd/liffo
```

### Run

Start a CPU worker:

```sh
llama-server -m /path/model.gguf --host 127.0.0.1 --port 8080 -c 2048 -ngl 0
```

Start Liffo in a second terminal:

```sh
./bin/liffo gateway --config configs/gateway.yaml
```

In a third terminal, check readiness and run a streaming benchmark through the gateway:

```sh
curl -fsS http://127.0.0.1:8088/ready
run_dir="$(mktemp -d)"
./bin/liffo bench --config configs/bench.yaml --mode cpu --total-requests 10 --out "$run_dir"
./bin/liffo report --raw "$run_dir" --target "gateway smoke run"
```

Look for `n=10 errors=0` in the benchmark output.

For managed llama.cpp lifecycle, copy
`configs/gateway-managed.example.yaml`, replace its paths, and set a long random
`LIFFO_ADMIN_TOKEN`.

## Reference benchmarks

CPU and GPU matrices ran sequentially on the same Linux x86_64 allocation with
an AMD EPYC 7R13, NVIDIA L4, llama.cpp b12177, and Qwen2.5-0.5B-Instruct
Q4_K_M. Each concurrency cell contains 32 measured requests; all 256 requests
completed successfully with zero SSE parse errors.

| Median per-request decode throughput ↑ | Median time to first token ↓ |
|---|---|
| ![Grouped CPU and NVIDIA L4 median decode throughput bars at concurrency 1, 2, 4, and 8](docs/images/decode-throughput.svg) | ![Grouped CPU and NVIDIA L4 median TTFT bars at concurrency 1, 2, 4, and 8](docs/images/ttft-by-concurrency.svg) |

The GPU run records full `25/25` layer offload. These results describe one
small model and one hardware allocation, not universal performance. See the
[complete values, metric definitions, and reproduction guidance](docs/BENCHMARKS.md).

## Validation

The release gate includes build, unit and HTTP integration tests, `go vet`, the
race detector, Linux/Windows cross-builds, dependency verification,
`govulncheck`, license classification, and secret scanning. The current
candidate passes every local gate.

See [validation evidence](docs/VALIDATION.md) and the [project roadmap](docs/ROADMAP.md).

## Documentation

- [Architecture and request invariants](docs/ARCHITECTURE.md)
- [Reference benchmark methodology](docs/BENCHMARKS.md)
- [Curated CPU/GPU benchmark report](reports/reference-cpu-gpu-2026-11-06.md)
- [Real CPU/GPU inference guide](docs/REAL_INFERENCE.md)
- [Runnable configurations](examples/README.md)
- [Project roadmap](docs/ROADMAP.md)
- [Security policy](SECURITY.md)
- [Third-party dependency inventory](THIRD_PARTY_NOTICES.md)
- [Contributing](CONTRIBUTING.md)

## License

Liffo is released under the Apache License 2.0. See [LICENSE](LICENSE).

## Acknowledgments

Liffo builds on the OpenAI-compatible server interface provided by
`llama.cpp`, with operational telemetry from Prometheus and OpenTelemetry.
