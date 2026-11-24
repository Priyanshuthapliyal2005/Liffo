# Liffo

Liffo is a Go gateway for OpenAI-compatible inference workers. It bounds
admission, routes requests across workers, streams responses, and can manage
one local `llama-server` process. The reference backend is llama.cpp.

Run Liffo on loopback or a trusted network. Use an authenticated TLS proxy
and connection limits for external traffic. See the [deployment policy](SECURITY.md).

## Quickstart

Use Go 1.27.4, `llama-server`, and a GGUF model. In the first terminal, start a
CPU worker:

```sh
llama-server -m /path/to/model.gguf --host 127.0.0.1 --port 8080 -c 2048 -ngl 0
```

From the Liffo checkout in a second terminal:

```sh
go build -trimpath -o bin/liffo ./cmd/liffo
./bin/liffo gateway --config configs/gateway.yaml
```

In a third terminal, check readiness and run ten streaming requests:

```sh
curl -fsS http://127.0.0.1:8088/ready
run_dir="$(mktemp -d)"
./bin/liffo bench --config configs/bench.yaml --mode cpu --total-requests 10 --out "$run_dir"
./bin/liffo report --raw "$run_dir" --target "gateway smoke run"
```

Look for `n=10 errors=0` in the benchmark output. Stop both processes with
Ctrl-C when finished. See the [run guide](docs/REAL_INFERENCE.md) for managed
workers and GPU checks.

## Reference benchmark

The [CPU and NVIDIA L4 report](reports/reference-cpu-gpu-2026-11-06.md) measures
one Qwen2.5-0.5B-Instruct Q4_K_M model directly against llama.cpp. The
[benchmark guide](docs/BENCHMARKS.md) describes the workload and how to repeat
the setup.

## Documentation

- [HTTP request path, routing and lifecycle](docs/ARCHITECTURE.md)
- [Example worker topologies](examples/README.md)
- [Validation and supported scope](docs/VALIDATION.md)
- [Roadmap](docs/ROADMAP.md)
- [Contributing](CONTRIBUTING.md)

Liffo is licensed under [Apache 2.0](LICENSE). See the
[third-party notices](THIRD_PARTY_NOTICES.md).
