# Liffo

Liffo is a Go gateway for running local inference across OpenAI-compatible
workers. A slow or draining worker can tie up requests that another worker
could serve. Liffo bounds active and waiting requests, routes new work using
worker load and health, and streams the response back to the client.
`llama-server` is the reference backend.

Use Liffo on loopback or a trusted network. External traffic needs TLS, client
authentication, and connection limits at the edge. See the
[deployment policy](SECURITY.md).

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

## Measurements

The quickstart measures requests through Liffo. The
[CPU and NVIDIA L4 reference report](reports/reference-cpu-gpu-2026-11-06.md)
compares direct `llama-server` runs, so its latency and throughput figures
exclude Liffo. It shows how concurrency changed time to first token,
per-request decode rate, and total output for the same model and workload.

## Documentation

- [HTTP request path, routing and lifecycle](docs/ARCHITECTURE.md)
- [Example worker topologies](examples/README.md)
- [Validation and supported scope](docs/VALIDATION.md)
- [Roadmap](docs/ROADMAP.md)
- [Contributing](CONTRIBUTING.md)

Liffo is licensed under [Apache 2.0](LICENSE). See the
[third-party notices](THIRD_PARTY_NOTICES.md).
