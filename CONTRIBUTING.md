# Contributing to Liffo

> Small changes, explicit invariants, regression evidence, and no sensitive data.

[Project overview](README.md) · [Architecture](docs/ARCHITECTURE.md) · [Validation](docs/VALIDATION.md) · [Security](SECURITY.md)

---

## A good Liffo change

| Principle | What it means in practice |
|---|---|
| Keep the path understandable | Prefer extending an existing package over adding another abstraction |
| Prove behavior | Pair bug fixes with a regression test that exercises the failure |
| Protect streaming semantics | Never retry after user-visible output and always propagate cancellation |
| Measure honestly | Label hardware, model, runtime, concurrency, and offload configuration |
| Preserve privacy | Store prompt IDs and hashes—not prompt or completion bodies |

## Development loop

```sh
gofmt -w cmd internal
go test ./...
go vet ./...
go test -race ./...
```

For changes to the gateway, also run the streaming integration path. For
changes that affect real inference, rerun both CPU and GPU measurements and
retain the GPU offload log line.

## Pull-request checklist

- [ ] The change has one clear purpose
- [ ] New behavior is covered by tests
- [ ] Admission, lease, reservation, retry, and cleanup invariants still hold
- [ ] `go test`, `go vet`, and race-enabled tests pass
- [ ] Documentation and example configuration match the implementation
- [ ] Generated files, binaries, models, and local paths are excluded
- [ ] Performance language identifies the actual test environment
- [ ] The diff contains no credentials, prompt bodies, completion bodies, or raw traces

## Benchmark contributions

A useful benchmark submission includes:

1. CPU, RAM, OS, and accelerator model;
2. driver, CUDA/Metal backend, and llama.cpp revision;
3. model name, hash, quantization, context, and token limits;
4. worker slots, thread count, concurrency, and request count;
5. error and SSE-parse counts; and
6. an offload log line for GPU results.

Results from different environments belong in separate comparison groups.
See the [reference benchmark methodology](docs/BENCHMARKS.md).

## Keep out of the repository

Do not add model files, llama.cpp binaries, `.env` files, bearer tokens,
machine-specific configuration, generated reports, raw benchmark traces, or
prompt/completion bodies. Report security issues using the process in
[`SECURITY.md`](SECURITY.md), not in a public issue.
