# Liffo documentation

| Guide | Purpose |
|---|---|
| [Architecture](ARCHITECTURE.md) | Request path, package boundaries, and reliability invariants |
| [Benchmarks](BENCHMARKS.md) | Reference results, hardware manifest, metrics, and reproduction |
| [Real inference](REAL_INFERENCE.md) | CPU/GPU and managed llama.cpp walkthrough |
| [Validation](VALIDATION.md) | Release-candidate build, race, security, and integration evidence |
| [Release audit](RELEASE_AUDIT.md) | Retained, removed, and deferred functionality |
| [Porting](PORTING.md) | Moving reviewed changes across private and public histories |

Start with the root [README](../README.md) for installation and the shortest
runnable path. The documentation intentionally does not contain private raw
traces, prompt bodies, credentials, or machine-specific configuration.
