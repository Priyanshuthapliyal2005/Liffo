# Project roadmap

> Liffo already provides a runnable inference gateway and benchmark foundation.
> The roadmap focuses on hardening distribution and expanding validation.

[Documentation](README.md) · [Validation](VALIDATION.md) · [Contributing](../CONTRIBUTING.md)

---

## Available today

| Area | Status |
|---|:---:|
| OpenAI-compatible streaming gateway | ✅ |
| Bounded admission and overload shedding | ✅ |
| Load-, affinity-, capacity-, workload-, and multi-SLO routing | ✅ |
| Safe pre-output retry and stream recovery | ✅ |
| Local llama.cpp lifecycle management | ✅ |
| Prometheus metrics and OpenTelemetry spans | ✅ |
| Privacy-safe CPU/GPU benchmarking and reports | ✅ |
| Linux and Windows CI configuration | ✅ |

## Near-term priorities

- stabilize configuration for remote lifecycle agents and coordinators;
- define a versioned public Go API where external integrations need one;
- add broader model and accelerator validation;
- complete an external security review for non-loopback deployments;
- automate SBOMs, checksums, provenance, and signed binary releases; and
- publish compatibility and vulnerability-response policies.

## Release milestones

```mermaid
flowchart LR
    A[Source release candidate] --> B[External security review]
    B --> C[Reproducible release pipeline]
    C --> D[Signed multi-platform binaries]
    D --> E[Stable public API policy]
```

## Before the first supported binary release

- [ ] Document the supported deployment trust boundary
- [ ] Run hosted release gates on Windows and Linux
- [ ] Generate and publish an SBOM and complete license bundle
- [ ] Attach checksums, provenance, and signatures to artifacts
- [ ] Establish a private vulnerability-reporting channel
- [ ] Define version compatibility and support expectations

The source can be built and evaluated today. These items govern a formally
supported binary distribution and its long-term maintenance.
