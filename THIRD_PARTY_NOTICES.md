# Third-party dependencies

> A compact, auditable runtime dependency set built around Prometheus,
> OpenTelemetry, Go system APIs, and YAML configuration.

[Project overview](README.md) · [Validation evidence](docs/VALIDATION.md) · [Apache-2.0 license](LICENSE)

---

## Direct runtime inventory

| Module | Version | License | Purpose |
|---|---:|---|---|
| `github.com/prometheus/client_golang` | v1.24.1 | Apache-2.0 | Metrics exposition |
| `github.com/prometheus/common` | v0.70.1 | Apache-2.0 | Prometheus model and helpers |
| `go.opentelemetry.io/otel` | v1.45.0 | Apache-2.0 | Tracing API |
| `go.opentelemetry.io/otel/exporters/stdout/stdouttrace` | v1.45.0 | Apache-2.0 | Local trace export |
| `go.opentelemetry.io/otel/sdk` | v1.45.0 | Apache-2.0 | Tracing SDK |
| `go.opentelemetry.io/otel/trace` | v1.45.0 | Apache-2.0 | Trace interfaces |
| `golang.org/x/sys` | v0.47.0 | BSD-3-Clause | Platform system calls |
| `gopkg.in/yaml.v3` | v3.0.1 | MIT / Apache-2.0 | Configuration parsing |

Exact transitive versions are locked in [`go.sum`](go.sum). An automated
`go-licenses report ./...` audit classified selected transitive packages as
Apache-2.0, BSD-3-Clause, or MIT.

## Distribution requirements

Source review and binary distribution have different evidence needs. Before
publishing a binary release:

- regenerate the dependency and license inventory for the exact tag;
- produce an SBOM and include all required license texts;
- run vulnerability analysis against the final dependency graph;
- attach checksums, build provenance, and signatures; and
- review separately distributed model and inference-runtime licenses.

The license tool reports expected warnings for assembly/C source inspection
and for an unreleased local module without a version tag. These warnings do not
change the direct inventory above, but should be reevaluated for a tagged
binary release.
