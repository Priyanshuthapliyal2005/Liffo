# Third-party dependency inventory

This source release uses the following direct Go modules. Transitive versions
are locked in `go.sum`; binary distributors should regenerate a complete
license bundle for the exact release build.

| Module | Version | License |
|---|---:|---|
| github.com/prometheus/client_golang | v1.24.1 | Apache-2.0 |
| github.com/prometheus/common | v0.70.1 | Apache-2.0 |
| go.opentelemetry.io/otel | v1.45.0 | Apache-2.0 |
| go.opentelemetry.io/otel/exporters/stdout/stdouttrace | v1.45.0 | Apache-2.0 |
| go.opentelemetry.io/otel/sdk | v1.45.0 | Apache-2.0 |
| go.opentelemetry.io/otel/trace | v1.45.0 | Apache-2.0 |
| golang.org/x/sys | v0.47.0 | BSD-3-Clause |
| gopkg.in/yaml.v3 | v3.0.1 | MIT and Apache-2.0 dual license |

An automated `go-licenses report ./...` audit also classified the selected
transitive packages as Apache-2.0, BSD-3-Clause, or MIT. The tool warned only
that assembly/C source files cannot be dependency-inspected and that this
unreleased local module has no version tag; both are expected at this stage.
