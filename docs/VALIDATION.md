# Release-candidate validation

Validation was run on 2026-11-07 using macOS 27.0.1 (Darwin 27.0.0), Apple M5
arm64, 16 GiB RAM, and the pinned Go 1.27.3 toolchain.

## Passed gates

| Gate | Result |
|---|---|
| `go test ./...` | pass, all packages |
| `go vet ./...` | pass |
| `go test -race ./...` | pass, all packages |
| CLI streaming smoke test | pass: config -> gateway -> mock OpenAI SSE worker |
| native arm64 build | pass |
| Linux amd64 cross-build | pass |
| Windows amd64 cross-build | pass |
| `govulncheck ./...` | no vulnerabilities found |
| `go-licenses report ./...` | dependency licenses classified; expected source-file inspection warnings only |
| Gitleaks filesystem scan | no leaks found |
| sensitive-data/path scan | no credentials, private keys, private paths, research IDs, raw traces, models or environment files found |

The first vulnerability scan found reachable advisories in Go 1.27.1 and
OpenTelemetry 1.44.0. The candidate was upgraded to the fixed Go 1.27.3
toolchain and OpenTelemetry 1.45.0, after which the scan returned no
vulnerabilities.

The smoke test is intentionally self-contained and makes no use of files from
another repository. It verifies a real HTTP streaming path through the public
CLI configuration and gateway to an in-process OpenAI-compatible worker.

## Not claimed

No model or inference binary is distributed in this candidate, so this
extraction run does not claim new real-model performance numbers or GPU
offload. Follow `docs/REAL_INFERENCE.md` on the intended release hardware and
retain the actual hardware manifest and llama.cpp offload log before making
performance claims.
