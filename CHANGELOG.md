# Changelog

All notable changes to Liffo are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/).

## Unreleased

### Planned

- Hosted Windows and Linux release-gate execution
- Stable multi-worker lifecycle configuration in the CLI
- Signed release artifacts, SBOM, and provenance

## 0.1.0-rc1 - 2026-11-07

### Added

- OpenAI-compatible streaming gateway
- Bounded admission and overload shedding
- Load-, affinity-, capacity-, estimated-work-, and multi-SLO routing
- HTTP worker-health recovery across requests and stage-aware retry in the internal executor
- Managed llama.cpp lifecycle with drain-before-evict semantics
- Prometheus and OpenTelemetry observability
- Privacy-safe benchmark client and reports
- CPU/GPU reference benchmarks and reproducibility guidance
- Linux and Windows CI configuration

### Security

- Allowlisted logical model IDs confined beneath `model_root`
- Authenticated lifecycle mutation endpoints
- Go 1.27.3 and OpenTelemetry 1.45.0 after vulnerability remediation
