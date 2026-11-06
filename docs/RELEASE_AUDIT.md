# Release-candidate audit

## Retained

- OpenAI-compatible streaming gateway and shared request path
- bounded admission, queue shedding, capacity reservation and keep-alives
- static/adaptive policies and validated routing methods
- safe pre-output retry and stream recovery semantics
- lifecycle state machine, local llama.cpp process loader, agent loader and
  multi-worker coordinator library
- Prometheus metrics, OpenTelemetry spans, system sampling and privacy-safe
  JSONL/report generation
- regression tests for admission bounds, lease ordering, poison recovery,
  capacity initialization, idle tie-breaking, streaming and lifecycle security

## Removed

- raw and generated benchmark results, reports and hardware-specific evidence
- research notebooks, papers, matrices, stress/soak/chaos harnesses and
  one-off experiment commands
- private cloud, environment and provider integrations
- model files, llama.cpp binaries, build artifacts and machine-specific configs
- agent instructions, transcripts, private repository metadata and history
- the monolithic research CLI and duplicate experiment orchestration

## Deferred

- a stable public Go API (packages intentionally remain `internal`)
- CLI configuration for remote lifecycle agents and multi-worker coordinators
- package signing, release automation, SBOM/provenance and published images
- externally reviewed security model for non-loopback deployments
- final module/repository namespace and public support policy

## Dependency and license posture

The direct runtime dependencies are Prometheus client libraries,
OpenTelemetry, `golang.org/x/sys`, and YAML v3. The project is Apache-2.0.
The candidate includes its direct dependency inventory. Before publishing
binary releases, generate a complete license bundle for the exact build and
retain vulnerability scan evidence from the release environment.
