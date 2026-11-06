# Architecture

## Request path

```text
OpenAI client
  -> HTTP security and rate limit
  -> bounded admission queue
  -> lifecycle readiness lease
  -> route and reserve worker capacity
  -> stream upstream SSE
  -> safe retry only before visible output
  -> metrics, span, and privacy-safe result
```

`internal/adapt` owns the gateway, admission boundary, adaptive controller and
HTTP streaming path. Both direct and HTTP requests enter the same admission
and lifecycle sequence, preventing reservation leaks and bypasses.

`internal/route` implements round-robin, least-inflight, prefix-affinity,
hybrid, capacity-normalized, estimated-workload, and multi-SLO routing. Worker
capacity is reserved atomically with selection. Retry decisions use explicit
stream stages and stop after visible output.

`internal/modellife` implements the loading, ready, draining, evicting, failed,
and stopped state machine. The local loader starts a `llama-server` process
from an allowlisted logical model ID. Model files must resolve beneath the
configured model root. The coordinator and authenticated agent loader remain
available for multi-worker deployments, but the release CLI initially exposes
only the simpler single-local-worker lifecycle configuration.

`internal/obs` supplies Prometheus metrics and OpenTelemetry spans.
`internal/client`, `internal/sse`, `internal/sysmon`, `internal/metrics`, and
`internal/report` implement real streaming benchmarks and reporting without
persisting prompt or completion bodies.

## Deliberate boundaries

Liffo admission is request scheduling, not llama.cpp continuous batching.
The gateway does not download models, provide TLS, authenticate ordinary
completion requests, or claim hardware comparability. Those remain explicit
deployment responsibilities.
