# Runnable configurations

These examples assume local OpenAI-compatible workers and contain no model
paths, credentials, or provider-specific settings.

## Two-worker least-inflight gateway

Start workers on ports 8080 and 8081, then run:

```sh
./bin/liffo gateway --config examples/two-workers.yaml
./bin/liffo status --gateway http://127.0.0.1:8088
```

## Multi-SLO routing

`multi-slo.yaml` demonstrates static cost, latency, quality, and capacity
metadata. The values are examples, not measured claims. Replace them with
values derived from your own reproducible calibration.

```sh
./bin/liffo gateway --config examples/multi-slo.yaml
```

Use `configs/bench.yaml` with either gateway and keep generated JSONL under
`results/raw/`, which is ignored by Git.
