# Example configurations

Build Liffo using the [quickstart](../README.md#quickstart). Start compatible
workers on ports 8080 and 8081, then choose one topology:

- [Two workers](two-workers.yaml) selects by in-flight count.
- [Multi-SLO](multi-slo.yaml) uses configured cost, latency, load, SLO fit and
  quality. Calibrate the example metadata against your own workers.

```sh
./bin/liffo gateway --config examples/two-workers.yaml
```

In another terminal, check `http://127.0.0.1:8088/ready` before running the
quickstart workload.
