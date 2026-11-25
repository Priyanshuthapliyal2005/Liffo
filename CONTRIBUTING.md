# Contributing

Run the checks from the Liffo checkout:

```sh
gofmt -w cmd internal
make check
```

For behavior changes, add or update a regression test at the affected request
path. Keep examples consistent with the CLI and configuration defaults. For
performance changes, document the workload, endpoint, model and hardware.
See the [benchmark method](docs/benchmarks.md) and
[validation commands](docs/validation.md).

Keep credentials, model files, machine-specific paths and raw request traces
out of changes. See [SECURITY.md](SECURITY.md) for vulnerability reporting.
