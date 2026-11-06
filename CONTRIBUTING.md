# Contributing

Keep production changes small and pair bug fixes with regression tests. Run:

```sh
gofmt -w cmd internal
go test ./...
go vet ./...
go test -race ./...
```

Do not add prompt/completion bodies, raw benchmark traces, model files,
credentials, environment-specific paths, or generated reports. Real inference
claims must state the actual hardware and retain server-log proof for GPU
offload.
