## Summary

Describe the problem and the smallest change that solves it.

## Validation

- [ ] `go test ./...`
- [ ] `go vet ./...`
- [ ] `go test -race ./...`
- [ ] Relevant real CPU/GPU validation was rerun, or the reason it is not applicable is documented

## Safety and release boundary

- [ ] No credentials, private paths, model files, prompt/completion bodies, or raw private traces
- [ ] New behavior has regression coverage
- [ ] Public configuration and documentation are updated
- [ ] Hardware and benchmark conditions are stated for performance claims
- [ ] Managed inference processes are cleaned up after validation
