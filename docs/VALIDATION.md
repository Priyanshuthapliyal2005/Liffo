# Validation

The source candidate targets local or trusted deployments. The self-contained
gateway smoke test uses a loopback HTTP worker to check streaming through the
configured gateway. The [reference benchmark](../reports/reference-cpu-gpu-2026-11-06.md)
records separate real-model runs.

## CI

The workflow runs unit tests, vet and the CLI build on Linux and Windows.
An Ubuntu job runs the race suite and vulnerability scanner. The
[November 7 baseline run](https://github.com/Priyanshuthapliyal2005/Liffo/actions/runs/42329660617)
passed on `bfbd782d11153280ee30715df47d80b72183133c`. For a later candidate,
use the workflow result for its own commit.

## Local checks

```sh
make check
go mod verify
GOOS=linux GOARCH=amd64 go build -o /tmp/liffo-linux-amd64 ./cmd/liffo
GOOS=windows GOARCH=amd64 go build -o /tmp/liffo-windows-amd64.exe ./cmd/liffo
make vuln
```

`make check` runs tests, vet, the race suite and the CLI build. A cross-build
checks compilation for its target. The hosted jobs execute tests on their
native runners.

Review dependencies and [license notices](../THIRD_PARTY_NOTICES.md) and scan
the candidate tree and history for secrets before publication. Record the
revision, tool versions and scan date. These checks cover source and build
evidence. See the [deployment policy](../SECURITY.md) and
[binary distribution requirements](ROADMAP.md) for the remaining scope.
