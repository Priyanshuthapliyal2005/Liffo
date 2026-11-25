.PHONY: build test race vet check vuln clean

build:
	go build -trimpath -o bin/liffo ./cmd/liffo

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: test vet race build

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...

clean:
	go clean

