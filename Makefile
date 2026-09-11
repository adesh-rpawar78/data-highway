.PHONY: run build test bench cover vet fmt docker

run:
	go run ./cmd/server

build:
	go build -o bin/datahighway ./cmd/server

test:
	go test ./... -v -race

bench:
	go test ./internal/pipeline/... -bench=. -benchmem -run=^$$

cover:
	go test ./... -race -coverprofile=coverage.out
	go tool cover -func=coverage.out

vet:
	go vet ./...

fmt:
	gofmt -l -w .

docker:
	docker build -t datahighway:latest .
