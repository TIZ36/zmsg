.PHONY: build test lint clean integration-test docker-up docker-down

# Build
build:
    go build -o bin/zmsg ./cmd/example/

# Run tests
test:
    go test -v ./tests/... -coverprofile=coverage.out

# Integration tests
integration-test:
    docker-compose up -d
    sleep 5
    go test -v ./tests/integration/...
    docker-compose down

# Lint
lint:
    golangci-lint run

# Clean
clean:
    rm -rf bin/
    rm -rf coverage.out

# Run example
run:
    go run cmd/example/main.go

# Docker compose
docker-up:
    docker-compose up -d

docker-down:
    docker-compose down

# Benchmark
benchmark:
    go test -bench=. -benchmem ./tests/performance/...

# Code coverage
coverage:
    go test -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out

# Generate docs
docs:
    godoc -http=:6060