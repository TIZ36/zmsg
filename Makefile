.PHONY: build test lint clean integration-test docker-up docker-down bench report analyse

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
	rm -rf tests/reports/

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

# Run benchmark tests and save results
bench:
	@mkdir -p tests/reports
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	BENCH_FILE="tests/reports/bench_$$TIMESTAMP.txt"; \
	echo "=== Running benchmarks ==="; \
	go test -bench=. -benchmem -benchtime=2s ./tests/ 2>&1 | tee $$BENCH_FILE; \
	echo ""; \
	echo "Benchmark results saved to: $$BENCH_FILE"; \
	ln -sf "bench_$$TIMESTAMP.txt" tests/reports/latest.txt

# Generate benchmark report and charts
report:
	@if [ ! -f tests/reports/latest.txt ]; then \
		echo "Error: No benchmark data found. Please run 'make bench' first."; \
		exit 1; \
	fi
	@echo "=== Generating benchmark report ==="
	@TIMESTAMP=$$(basename tests/reports/latest.txt .txt | sed 's/bench_//'); \
	go run tools/benchplot/main.go < tests/reports/latest.txt; \
	if [ -f bench_report.svg ]; then \
		mv bench_report.svg tests/reports/bench_$$TIMESTAMP.svg; \
		rm -f tests/reports/latest.svg; \
		ln -s "bench_$$TIMESTAMP.svg" tests/reports/latest.svg; \
		echo "SVG chart: tests/reports/bench_$$TIMESTAMP.svg"; \
	fi; \
	if [ -f bench_report.md ]; then \
		mv bench_report.md tests/reports/bench_$$TIMESTAMP.md; \
		rm -f tests/reports/latest.md; \
		ln -s "bench_$$TIMESTAMP.md" tests/reports/latest.md; \
		echo "Markdown report: tests/reports/bench_$$TIMESTAMP.md"; \
	fi
	@echo ""
	@echo "=== Report generated ==="
	@echo "Latest reports:"
	@echo "  tests/reports/latest.txt"
	@echo "  tests/reports/latest.svg"
	@echo "  tests/reports/latest.md"

# Code coverage
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Generate docs
docs:
	godoc -http=:6060

# Generate industry analysis report
analyse:
	@if [ ! -f tests/reports/latest.txt ]; then \
		echo "Error: No benchmark data found. Please run 'make bench' first."; \
		exit 1; \
	fi
	@echo "=== Generating industry analysis report ==="
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	go run tools/analyse/main.go tests/reports/latest.txt > tests/reports/analysis_$$TIMESTAMP.md; \
	ln -sf "analysis_$$TIMESTAMP.md" tests/reports/latest_analysis.md; \
	echo "Analysis report: tests/reports/analysis_$$TIMESTAMP.md"; \
	echo "Latest report: tests/reports/latest_analysis.md"