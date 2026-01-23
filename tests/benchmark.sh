#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Running benchmarks (requires Redis/Postgres running)..."
cd "$ROOT_DIR"
go test -bench=. -benchmem ./tests/...
