#!/bin/bash
# zmsg Benchmark Runner
# 运行基准测试并生成图表报告

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPORT_DIR="${SCRIPT_DIR}/reports"

# 创建报告目录
mkdir -p "$REPORT_DIR"

# 时间戳
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BENCH_FILE="${REPORT_DIR}/bench_${TIMESTAMP}.txt"

echo "=== zmsg Benchmark Suite ==="
echo "Project: $PROJECT_DIR"
echo "Output:  $REPORT_DIR"
echo ""

# 检查依赖
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed"
    exit 1
fi

# 运行基准测试
echo "Running benchmarks..."
cd "$PROJECT_DIR"

# 完整的 benchmark 测试
go test -bench=. -benchmem -benchtime=2s ./tests/ 2>&1 | tee "$BENCH_FILE"

echo ""
echo "Benchmark results saved to: $BENCH_FILE"

# 生成图表报告
echo ""
echo "Generating reports..."
cd "$SCRIPT_DIR"

if [ -f "$BENCH_FILE" ]; then
    go run ./tools/benchplot/main.go < "$BENCH_FILE"
    
    # 移动生成的文件到报告目录
    if [ -f "bench_report.svg" ]; then
        mv bench_report.svg "${REPORT_DIR}/bench_${TIMESTAMP}.svg"
        echo "SVG chart: ${REPORT_DIR}/bench_${TIMESTAMP}.svg"
    fi
    
    if [ -f "bench_report.md" ]; then
        mv bench_report.md "${REPORT_DIR}/bench_${TIMESTAMP}.md"
        echo "Markdown:  ${REPORT_DIR}/bench_${TIMESTAMP}.md"
    fi
fi

# 创建 latest 链接
cd "$REPORT_DIR"
ln -sf "bench_${TIMESTAMP}.txt" latest.txt
ln -sf "bench_${TIMESTAMP}.svg" latest.svg
ln -sf "bench_${TIMESTAMP}.md" latest.md

echo ""
echo "=== Done ==="
echo "Latest reports:"
echo "  ${REPORT_DIR}/latest.txt"
echo "  ${REPORT_DIR}/latest.svg"
echo "  ${REPORT_DIR}/latest.md"
