// benchplot - 从 Go benchmark 输出生成 SVG 折线图
//
// 用法:
//
//	go test -bench=. ./tests/ -benchmem | go run ./tests/tools/benchplot/main.go
//	go test -bench=. ./tests/ -benchmem > bench.txt && go run ./tests/tools/benchplot/main.go < bench.txt
//
// 输出:
//
//	bench_report.svg - 性能图表
//	bench_report.md  - Markdown 报告
package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type BenchResult struct {
	Name        string
	Iterations  int64
	NsPerOp     float64
	BytesPerOp  int64
	AllocsPerOp int64
}

func main() {
	results := parseBenchOutput(os.Stdin)
	if len(results) == 0 {
		fmt.Println("No benchmark results found")
		os.Exit(1)
	}

	// 生成 SVG 图表
	generateSVG(results, "bench_report.svg")

	// 生成 Markdown 报告
	generateMarkdown(results, "bench_report.md")

	fmt.Printf("Generated:\n")
	fmt.Printf("  - bench_report.svg (performance chart)\n")
	fmt.Printf("  - bench_report.md  (markdown report)\n")
}

func parseBenchOutput(f *os.File) []BenchResult {
	var results []BenchResult
	scanner := bufio.NewScanner(f)

	// 匹配 benchmark 输出行
	// BenchmarkXxx-N    123456    1234 ns/op    123 B/op    12 allocs/op
	re := regexp.MustCompile(`^(Benchmark\S+)-\d+\s+(\d+)\s+([\d.]+)\s+ns/op(?:\s+(\d+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?`)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if matches != nil {
			name := matches[1]
			iterations, _ := strconv.ParseInt(matches[2], 10, 64)
			nsPerOp, _ := strconv.ParseFloat(matches[3], 64)

			var bytesPerOp, allocsPerOp int64
			if matches[4] != "" {
				bytesPerOp, _ = strconv.ParseInt(matches[4], 10, 64)
			}
			if matches[5] != "" {
				allocsPerOp, _ = strconv.ParseInt(matches[5], 10, 64)
			}

			results = append(results, BenchResult{
				Name:        name,
				Iterations:  iterations,
				NsPerOp:     nsPerOp,
				BytesPerOp:  bytesPerOp,
				AllocsPerOp: allocsPerOp,
			})
		}
	}

	return results
}

func generateSVG(results []BenchResult, filename string) {
	// 图表尺寸
	width := 900
	height := 500
	marginLeft := 120
	marginRight := 50
	marginTop := 60
	marginBottom := 150

	chartWidth := width - marginLeft - marginRight
	chartHeight := height - marginTop - marginBottom

	// 找出最大值用于缩放
	var maxNs float64
	for _, r := range results {
		if r.NsPerOp > maxNs {
			maxNs = r.NsPerOp
		}
	}

	// 生成 SVG
	var svg strings.Builder
	svg.WriteString(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">
<style>
  .title { font: bold 18px sans-serif; fill: #333; }
  .subtitle { font: 12px sans-serif; fill: #666; }
  .axis-label { font: 11px sans-serif; fill: #666; }
  .bar-label { font: 10px sans-serif; fill: #333; }
  .value-label { font: 10px sans-serif; fill: #fff; }
  .grid { stroke: #eee; stroke-width: 1; }
  .bar { fill: #4CAF50; }
  .bar:hover { fill: #45a049; }
  .bar-parallel { fill: #2196F3; }
  .bar-parallel:hover { fill: #1976D2; }
</style>
<rect width="100%%" height="100%%" fill="#fafafa"/>
`, width, height, width, height))

	// 标题
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="30" class="title" text-anchor="middle">zmsg Benchmark Results</text>
`, width/2))
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="48" class="subtitle" text-anchor="middle">Generated: %s</text>
`, width/2, time.Now().Format("2006-01-02 15:04:05")))

	// 绘制网格线
	gridLines := 5
	for i := 0; i <= gridLines; i++ {
		y := marginTop + chartHeight - (chartHeight * i / gridLines)
		svg.WriteString(fmt.Sprintf(`<line x1="%d" y1="%d" x2="%d" y2="%d" class="grid"/>
`, marginLeft, y, marginLeft+chartWidth, y))

		// Y轴标签
		value := maxNs * float64(i) / float64(gridLines)
		label := formatNs(value)
		svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="axis-label" text-anchor="end">%s</text>
`, marginLeft-10, y+4, label))
	}

	// 绘制柱状图
	barWidth := chartWidth / (len(results) + 1)
	barPadding := barWidth / 4

	for i, r := range results {
		x := marginLeft + barWidth/2 + i*barWidth
		barH := int(float64(chartHeight) * r.NsPerOp / maxNs)
		y := marginTop + chartHeight - barH

		// 判断是否是 Parallel 测试
		barClass := "bar"
		if strings.Contains(r.Name, "Parallel") {
			barClass = "bar-parallel"
		}

		svg.WriteString(fmt.Sprintf(`<rect x="%d" y="%d" width="%d" height="%d" class="%s" rx="2"/>
`, x-barWidth/2+barPadding, y, barWidth-barPadding*2, barH, barClass))

		// 值标签
		if barH > 20 {
			svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="value-label" text-anchor="middle">%s</text>
`, x, y+15, formatNs(r.NsPerOp)))
		}

		// X轴标签（旋转显示）
		shortName := strings.TrimPrefix(r.Name, "Benchmark")
		svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="bar-label" text-anchor="start" transform="rotate(45 %d %d)">%s</text>
`, x, marginTop+chartHeight+10, x, marginTop+chartHeight+10, shortName))
	}

	// 图例
	legendY := height - 20
	svg.WriteString(fmt.Sprintf(`<rect x="%d" y="%d" width="15" height="15" class="bar"/>
`, marginLeft, legendY-12))
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="axis-label">Sequential</text>
`, marginLeft+20, legendY))
	svg.WriteString(fmt.Sprintf(`<rect x="%d" y="%d" width="15" height="15" class="bar-parallel"/>
`, marginLeft+120, legendY-12))
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="axis-label">Parallel</text>
`, marginLeft+140, legendY))

	svg.WriteString("</svg>")

	// 写入文件
	_ = os.WriteFile(filename, []byte(svg.String()), 0644)
}

func generateMarkdown(results []BenchResult, filename string) {
	var md strings.Builder

	md.WriteString("# zmsg Benchmark Report\n\n")
	md.WriteString(fmt.Sprintf("Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	// 按类别分组
	categories := map[string][]BenchResult{
		"SQL Builder":   {},
		"Cache":         {},
		"ID":            {},
		"PeriodicStore": {},
		"JSON":          {},
	}

	for _, r := range results {
		name := r.Name
		switch {
		case strings.Contains(name, "SQL") || strings.Contains(name, "Counter") ||
			strings.Contains(name, "Slice") || strings.Contains(name, "Map") ||
			strings.Contains(name, "Table"):
			categories["SQL Builder"] = append(categories["SQL Builder"], r)
		case strings.Contains(name, "Cache") || strings.Contains(name, "Get"):
			categories["Cache"] = append(categories["Cache"], r)
		case strings.Contains(name, "NextID"):
			categories["ID"] = append(categories["ID"], r)
		case strings.Contains(name, "Periodic"):
			categories["PeriodicStore"] = append(categories["PeriodicStore"], r)
		case strings.Contains(name, "JSON"):
			categories["JSON"] = append(categories["JSON"], r)
		}
	}

	// 输出各类别
	categoryOrder := []string{"SQL Builder", "Cache", "ID", "PeriodicStore", "JSON"}
	for _, cat := range categoryOrder {
		results := categories[cat]
		if len(results) == 0 {
			continue
		}

		md.WriteString(fmt.Sprintf("## %s\n\n", cat))
		md.WriteString("| Benchmark | Iterations | ns/op | B/op | allocs/op |\n")
		md.WriteString("|-----------|------------|-------|------|----------|\n")

		for _, r := range results {
			shortName := strings.TrimPrefix(r.Name, "Benchmark")
			md.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d |\n",
				shortName,
				formatNumber(r.Iterations),
				formatNs(r.NsPerOp),
				r.BytesPerOp,
				r.AllocsPerOp,
			))
		}
		md.WriteString("\n")
	}

	// 性能总结
	md.WriteString("## Summary\n\n")

	// 找出最快和最慢
	sorted := make([]BenchResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NsPerOp < sorted[j].NsPerOp
	})

	md.WriteString("### Fastest Operations\n\n")
	for i := 0; i < 5 && i < len(sorted); i++ {
		r := sorted[i]
		md.WriteString(fmt.Sprintf("- **%s**: %s\n",
			strings.TrimPrefix(r.Name, "Benchmark"),
			formatNs(r.NsPerOp),
		))
	}

	md.WriteString("\n### Slowest Operations\n\n")
	for i := len(sorted) - 1; i >= len(sorted)-5 && i >= 0; i-- {
		r := sorted[i]
		md.WriteString(fmt.Sprintf("- **%s**: %s\n",
			strings.TrimPrefix(r.Name, "Benchmark"),
			formatNs(r.NsPerOp),
		))
	}

	// 写入文件
	_ = os.WriteFile(filename, []byte(md.String()), 0644)
}

func formatNs(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fµs", ns/1e3)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}

func formatNumber(n int64) string {
	if n >= 1e9 {
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	}
	if n >= 1e6 {
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	}
	if n >= 1e3 {
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	}
	return fmt.Sprintf("%d", n)
}
