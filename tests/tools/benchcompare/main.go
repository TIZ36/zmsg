// benchcompare - 比较多个 benchmark 报告并生成趋势图
//
// 用法:
//
//	go run ./tests/tools/benchcompare/main.go ./tests/reports/bench_*.txt
//
// 输出:
//
//	bench_trend.svg - 性能趋势图（类似 git 提交历史图）
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type BenchResult struct {
	NsPerOp     float64
	BytesPerOp  int64
	AllocsPerOp int64
}

type BenchReport struct {
	Filename  string
	Timestamp time.Time
	Results   map[string]BenchResult
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: benchcompare <bench_file1.txt> [bench_file2.txt] ...")
		fmt.Println("       benchcompare ./tests/reports/bench_*.txt")
		os.Exit(1)
	}

	// 解析所有报告文件
	var reports []BenchReport
	for _, pattern := range os.Args[1:] {
		files, err := filepath.Glob(pattern)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		for _, file := range files {
			report := parseReport(file)
			if report != nil && len(report.Results) > 0 {
				reports = append(reports, *report)
			}
		}
	}

	if len(reports) == 0 {
		fmt.Println("No benchmark reports found")
		os.Exit(1)
	}

	// 按时间排序
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].Timestamp.Before(reports[j].Timestamp)
	})

	// 生成趋势图
	generateTrendSVG(reports, "bench_trend.svg")
	generateCompareMarkdown(reports, "bench_compare.md")

	fmt.Printf("Generated:\n")
	fmt.Printf("  - bench_trend.svg   (trend chart)\n")
	fmt.Printf("  - bench_compare.md  (comparison report)\n")
}

func parseReport(filename string) *BenchReport {
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	defer file.Close()

	// 从文件名提取时间戳
	base := filepath.Base(filename)
	timestamp := extractTimestamp(base)

	results := make(map[string]BenchResult)
	scanner := bufio.NewScanner(file)

	re := regexp.MustCompile(`^(Benchmark\S+)-\d+\s+\d+\s+([\d.]+)\s+ns/op(?:\s+(\d+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?`)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if matches != nil {
			name := matches[1]
			nsPerOp, _ := strconv.ParseFloat(matches[2], 64)

			var bytesPerOp, allocsPerOp int64
			if matches[3] != "" {
				bytesPerOp, _ = strconv.ParseInt(matches[3], 10, 64)
			}
			if matches[4] != "" {
				allocsPerOp, _ = strconv.ParseInt(matches[4], 10, 64)
			}

			results[name] = BenchResult{
				NsPerOp:     nsPerOp,
				BytesPerOp:  bytesPerOp,
				AllocsPerOp: allocsPerOp,
			}
		}
	}

	return &BenchReport{
		Filename:  filename,
		Timestamp: timestamp,
		Results:   results,
	}
}

func extractTimestamp(filename string) time.Time {
	// 尝试从文件名提取时间戳 bench_20240123_153045.txt
	re := regexp.MustCompile(`(\d{8})_(\d{6})`)
	matches := re.FindStringSubmatch(filename)
	if matches != nil {
		t, err := time.Parse("20060102_150405", matches[1]+"_"+matches[2])
		if err == nil {
			return t
		}
	}
	// 使用文件修改时间
	info, err := os.Stat(filename)
	if err == nil {
		return info.ModTime()
	}
	return time.Now()
}

func generateTrendSVG(reports []BenchReport, filename string) {
	// 收集所有 benchmark 名称
	benchNames := make(map[string]bool)
	for _, r := range reports {
		for name := range r.Results {
			benchNames[name] = true
		}
	}

	// 选择要显示的 benchmark（最多 8 个）
	var names []string
	priorityNames := []string{
		"BenchmarkCacheOnly",
		"BenchmarkCacheOnly_Parallel",
		"BenchmarkGet",
		"BenchmarkGet_Parallel",
		"BenchmarkNextID",
		"BenchmarkCacheAndPeriodicStore",
		"BenchmarkSQL_Builder/SQL_Basic",
		"BenchmarkJSON_Marshal",
	}

	for _, name := range priorityNames {
		if benchNames[name] {
			names = append(names, name)
		}
	}

	if len(names) == 0 {
		for name := range benchNames {
			names = append(names, name)
			if len(names) >= 8 {
				break
			}
		}
	}

	// 图表尺寸
	width := 1000
	height := 600
	marginLeft := 100
	marginRight := 200
	marginTop := 80
	marginBottom := 80

	chartWidth := width - marginLeft - marginRight
	chartHeight := height - marginTop - marginBottom

	// 找出最大值
	var maxNs float64
	for _, r := range reports {
		for _, name := range names {
			if result, ok := r.Results[name]; ok {
				if result.NsPerOp > maxNs {
					maxNs = result.NsPerOp
				}
			}
		}
	}

	// 颜色
	colors := []string{"#4CAF50", "#2196F3", "#FF9800", "#E91E63", "#9C27B0", "#00BCD4", "#795548", "#607D8B"}

	var svg strings.Builder
	svg.WriteString(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">
<style>
  .title { font: bold 20px sans-serif; fill: #333; }
  .subtitle { font: 12px sans-serif; fill: #666; }
  .axis-label { font: 11px sans-serif; fill: #666; }
  .legend { font: 11px sans-serif; fill: #333; }
  .grid { stroke: #eee; stroke-width: 1; }
  .line { fill: none; stroke-width: 2; }
  .dot { stroke-width: 2; }
</style>
<rect width="100%%" height="100%%" fill="#fafafa"/>
`, width, height, width, height))

	// 标题
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="35" class="title" text-anchor="middle">zmsg Performance Trend</text>
`, width/2))
	svg.WriteString(fmt.Sprintf(`<text x="%d" y="55" class="subtitle" text-anchor="middle">%d reports from %s to %s</text>
`, width/2, len(reports),
		reports[0].Timestamp.Format("2006-01-02"),
		reports[len(reports)-1].Timestamp.Format("2006-01-02")))

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

	// X轴标签
	for i, r := range reports {
		x := marginLeft + (chartWidth * i / (len(reports) - 1))
		if len(reports) == 1 {
			x = marginLeft + chartWidth/2
		}
		svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="axis-label" text-anchor="middle">%s</text>
`, x, marginTop+chartHeight+20, r.Timestamp.Format("01/02")))
	}

	// 绘制折线
	for nameIdx, name := range names {
		color := colors[nameIdx%len(colors)]
		var points []string
		var dotPoints []struct{ x, y int }

		for i, r := range reports {
			if result, ok := r.Results[name]; ok {
				x := marginLeft + (chartWidth * i / max(1, len(reports)-1))
				y := marginTop + chartHeight - int(float64(chartHeight)*result.NsPerOp/maxNs)
				points = append(points, fmt.Sprintf("%d,%d", x, y))
				dotPoints = append(dotPoints, struct{ x, y int }{x, y})
			}
		}

		if len(points) > 0 {
			// 折线
			svg.WriteString(fmt.Sprintf(`<polyline points="%s" class="line" stroke="%s"/>
`, strings.Join(points, " "), color))

			// 数据点
			for _, p := range dotPoints {
				svg.WriteString(fmt.Sprintf(`<circle cx="%d" cy="%d" r="4" class="dot" fill="%s" stroke="white"/>
`, p.x, p.y, color))
			}
		}
	}

	// 图例
	legendX := width - marginRight + 20
	legendY := marginTop
	for i, name := range names {
		shortName := strings.TrimPrefix(name, "Benchmark")
		if len(shortName) > 20 {
			shortName = shortName[:17] + "..."
		}
		color := colors[i%len(colors)]
		y := legendY + i*20

		svg.WriteString(fmt.Sprintf(`<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>
`, legendX, y, legendX+20, y, color))
		svg.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="legend">%s</text>
`, legendX+25, y+4, shortName))
	}

	svg.WriteString("</svg>")
	_ = os.WriteFile(filename, []byte(svg.String()), 0644)
}

func generateCompareMarkdown(reports []BenchReport, filename string) {
	var md strings.Builder

	md.WriteString("# zmsg Benchmark Comparison\n\n")
	md.WriteString(fmt.Sprintf("Comparing %d reports\n\n", len(reports)))

	// 表头
	md.WriteString("| Benchmark |")
	for _, r := range reports {
		md.WriteString(fmt.Sprintf(" %s |", r.Timestamp.Format("01/02 15:04")))
	}
	md.WriteString(" Change |\n")

	md.WriteString("|-----------|")
	for range reports {
		md.WriteString("------------|")
	}
	md.WriteString("--------|\n")

	// 收集所有 benchmark 名称
	benchNames := make(map[string]bool)
	for _, r := range reports {
		for name := range r.Results {
			benchNames[name] = true
		}
	}

	var names []string
	for name := range benchNames {
		names = append(names, name)
	}
	sort.Strings(names)

	// 数据行
	for _, name := range names {
		shortName := strings.TrimPrefix(name, "Benchmark")
		md.WriteString(fmt.Sprintf("| %s |", shortName))

		var first, last float64
		for i, r := range reports {
			if result, ok := r.Results[name]; ok {
				md.WriteString(fmt.Sprintf(" %s |", formatNs(result.NsPerOp)))
				if i == 0 {
					first = result.NsPerOp
				}
				last = result.NsPerOp
			} else {
				md.WriteString(" - |")
			}
		}

		// 变化百分比
		if first > 0 && last > 0 {
			change := (last - first) / first * 100
			if change > 0 {
				md.WriteString(fmt.Sprintf(" 🔴 +%.1f%% |", change))
			} else if change < 0 {
				md.WriteString(fmt.Sprintf(" 🟢 %.1f%% |", change))
			} else {
				md.WriteString(" ⚪ 0% |")
			}
		} else {
			md.WriteString(" - |")
		}
		md.WriteString("\n")
	}

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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
