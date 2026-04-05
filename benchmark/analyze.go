package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type AnalysisSummary struct {
	RunID           string                        `json:"run_id"`
	RunName         string                        `json:"run_name"`
	Topology        string                        `json:"topology"`
	ClientPlacement string                        `json:"client_placement"`
	GitSHA          string                        `json:"git_sha"`
	Scenarios       []ScenarioReport              `json:"scenarios"`
	Coordinator     map[string]float64            `json:"coordinator_metrics"`
	Storage         map[string]map[string]float64 `json:"storage_metrics"`
	System          map[string]SystemSeries       `json:"system"`
}

type SystemSeries struct {
	CPUUtilization []Point `json:"cpu_utilization"`
	NetworkRXMBps  []Point `json:"network_rx_mbps"`
	NetworkTXMBps  []Point `json:"network_tx_mbps"`
	DiskReadMBps   []Point `json:"disk_read_mbps"`
	DiskWriteMBps  []Point `json:"disk_write_mbps"`
}

type Point struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

func AnalyzeRun(runDir string) (AnalysisSummary, error) {
	state, err := ReadRunState(filepath.Join(runDir, RunStateFileName))
	if err != nil {
		return AnalysisSummary{}, err
	}
	var report LoadGenReport
	if err := LoadJSON(filepath.Join(runDir, "artifacts", "client", "loadgen-report.json"), &report); err != nil {
		return AnalysisSummary{}, err
	}
	summary := AnalysisSummary{
		RunID:           state.RunID,
		RunName:         state.RunName,
		Topology:        state.Topology,
		ClientPlacement: state.ClientPlacement,
		GitSHA:          state.GitSHA,
		Scenarios:       report.Scenarios,
		Coordinator:     map[string]float64{},
		Storage:         map[string]map[string]float64{},
		System:          map[string]SystemSeries{},
	}

	coordMetrics, err := parsePromMetrics(filepath.Join(runDir, "artifacts", "coordinator", "metrics.prom"))
	if err == nil {
		summary.Coordinator = coordMetrics
	}
	for _, nodeID := range []string{"storage-a", "storage-b", "storage-c", "client", "coordinator"} {
		probePath := filepath.Join(runDir, "artifacts", nodeID, "probe.jsonl")
		if series, err := buildSystemSeries(probePath); err == nil {
			summary.System[nodeID] = series
		}
		if strings.HasPrefix(nodeID, "storage-") {
			metrics, err := parsePromMetrics(filepath.Join(runDir, "artifacts", nodeID, "metrics.prom"))
			if err == nil {
				summary.Storage[nodeID] = metrics
			}
		}
	}

	analysisDir := filepath.Join(runDir, "analysis")
	if err := os.MkdirAll(analysisDir, 0o755); err != nil {
		return AnalysisSummary{}, fmt.Errorf("mkdir analysis dir: %w", err)
	}
	if err := SaveJSON(filepath.Join(analysisDir, "summary.json"), summary); err != nil {
		return AnalysisSummary{}, err
	}
	if err := writeHTMLReport(filepath.Join(analysisDir, "index.html"), summary); err != nil {
		return AnalysisSummary{}, err
	}
	return summary, nil
}

func parsePromMetrics(path string) (map[string]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse prometheus metrics %q: %w", path, err)
	}
	out := map[string]float64{}
	for name, family := range families {
		if len(family.Metric) == 0 {
			continue
		}
		if len(family.Metric) == 1 {
			out[name] = metricValue(family.Metric[0])
			continue
		}
		for _, metric := range family.Metric {
			labelParts := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				labelParts = append(labelParts, label.GetName()+"="+label.GetValue())
			}
			sort.Strings(labelParts)
			out[name+"{"+strings.Join(labelParts, ",")+"}"] = metricValue(metric)
		}
	}
	return out, nil
}

func metricValue(metric *dto.Metric) float64 {
	switch {
	case metric.Gauge != nil:
		return metric.Gauge.GetValue()
	case metric.Counter != nil:
		return metric.Counter.GetValue()
	case metric.Untyped != nil:
		return metric.Untyped.GetValue()
	case metric.Histogram != nil:
		return metric.Histogram.GetSampleSum()
	default:
		return 0
	}
}

func buildSystemSeries(path string) (SystemSeries, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SystemSeries{}, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return SystemSeries{}, fmt.Errorf("not enough probe data")
	}
	var samples []ProbeSample
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var sample ProbeSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			return SystemSeries{}, fmt.Errorf("decode probe line: %w", err)
		}
		samples = append(samples, sample)
	}
	if len(samples) < 2 {
		return SystemSeries{}, fmt.Errorf("not enough probe samples")
	}
	out := SystemSeries{}
	for i := 1; i < len(samples); i++ {
		prev := samples[i-1]
		cur := samples[i]
		seconds := cur.Timestamp.Sub(prev.Timestamp).Seconds()
		if seconds <= 0 {
			continue
		}
		totalPrev := prev.CPU.User + prev.CPU.Nice + prev.CPU.System + prev.CPU.Idle + prev.CPU.IOWait + prev.CPU.IRQ + prev.CPU.SoftIRQ + prev.CPU.Steal
		totalCur := cur.CPU.User + cur.CPU.Nice + cur.CPU.System + cur.CPU.Idle + cur.CPU.IOWait + cur.CPU.IRQ + cur.CPU.SoftIRQ + cur.CPU.Steal
		totalDelta := float64(totalCur - totalPrev)
		idleDelta := float64(cur.CPU.Idle - prev.CPU.Idle)
		cpuUtil := 0.0
		if totalDelta > 0 {
			cpuUtil = 100 * (1 - idleDelta/totalDelta)
		}
		out.CPUUtilization = append(out.CPUUtilization, Point{Timestamp: cur.Timestamp, Value: cpuUtil})
		out.NetworkRXMBps = append(out.NetworkRXMBps, Point{Timestamp: cur.Timestamp, Value: sumNetDelta(prev.Network, cur.Network, true) / seconds / 1024 / 1024})
		out.NetworkTXMBps = append(out.NetworkTXMBps, Point{Timestamp: cur.Timestamp, Value: sumNetDelta(prev.Network, cur.Network, false) / seconds / 1024 / 1024})
		readBytes, writeBytes := sumDiskDelta(prev.Disks, cur.Disks)
		out.DiskReadMBps = append(out.DiskReadMBps, Point{Timestamp: cur.Timestamp, Value: readBytes / seconds / 1024 / 1024})
		out.DiskWriteMBps = append(out.DiskWriteMBps, Point{Timestamp: cur.Timestamp, Value: writeBytes / seconds / 1024 / 1024})
	}
	return out, nil
}

func sumNetDelta(prev map[string]NetSample, cur map[string]NetSample, rx bool) float64 {
	total := 0.0
	for name, now := range cur {
		before := prev[name]
		if rx {
			total += float64(now.RXBytes - before.RXBytes)
		} else {
			total += float64(now.TXBytes - before.TXBytes)
		}
	}
	return total
}

func sumDiskDelta(prev map[string]DiskSample, cur map[string]DiskSample) (float64, float64) {
	var readBytes float64
	var writeBytes float64
	for name, now := range cur {
		before := prev[name]
		readBytes += float64(now.ReadSectors-before.ReadSectors) * 512
		writeBytes += float64(now.WriteSectors-before.WriteSectors) * 512
	}
	return readBytes, writeBytes
}

func writeHTMLReport(path string, summary AnalysisSummary) error {
	tpl := template.Must(template.New("report").Funcs(template.FuncMap{
		"chart":      renderPolyline,
		"metricRows": formatMetricRows,
	}).Parse(reportTemplate))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create html report: %w", err)
	}
	defer file.Close()
	return tpl.Execute(file, summary)
}

func formatMetricRows(metrics map[string]float64) []string {
	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]string, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, fmt.Sprintf("%s = %.3f", key, metrics[key]))
	}
	return rows
}

func renderPolyline(points []Point, width float64, height float64) template.HTML {
	if len(points) == 0 {
		return template.HTML("")
	}
	maxValue := 0.0
	for _, point := range points {
		maxValue = math.Max(maxValue, point.Value)
	}
	if maxValue == 0 {
		maxValue = 1
	}
	var parts []string
	for idx, point := range points {
		x := 0.0
		if len(points) > 1 {
			x = width * float64(idx) / float64(len(points)-1)
		}
		y := height - (height * point.Value / maxValue)
		parts = append(parts, fmt.Sprintf("%.2f,%.2f", x, y))
	}
	return template.HTML(strings.Join(parts, " "))
}

const reportTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>chainrep benchmark {{.RunID}}</title>
  <style>
    body { font-family: Georgia, serif; margin: 2rem auto; max-width: 1100px; color: #1d1d1d; background: linear-gradient(180deg, #f8f4ec, #ffffff); }
    h1, h2 { margin-bottom: 0.25rem; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 1.5rem; }
    th, td { border-bottom: 1px solid #d7d1c4; padding: 0.5rem; text-align: left; vertical-align: top; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; }
    .card { background: rgba(255,255,255,0.8); border: 1px solid #e2dbcf; border-radius: 12px; padding: 1rem; }
    code { font-size: 0.9rem; white-space: pre-wrap; }
    svg { width: 100%; height: 120px; background: #fffdf8; border-radius: 8px; border: 1px solid #eee6d8; }
    polyline { fill: none; stroke: #2b6b7f; stroke-width: 2; }
  </style>
</head>
<body>
  <h1>chainrep benchmark {{.RunID}}</h1>
  <p>run={{.RunName}} topology={{.Topology}} client_placement={{.ClientPlacement}} git={{.GitSHA}}</p>

  <h2>Scenarios</h2>
  <table>
    <thead><tr><th>Name</th><th>Kind</th><th>Concurrency</th><th>Throughput</th><th>P50</th><th>P95</th><th>P99</th><th>Errors</th></tr></thead>
    <tbody>
      {{range .Scenarios}}
      <tr>
        <td>{{.Name}}</td>
        <td>{{.Kind}}</td>
        <td>{{.Concurrency}}</td>
        <td>{{printf "%.2f ops/s" .Throughput}}</td>
        <td>{{printf "%.3f ms" .P50Millis}}</td>
        <td>{{printf "%.3f ms" .P95Millis}}</td>
        <td>{{printf "%.3f ms" .P99Millis}}</td>
        <td>{{.ErrorOps}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>

  <div class="grid">
    <div class="card">
      <h2>Coordinator Metrics</h2>
      <code>{{range metricRows .Coordinator}}{{.}}
{{end}}</code>
    </div>
    <div class="card">
      <h2>Storage Metrics</h2>
      {{range $node, $metrics := .Storage}}
      <h3>{{$node}}</h3>
      <code>{{range metricRows $metrics}}{{.}}
{{end}}</code>
      {{end}}
    </div>
  </div>

  <h2>System Charts</h2>
  <div class="grid">
    {{range $node, $series := .System}}
    <div class="card">
      <h3>{{$node}} CPU</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.CPUUtilization 300 120}}"></polyline></svg>
      <h3>{{$node}} Network RX</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.NetworkRXMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Network TX</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.NetworkTXMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Disk Read</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.DiskReadMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Disk Write</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.DiskWriteMBps 300 120}}"></polyline></svg>
    </div>
    {{end}}
  </div>
</body>
</html>`
