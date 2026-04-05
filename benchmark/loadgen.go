package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danthegoodman1/chainrep/client"
	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/quickstart"
	"github.com/danthegoodman1/chainrep/storage"
	"github.com/danthegoodman1/chainrep/transport/grpcx"
)

type LoadGenProcessConfig struct {
	RunID        string          `json:"run_id"`
	ManifestPath string          `json:"manifest_path"`
	OutputDir    string          `json:"output_dir"`
	Workload     WorkloadProfile `json:"workload"`
}

type latencySample struct {
	At         time.Time `json:"at"`
	Millis     float64   `json:"latency_ms"`
	Successful bool      `json:"successful"`
	Operation  string    `json:"operation"`
	Error      string    `json:"error,omitempty"`
}

func RunLoadGen(ctx context.Context, cfg LoadGenProcessConfig) (LoadGenReport, error) {
	if cfg.ManifestPath == "" {
		return LoadGenReport{}, fmt.Errorf("loadgen manifest path must not be empty")
	}
	if cfg.OutputDir == "" {
		return LoadGenReport{}, fmt.Errorf("loadgen output dir must not be empty")
	}
	manifest, err := quickstart.Load(cfg.ManifestPath)
	if err != nil {
		return LoadGenReport{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return LoadGenReport{}, fmt.Errorf("mkdir output dir: %w", err)
	}
	report := LoadGenReport{RunID: cfg.RunID, StartedAt: time.Now().UTC()}
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	transport := grpcx.NewClientTransport(pool)
	rng := rand.New(rand.NewSource(cfg.Workload.Seed))

	preloadStarted := time.Now()
	for i := 0; i < cfg.Workload.PreloadKeys; i++ {
		key := fmt.Sprintf("preload-%06d", i)
		value := sizedValue("preload", cfg.Workload.ValueBytes)
		reqCtx, cancel := context.WithTimeout(ctx, cfg.Workload.RequestTimeout)
		_, _, _, err := putWithRefresh(reqCtx, admin, transport, key, value)
		cancel()
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("preload key %q: %v", key, err))
			return report, err
		}
	}
	report.Preload = PreloadReport{
		Keys:     cfg.Workload.PreloadKeys,
		Duration: time.Since(preloadStarted),
	}

	for _, scenario := range cfg.Workload.Scenarios {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		default:
		}
		result, err := runScenario(ctx, admin, transport, cfg.OutputDir, cfg.Workload, scenario, rng)
		if err != nil {
			report.Errors = append(report.Errors, err.Error())
			return report, err
		}
		report.Scenarios = append(report.Scenarios, result)
		if cfg.Workload.PerScenarioPause > 0 {
			select {
			case <-ctx.Done():
				return report, ctx.Err()
			case <-time.After(cfg.Workload.PerScenarioPause):
			}
		}
	}
	report.FinishedAt = time.Now().UTC()
	if err := SaveJSON(filepath.Join(cfg.OutputDir, "loadgen-report.json"), report); err != nil {
		return LoadGenReport{}, err
	}
	if err := SaveJSON(filepath.Join(cfg.OutputDir, "loadgen-manifest.json"), manifest); err != nil {
		return LoadGenReport{}, err
	}
	return report, nil
}

func runScenario(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	outputDir string,
	workload WorkloadProfile,
	scenario ScenarioProfile,
	rng *rand.Rand,
) (ScenarioReport, error) {
	report := ScenarioReport{
		Name:        scenario.Name,
		Kind:        scenario.Kind,
		Concurrency: scenario.Concurrency,
		Warmup:      scenario.Warmup,
		Duration:    scenario.Duration,
		StartedAt:   time.Now().UTC(),
	}
	samplePath := filepath.Join(outputDir, scenario.Name+"-samples.jsonl")
	file, err := os.Create(samplePath)
	if err != nil {
		return ScenarioReport{}, fmt.Errorf("create sample file: %w", err)
	}
	defer file.Close()

	type opResult struct {
		at        time.Time
		latency   time.Duration
		success   bool
		operation string
		err       string
	}

	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var warmup atomic.Bool
	if scenario.Warmup > 0 {
		warmup.Store(true)
		go func() {
			select {
			case <-opCtx.Done():
			case <-time.After(scenario.Warmup):
				warmup.Store(false)
			}
		}()
	}
	endTimer := time.NewTimer(scenario.Warmup + scenario.Duration)
	defer endTimer.Stop()

	results := make(chan opResult, scenario.Concurrency*2)
	var wg sync.WaitGroup
	for workerID := 0; workerID < scenario.Concurrency; workerID++ {
		workerSeed := rng.Int63()
		wg.Add(1)
		go func(workerID int, seed int64) {
			defer wg.Done()
			workerRand := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-opCtx.Done():
					return
				default:
				}
				start := time.Now()
				kind := scenario.Kind
				if kind == "mixed" {
					if workerRand.Intn(100) < scenario.ReadPercent {
						kind = "get"
					} else {
						kind = "put"
					}
				}
				keyIndex := workerRand.Intn(workload.PreloadKeys)
				key := fmt.Sprintf("preload-%06d", keyIndex)
				requestCtx, cancel := context.WithTimeout(opCtx, workload.RequestTimeout)
				var callErr error
				switch kind {
				case "get":
					_, _, _, callErr = getWithRefresh(requestCtx, admin, transport, key)
				case "put":
					value := sizedValue(fmt.Sprintf("w-%d-%d", workerID, workerRand.Int63()), scenario.ValueBytes)
					_, _, _, callErr = putWithRefresh(requestCtx, admin, transport, key, value)
				default:
					callErr = fmt.Errorf("unsupported scenario kind %q", kind)
				}
				cancel()
				result := opResult{
					at:        time.Now().UTC(),
					latency:   time.Since(start),
					success:   callErr == nil,
					operation: kind,
				}
				if callErr != nil {
					result.err = callErr.Error()
				}
				select {
				case <-opCtx.Done():
					return
				case results <- result:
				}
			}
		}(workerID, workerSeed)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(results)
	}()

	var samples []float64
	var intervalSamples []latencySample
	intervalStarted := time.Now()
	flushInterval := func(now time.Time) {
		if len(intervalSamples) == 0 {
			return
		}
		report.Intervals = append(report.Intervals, buildIntervalSummary(now, intervalSamples))
		intervalSamples = intervalSamples[:0]
		intervalStarted = now
	}

loop:
	for {
		select {
		case <-ctx.Done():
			cancel()
			<-done
			return ScenarioReport{}, ctx.Err()
		case <-endTimer.C:
			cancel()
		case result, ok := <-results:
			if !ok {
				break loop
			}
			sample := latencySample{
				At:         result.at,
				Millis:     float64(result.latency) / float64(time.Millisecond),
				Successful: result.success,
				Operation:  result.operation,
				Error:      result.err,
			}
			if err := writeJSONLine(file, sample); err != nil {
				return ScenarioReport{}, err
			}
			if warmup.Load() {
				continue
			}
			report.TotalOps++
			if result.success {
				report.SuccessOps++
			} else {
				report.ErrorOps++
			}
			if result.operation == "get" {
				report.ReadOps++
			} else {
				report.WriteOps++
			}
			samples = append(samples, sample.Millis)
			intervalSamples = append(intervalSamples, sample)
			if sample.At.Sub(intervalStarted) >= workload.Interval {
				flushInterval(sample.At)
			}
		}
	}
	flushInterval(time.Now().UTC())
	report.FinishedAt = time.Now().UTC()
	report.P50Millis = percentile(samples, 50)
	report.P95Millis = percentile(samples, 95)
	report.P99Millis = percentile(samples, 99)
	report.MaxMillis = percentile(samples, 100)
	measuredDuration := report.FinishedAt.Sub(report.StartedAt) - scenario.Warmup
	if measuredDuration > 0 {
		report.Throughput = float64(report.TotalOps) / measuredDuration.Seconds()
	}
	report.Histogram = histogram(samples)
	return report, nil
}

func buildIntervalSummary(at time.Time, samples []latencySample) IntervalSample {
	latencies := make([]float64, 0, len(samples))
	var ops, errs, reads, writes int64
	for _, sample := range samples {
		ops++
		if !sample.Successful {
			errs++
		}
		if sample.Operation == "get" {
			reads++
		} else {
			writes++
		}
		latencies = append(latencies, sample.Millis)
	}
	return IntervalSample{
		Timestamp: at.UTC(),
		Ops:       ops,
		Errors:    errs,
		Reads:     reads,
		Writes:    writes,
		P50Millis: percentile(latencies, 50),
		P95Millis: percentile(latencies, 95),
		P99Millis: percentile(latencies, 99),
	}
}

func histogram(samples []float64) []HistogramBucket {
	if len(samples) == 0 {
		return nil
	}
	limits := []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000}
	maxSample := 0.0
	for _, sample := range samples {
		if sample > maxSample {
			maxSample = sample
		}
	}
	buckets := make([]HistogramBucket, 0, len(limits)+1)
	for _, limit := range limits {
		buckets = append(buckets, HistogramBucket{UpperMillis: limit})
	}
	if maxSample > limits[len(limits)-1] {
		buckets = append(buckets, HistogramBucket{UpperMillis: math.Ceil(maxSample)})
	}
	for _, sample := range samples {
		for i := range buckets {
			if sample <= buckets[i].UpperMillis {
				buckets[i].Count++
				break
			}
		}
	}
	return buckets
}

func percentile(samples []float64, pct float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cloned := slices.Clone(samples)
	sort.Float64s(cloned)
	if pct >= 100 {
		return cloned[len(cloned)-1]
	}
	rank := (pct / 100) * float64(len(cloned)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return cloned[lo]
	}
	frac := rank - float64(lo)
	return cloned[lo] + (cloned[hi]-cloned[lo])*frac
}

func writeJSONLine(file *os.File, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal jsonl: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write jsonl: %w", err)
	}
	return nil
}

func sizedValue(seed string, size int) string {
	if size <= len(seed) {
		return seed[:size]
	}
	var b strings.Builder
	for b.Len() < size {
		b.WriteString(seed)
		b.WriteByte('-')
	}
	out := b.String()
	return out[:size]
}

func getWithRefresh(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	key string,
) (coordserver.SlotRoute, string, storage.ReadResult, error) {
	return withSnapshotRetry(
		ctx,
		admin,
		key,
		func(route coordserver.SlotRoute, target string) (storage.ReadResult, error) {
			if !route.Readable || route.TailNodeID == "" {
				return storage.ReadResult{}, fmt.Errorf("%w: slot %d is not readable", client.ErrNoRoute, route.Slot)
			}
			return transport.Get(ctx, target, storage.ClientGetRequest{
				Slot:                 route.Slot,
				Key:                  key,
				ExpectedChainVersion: route.ChainVersion,
			})
		},
		func(route coordserver.SlotRoute) string { return routeTarget(route.TailEndpoint, route.TailNodeID) },
	)
}

func putWithRefresh(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	key string,
	value string,
) (coordserver.SlotRoute, string, storage.CommitResult, error) {
	return withSnapshotRetry(
		ctx,
		admin,
		key,
		func(route coordserver.SlotRoute, target string) (storage.CommitResult, error) {
			if !route.Writable || route.HeadNodeID == "" {
				return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", client.ErrNoRoute, route.Slot)
			}
			return transport.Put(ctx, target, storage.ClientPutRequest{
				Slot:                 route.Slot,
				Key:                  key,
				Value:                value,
				ExpectedChainVersion: route.ChainVersion,
			})
		},
		func(route coordserver.SlotRoute) string { return routeTarget(route.HeadEndpoint, route.HeadNodeID) },
	)
}

func withSnapshotRetry[T any](
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	key string,
	call func(coordserver.SlotRoute, string) (T, error),
	targetFor func(coordserver.SlotRoute) string,
) (coordserver.SlotRoute, string, T, error) {
	var zero T
	snapshot, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, fmt.Errorf("fetch routing snapshot: %w", err)
	}
	route, err := client.RouteForKey(snapshot, key)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	target := targetFor(route)
	result, err := call(route, target)
	if err == nil {
		return route, target, result, nil
	}
	if !isRoutingMismatchError(err) {
		return coordserver.SlotRoute{}, "", zero, err
	}
	snapshot, err = admin.RoutingSnapshot(ctx)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, fmt.Errorf("refresh routing snapshot: %w", err)
	}
	route, err = client.RouteForKey(snapshot, key)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	target = targetFor(route)
	result, err = call(route, target)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	return route, target, result, nil
}

func isRoutingMismatchError(err error) bool {
	var mismatch *storage.RoutingMismatchError
	return errors.As(err, &mismatch) || strings.Contains(err.Error(), storage.ErrRoutingMismatch.Error())
}

func routeTarget(endpoint string, fallback string) string {
	if endpoint != "" {
		return endpoint
	}
	return fallback
}
