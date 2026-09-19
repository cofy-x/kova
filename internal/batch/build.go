package batch

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"

	"go.opentelemetry.io/otel/attribute"
)

func RunBuild(opts Options) error {
	opCtx := opts.Ctx
	if opCtx == nil {
		opCtx = context.Background()
	}
	opts.BuildFormat = source.NormalizeBuildFormatValue(opts.BuildFormat)
	opCtx, op := observability.StartOperation(opCtx, observability.OperationConfig{
		Name: "kova.batch.build",
		SpanAttrs: []attribute.KeyValue{
			attribute.String("kova.build.format", opts.BuildFormat),
			attribute.Int("kova.concurrency", opts.Concurrency),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.String("kova.build.format", opts.BuildFormat),
		},
		Counter:  observability.Instrument{Name: "kova_batch_builds_total", Description: "Batch build operations"},
		Duration: observability.Instrument{Name: "kova_batch_build_duration_seconds", Description: "Batch build operation duration"},
	})
	var runErr error
	defer func() { op.End(runErr) }()

	if opts.Ctx == nil {
		logging.ResetCommandStartTime(time.Now())
	}

	if len(opts.Addrs) == 0 && len(opts.PlatformAddrs) == 0 {
		runErr = fmt.Errorf("BuildKit platform addresses are required")
		return runErr
	}

	buildFormats, err := source.ParseBuildFormats(opts.BuildFormat)
	if err != nil {
		runErr = err
		return runErr
	}

	imageDirs := opts.ImageDirs
	var singleCleanup func()
	if opts.ImageDir != "" {
		var err error
		imageDirs, singleCleanup, err = source.PrepareSingleImageDir(opts.ImageDir, opts.Target, opts.Platform, opts.Vars)
		if err != nil {
			runErr = err
			return runErr
		}
		defer singleCleanup()
	}

	specs, cleanup, err := source.LoadBuildSpecsForFormats(imageDirs, opts.Target, opts.Platform, buildFormats, opts.Vars)
	if err != nil {
		runErr = err
		return runErr
	}
	if cleanup != nil {
		defer cleanup()
	}
	if len(specs) == 0 {
		if opts.Target != "" {
			runErr = fmt.Errorf("build target %q did not match any image metadata for format %q", opts.Target, source.NormalizeBuildFormatValue(opts.BuildFormat))
			return runErr
		}
		logging.Infof("No targets to build")
		op.SetResult(observability.ResultSkipped)
		return nil
	}
	jobs := groupBuildSpecs(specs)
	if len(jobs) == 0 {
		logging.Infof("No targets to build")
		op.SetResult(observability.ResultSkipped)
		return nil
	}

	resultStore, err := store.NewStore(opts.ResultPath, opts.LogsPath)
	if err != nil {
		runErr = err
		return runErr
	}
	defer resultStore.Close()

	totalTargets := len(jobs)

	logging.ResetProgress(len(jobs))
	defer logging.ClearProgress()

	platformPools := make(map[string]*scheduler.Pool, len(opts.PlatformAddrs))
	addressCount := 0
	for platform, addrs := range opts.PlatformAddrs {
		platformPools[platform] = scheduler.NewPool(addrs, opts.Concurrency)
		addressCount += len(addrs)
	}
	var defaultPool *scheduler.Pool
	if len(opts.Addrs) > 0 {
		defaultPool = scheduler.NewPool(opts.Addrs, opts.Concurrency)
		addressCount += len(opts.Addrs)
	}
	logging.Infof("Building %d target(s) across %d platform-scoped address(es), concurrency=%d",
		len(jobs), addressCount, opts.Concurrency)
	globalSem := make(chan struct{}, opts.Concurrency)
	outcomeCounters := store.NewOutcomeCounters(totalTargets, nil)

	ctx, cancel := context.WithCancel(opCtx)
	defer cancel()
	if defaultPool != nil {
		scheduler.StartRefresher(ctx, defaultPool, opts.AddrsRaw, opts.OOMCooldown)
	}
	for platform, pool := range platformPools {
		scheduler.StartRefresher(ctx, pool, opts.PlatformAddrsRaw[platform], opts.OOMCooldown)
	}

	if opts.Ctx == nil {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigCh
			logging.Infof("Received interrupt, cancelling builds...")
			cancel()
		}()
	}

	type buildTask struct {
		job buildJob
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		results  []store.Entry
		failed   atomic.Bool
		fatalMu  sync.Mutex
		fatalErr error
	)

	recordFatalErr := func(err error) {
		if err == nil {
			return
		}
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		fatalMu.Unlock()
	}

	// scheduleTask picks a slot via consistent hash and launches the build goroutine.
	scheduleTask := func(task buildTask) bool {
		if failed.Load() && opts.Failfast {
			return false
		}
		if ctx.Err() != nil {
			return false
		}

		addrPool := platformPools[task.job.platform]
		if addrPool == nil {
			addrPool = defaultPool
		}
		if addrPool == nil {
			recordFatalErr(fmt.Errorf("no BuildKit worker capacity for platform %s", task.job.platform))
			failed.Store(true)
			return false
		}
		var slot *scheduler.Slot
		for {
			if ctx.Err() != nil {
				return false
			}

			snapshot := addrPool.Snapshot()
			slot = scheduler.PickAvailableSlot(snapshot, task.job.key)
			if slot == nil {
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return false
				}
				continue
			}

			break
		}

		select {
		case globalSem <- struct{}{}:
		case <-ctx.Done():
			<-slot.Sem
			return false
		}

		wg.Add(1)
		go func(task buildTask, slot *scheduler.Slot) {
			defer wg.Done()
			defer func() { <-globalSem }()
			defer func() { <-slot.Sem }()

			taskStartedAt := time.Now()
			taskEntry := store.Entry{
				StartedAt: taskStartedAt.Format(time.RFC3339),
				Target:    task.job.key,
				Success:   true,
			}

			for _, spec := range task.job.specs {
				entry := executeBuild(ctx, spec, slot.Addr, opts)
				taskEntry.NodeIP = entry.NodeIP

				if err := resultStore.UpsertResult(entry); err != nil {
					persistErr := fmt.Errorf("store result for %s: %w", entry.Target, err)
					logging.Errorf("Failed to store result for %s: %v", entry.Target, err)
					recordFatalErr(persistErr)
					failed.Store(true)
					cancel()
					return
				}

				if !entry.Success {
					taskEntry.Success = false
					break
				}
			}

			finishedAt := time.Now()
			taskEntry.FinishedAt = finishedAt.Format(time.RFC3339)
			taskEntry.Elapsed = logging.FormatElapsed(finishedAt.Sub(taskStartedAt))

			logging.AdvanceProgress()

			succeededCount, totalCount, failedCount := outcomeCounters.Apply(task.job.key, taskEntry.Success)

			mu.Lock()
			results = append(results, taskEntry)
			mu.Unlock()

			printBuildResult(taskEntry, succeededCount, totalCount, failedCount)

			if !taskEntry.Success {
				failed.Store(true)
				if opts.Failfast {
					cancel()
				}
			}
		}(task, slot)

		return true
	}

	// First pass: schedule all initial tasks.
	for _, job := range jobs {
		if !scheduleTask(buildTask{job: job}) {
			break
		}
	}

	wg.Wait()

	printSummary(results)

	fatalMu.Lock()
	defer fatalMu.Unlock()
	if fatalErr != nil {
		runErr = fatalErr
		return runErr
	}
	if err := opCtx.Err(); err != nil {
		runErr = err
		return runErr
	}

	if failed.Load() {
		runErr = fmt.Errorf("one or more builds failed")
		return runErr
	}
	return nil
}
