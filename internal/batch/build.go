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

	if len(opts.Addrs) == 0 {
		runErr = fmt.Errorf("--addrs is required")
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
		imageDirs, singleCleanup, err = source.PrepareSingleImageDir(opts.ImageDir, opts.Target, opts.Vars)
		if err != nil {
			runErr = err
			return runErr
		}
		defer singleCleanup()
	}

	specs, cleanup, err := source.LoadBuildSpecsForFormats(imageDirs, opts.Target, buildFormats, opts.Vars)
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
	jobs, existingOutcomes, skippedSucceeded, skippedFailed, err := filterBuildJobs(jobs, resultStore, opts.SkipFail)
	if err != nil {
		runErr = err
		return runErr
	}

	if skippedSucceeded > 0 || skippedFailed > 0 {
		logging.Infof("Skipped %d previously successful target(s) and %d previously failed target(s)", skippedSucceeded, skippedFailed)
	}
	if len(jobs) == 0 {
		logging.Infof("No targets to build")
		op.SetResult(observability.ResultSkipped)
		return nil
	}

	logging.ResetProgress(len(jobs))
	defer logging.ClearProgress()

	logging.Infof("Building %d target(s) across %d address(es), concurrency=%d, retry=%d",
		len(jobs), len(opts.Addrs), opts.Concurrency, opts.Retry)

	addrPool := scheduler.NewPool(opts.Addrs, opts.Concurrency)
	globalSem := make(chan struct{}, opts.Concurrency)
	outcomeCounters := store.NewOutcomeCounters(totalTargets, existingOutcomes)

	ctx, cancel := context.WithCancel(opCtx)
	defer cancel()
	scheduler.StartRefresher(ctx, addrPool, opts.AddrsRaw, opts.OOMCooldown)

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
		job     buildJob
		attempt int
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

	// retryQueue collects failed tasks that still have retry budget.
	retryQueue := make(chan buildTask, len(jobs))

	// scheduleTask picks a slot via consistent hash and launches the build goroutine.
	scheduleTask := func(task buildTask) bool {
		if failed.Load() && opts.Failfast {
			return false
		}
		if ctx.Err() != nil {
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

			for idx, spec := range task.job.specs {
				entry := executeBuild(ctx, spec, slot.Addr, opts)
				taskEntry.NodeIP = entry.NodeIP

				if !entry.Success && task.attempt < opts.Retry {
					// Log every failure to logs.jsonl, even if we will retry.
					if entry.Logs != "" {
						if err := resultStore.AppendFailure(entry.Target, entry.Logs); err != nil {
							logging.Errorf("Failed to append failure log for %s: %v", entry.Target, err)
						}
					}
					remainingSpecs := append([]source.Spec(nil), task.job.specs[idx:]...)
					logging.Infof("[RETRY %d/%d] %s", task.attempt+1, opts.Retry, entry.Target)
					retryQueue <- buildTask{
						job: buildJob{
							key:   task.job.key,
							specs: remainingSpecs,
						},
						attempt: task.attempt + 1,
					}
					return
				}

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
		if !scheduleTask(buildTask{job: job, attempt: 0}) {
			break
		}
	}

	// Drain retry queue: wait for in-flight builds to finish, then re-schedule retries.
	for {
		wg.Wait()
		select {
		case task := <-retryQueue:
			// There may be more queued retries; drain them all before waiting again.
			tasks := []buildTask{task}
			for {
				select {
				case t := <-retryQueue:
					tasks = append(tasks, t)
				default:
					goto schedule
				}
			}
		schedule:
			for _, t := range tasks {
				if !scheduleTask(t) {
					break
				}
			}
		default:
			// No retries pending, we're done.
			goto done
		}
	}
done:

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
