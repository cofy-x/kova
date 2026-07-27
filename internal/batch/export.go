package batch

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"

	"go.opentelemetry.io/otel/attribute"
)

func RunExport(opts Options) (err error) {
	opCtx := opts.Ctx
	if opCtx == nil {
		opCtx = context.Background()
	}
	_, op := observability.StartOperation(opCtx, observability.OperationConfig{
		Name: "kova.batch.export",
		SpanAttrs: []attribute.KeyValue{
			attribute.Bool("kova.oci", opts.OCI),
			attribute.Bool("kova.with_fail", opts.WithFail),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.Bool("kova.oci", opts.OCI),
		},
		Counter:  observability.Instrument{Name: "kova_batch_exports_total", Description: "Batch export operations"},
		Duration: observability.Instrument{Name: "kova_batch_export_duration_seconds", Description: "Batch export duration"},
	})
	defer func() { op.End(err) }()

	logging.ResetCommandStartTime(time.Now())

	db, err := store.Open(opts.FromResultPath)
	if err != nil {
		return fmt.Errorf("open result database: %w", err)
	}
	defer db.Close()

	entries, err := db.All()
	if err != nil {
		return err
	}
	entries, err = selectExportEntries(entries, opts)
	if err != nil {
		return err
	}

	logging.ResetProgress(len(entries))
	defer logging.ClearProgress()

	file, err := os.Create(opts.ResultPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var exported int
	for _, entry := range entries {
		payload, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := writer.Write(payload); err != nil {
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
		exported++
		logging.AdvanceProgress()
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	op.SetAttributes(attribute.Int("kova.entries", exported))
	logging.Infof("Exported %d entries to %s", exported, opts.ResultPath)
	return nil
}

func selectExportEntries(entries []store.Entry, opts Options) ([]store.Entry, error) {
	exportable := make(map[string]store.Entry, len(entries))
	ordered := make([]store.Entry, 0, len(entries))
	for _, entry := range entries {
		if !source.TargetMatchesMode(entry.Target, opts.OCI) || (!entry.Success && !opts.WithFail) {
			continue
		}
		exportable[entry.Target] = entry
		ordered = append(ordered, entry)
	}
	if len(opts.ExportTargets) == 0 {
		return ordered, nil
	}

	selected := make([]store.Entry, 0, len(opts.ExportTargets))
	seen := make(map[string]struct{}, len(opts.ExportTargets))
	for _, rawTarget := range opts.ExportTargets {
		target := strings.TrimSpace(rawTarget)
		if target == "" {
			return nil, fmt.Errorf("export target must not be empty")
		}
		if _, duplicate := seen[target]; duplicate {
			continue
		}
		entry, ok := exportable[target]
		if !ok {
			mode := "build"
			if opts.OCI {
				mode = "OCI build"
			}
			if !opts.WithFail {
				mode = "successful " + mode
			}
			return nil, fmt.Errorf("export target %q was not found as a %s result", target, mode)
		}
		seen[target] = struct{}{}
		selected = append(selected, entry)
	}
	return selected, nil
}
