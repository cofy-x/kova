package buildcontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/store"
)

// genesisReceiptExporter permits only the bounded summary export of an
// already accepted request. Despite the runner's POST transport, this export
// reads its result DB and writes a temporary JSONL file; it cannot start a
// build or push an image. The Pod/CR/request identity is checked around every
// Exec, including when a committed admission ledger has disappeared.
type genesisReceiptExporter struct {
	reconciler *KovaBuildReconciler
	client     runnerexec.Client
}

func (e genesisReceiptExporter) Post(ctx context.Context, build *kovav1.KovaBuild, path, query string) ([]byte, error) {
	if path != "export" || (query != "with-fail=true&summary=true" && query != "with-fail=true&summary=true&oci=true") {
		return nil, fmt.Errorf("Genesis evidence export permits only bounded result summaries")
	}
	before, pod, err := e.reconciler.directGenesisWitness(ctx, build)
	if err != nil {
		return nil, err
	}
	if before.Status.Phase != kovav1.PhaseVerifying && before.Status.Phase != kovav1.PhaseFailedVerifying {
		return nil, fmt.Errorf("Genesis evidence export requires an accepted verification phase")
	}
	state, err := e.reconciler.observeBuildStatus(ctx, e.client, before)
	if err != nil {
		return nil, err
	}
	if (before.Status.Phase == kovav1.PhaseVerifying && state.Status != "completed") ||
		(before.Status.Phase == kovav1.PhaseFailedVerifying && state.Status != "failed" && state.Status != "error") {
		return nil, fmt.Errorf("Genesis evidence export requires matching terminal runner status")
	}
	data, postErr := e.client.Post(ctx, before, path, query)
	after, laterPod, err := e.reconciler.directGenesisWitness(ctx, before)
	if err != nil {
		return nil, err
	}
	if !samePodUID(pod, laterPod) {
		return nil, fmt.Errorf("Genesis runner Pod UID changed across evidence export")
	}
	if after.Status.Phase != before.Status.Phase {
		return nil, fmt.Errorf("Genesis CR phase changed across evidence export")
	}
	laterState, err := e.reconciler.observeBuildStatus(ctx, e.client, after)
	if err != nil {
		return nil, err
	}
	if laterState.Status != state.Status || laterState.RequestID != state.RequestID {
		return nil, fmt.Errorf("Genesis runner request changed across evidence export")
	}
	if postErr != nil {
		return nil, postErr
	}
	if err := validateGenesisExportTargets(data, before, query); err != nil {
		return nil, err
	}
	return data, nil
}

func validateGenesisExportTargets(data []byte, build *kovav1.KovaBuild, query string) error {
	format := "nydus"
	if query == "with-fail=true&summary=true&oci=true" {
		format = "oci"
	}
	expected := make(map[string]struct{})
	for _, result := range buildresult.Pending(build) {
		if result.Format == format {
			expected[result.Repository] = struct{}{}
		}
	}
	if len(expected) == 0 {
		return fmt.Errorf("Genesis export format has no requested targets")
	}
	seen := make(map[string]struct{}, len(expected))
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var entry store.Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("Genesis export contains invalid JSONL: %w", err)
		}
		if _, ok := expected[entry.Target]; !ok {
			return fmt.Errorf("Genesis export contains an unexpected target")
		}
		if _, duplicate := seen[entry.Target]; duplicate {
			return fmt.Errorf("Genesis export repeats a target")
		}
		seen[entry.Target] = struct{}{}
	}
	return nil
}
