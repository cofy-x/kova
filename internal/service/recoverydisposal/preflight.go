package recoverydisposal

import (
	"context"
	"encoding/json"
)

// ArchivePreflightReport is a pure, bounded qualification report. It certifies
// exact archived target bytes against an already verified plan, not complete
// inventory, durable external storage, physical retirement, current API state,
// mutation permission, incident closure, or capacity release.
type ArchivePreflightReport struct {
	Stage            string `json:"stage"`
	PlanDigest       string `json:"planDigest"`
	PlanEnvelope     string `json:"planEnvelopeDigest"`
	TargetCount      int    `json:"targetCount"`
	TargetBytes      int64  `json:"targetBytes"`
	RequiredAPICalls int    `json:"reservedExecutionApiCalls"`
}

// PreflightArchives qualifies the full ordered actionable target archive with
// the same implementation used by Execute. No API client or mutation grant is
// accepted, and there are no reads, writes, signers, or external URI fetches.
// Bodies may contain sensitive evidence; neither reports nor errors echo them.
// The caller must independently establish the full non-target/result archive.
func PreflightArchives(ctx context.Context, plan VerifiedExecutionPlan, archives []json.RawMessage) (ArchivePreflightReport, error) {
	report, _, err := preflightArchives(ctx, plan, archives, nil)
	return report, err
}

func preflightArchives(ctx context.Context, plan VerifiedExecutionPlan, archives []json.RawMessage, current func() bool) (ArchivePreflightReport, []qualifiedArchive, error) {
	qualified := func() bool { return ctx != nil && ctx.Err() == nil && (current == nil || current()) }
	if !plan.valid || !qualified() || len(archives) != len(plan.payload.Targets) {
		return ArchivePreflightReport{}, nil, ErrUnqualified
	}
	p := plan.payload
	// The worst-case budget applies to the whole plan, never truncated batches.
	reserved := 9 + 24*len(p.Targets)
	if p.Limits.MaxCalls < reserved {
		return ArchivePreflightReport{}, nil, ErrExecutionBudget
	}
	var total int64
	for _, raw := range archives {
		if !qualified() || int64(len(raw)) > p.Limits.MaxObjectArchiveBytes {
			return ArchivePreflightReport{}, nil, ErrUnqualified
		}
		total += int64(len(raw))
		if total > executionMaxTargetArchiveBytes {
			return ArchivePreflightReport{}, nil, ErrExecutionBudget
		}
		if total > p.Limits.MaxArchiveBytes || total > p.Evidence.ArchiveBytes {
			return ArchivePreflightReport{}, nil, ErrUnqualified
		}
	}
	refs := make([]qualifiedArchive, len(archives))
	for i, raw := range archives {
		if !qualified() {
			return ArchivePreflightReport{}, nil, ErrUnqualified
		}
		ref, digest, err := qualifyArchive(p.Targets[i], raw)
		if err != nil || digest != p.Targets[i].QualificationDigest || !qualified() {
			return ArchivePreflightReport{}, nil, ErrUnqualified
		}
		refs[i] = ref
	}
	return ArchivePreflightReport{Stage: "exact-target-archives-qualified-not-authorized", PlanDigest: plan.planDigest,
		PlanEnvelope: plan.envelopeDigest, TargetCount: len(archives), TargetBytes: total, RequiredAPICalls: reserved}, refs, nil
}
