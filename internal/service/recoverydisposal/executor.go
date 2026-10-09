package recoverydisposal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

const (
	executionCallTimeout           = 10 * time.Second
	executionMaxObservedBytes      = 64 * 1024 * 1024
	executionMaxTargetArchiveBytes = 64 * 1024 * 1024
	executionObservedStage         = "exact-targets-absent-observed-not-closed"
)

var (
	ErrExecutionUnknown = errors.New("exact disposal observation is unknown")
	ErrExecutionBudget  = errors.New("exact disposal execution budget exceeded")
)

// ExecutionAPI must be uncached and independently connected to the API
// identity pinned in the plan/grant, with bounded transport responses and no
// automatic mutation retries. A kube-system UID alone cannot authenticate an
// API endpoint. The closed GVK allowlist below is never dynamically extended.
// This interface deliberately has no Create, List, Update, status, namespace
// finalization, Exec, route, ledger-charge, or successor operation.
type ExecutionAPI interface {
	Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error
	Delete(context.Context, client.Object, ...client.DeleteOption) error
	Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error
}

// ExecutionObservation contains identities and API observations, never archive
// bodies. An absent target is not evidence of physical retirement or capacity.
type ExecutionObservation struct {
	Resource    ExecutionResource `json:"resource"`
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	UID         string            `json:"uid"`
	Observation string            `json:"observation"`
}

type ExecutionReport struct {
	Stage       string `json:"stage"`
	PlanDigest  string `json:"planDigest"`
	GrantDigest string `json:"grantDigest"`
	// APICalls counts interface invocations, not wire requests. Transport byte
	// ceilings and disabled mutation retries are caller obligations.
	APICalls         int                    `json:"apiCalls"`
	MutationAttempts int                    `json:"mutationAttempts"`
	ObservedBytes    int64                  `json:"observedBytes"`
	Observations     []ExecutionObservation `json:"observations"`
}

// Execute is a finite, opt-in exact-object executor, not a recovery controller.
// Independently reviewed external retirement/archival/no-reuse assertions and
// normal deletion of both original namespaces must already be established.
// Archives must contain one canonical, exact target body in plan order; the
// complete external archive also contains non-target records and results.
// All archive and current-object qualification is preflighted before any write.
// Each operation uses a fresh UID/RV, guarded by all three namespace identities;
// namespace-name reuse between those reads and a write is still an external
// fencing obligation, not a cross-object atomic guarantee provided by Kova.
// Any error or lost response stops immediately. There is no polling or retry,
// and a later invocation starts with fresh direct reads, never a remembered CAS
// token. Conditional Delete and finalizer-only Patch are monotonic exact-UID
// operations, not permission to replay a Create or adopt a replacement UID.
// Success is an API observation only: foreign finalizers may remain and neither
// namespace closure, physical retirement nor new admission capacity is certified.
func Execute(ctx context.Context, api ExecutionAPI, plan VerifiedExecutionPlan, grant VerifiedMutationGrant, archives []json.RawMessage) (ExecutionReport, error) {
	return execute(ctx, api, plan, grant, archives, time.Now)
}

type exactExecutor struct {
	ctx    context.Context
	api    ExecutionAPI
	plan   ExecutionPlanPayload
	grant  VerifiedMutationGrant
	now    func() time.Time
	refs   []qualifiedArchive
	report ExecutionReport
}

type qualifiedArchive struct {
	projection    []byte
	generation    int64
	hasGeneration bool
	deleting      bool
}

func execute(ctx context.Context, api ExecutionAPI, plan VerifiedExecutionPlan, grant VerifiedMutationGrant, archives []json.RawMessage, now func() time.Time) (ExecutionReport, error) {
	r := ExecutionReport{Stage: "not-started", PlanDigest: plan.planDigest, GrantDigest: grant.envelopeDigest,
		Observations: []ExecutionObservation{}}
	if ctx == nil || api == nil || !plan.valid || !grant.valid || !validMutationGrant(grant.payload, plan) ||
		!grant.current(now()) || ctx.Err() != nil || len(archives) != len(plan.payload.Targets) {
		return r, ErrUnqualified
	}
	// Keep an overall timer as well as per-call checks. Once started, a wall
	// clock rollback must not repeatedly rebuild and extend the grant window.
	executionCtx, cancel := context.WithDeadline(ctx, grant.expiresAt)
	defer cancel()
	_, refs, err := preflightArchives(executionCtx, plan, archives, func() bool { return grant.current(now()) })
	if err != nil {
		return r, err
	}
	if !grant.current(now()) {
		return r, ErrUnqualified
	}
	x := exactExecutor{ctx: executionCtx, api: api, plan: plan.Payload(), grant: grant, now: now, report: r, refs: refs}
	x.report.Stage = "unknown"
	fail := func(err error) (ExecutionReport, error) { return x.report, err }
	if err := x.guard(); err != nil {
		return fail(err)
	}
	for i := range x.plan.Targets {
		if _, _, err := x.target(i); err != nil {
			return fail(err)
		}
	}
	if err := x.guard(); err != nil {
		return fail(err)
	}
	for i := range x.plan.Targets {
		observation, err := x.dispose(i)
		if err != nil {
			return fail(err)
		}
		t := x.plan.Targets[i]
		x.report.Observations = append(x.report.Observations, ExecutionObservation{
			Resource: t.Resource, Namespace: t.Namespace, Name: t.Name, UID: t.UID, Observation: observation})
	}
	if err := x.guard(); err != nil {
		return fail(err)
	}
	x.report.Stage = executionObservedStage
	for _, observation := range x.report.Observations {
		if observation.Observation != "absent-at-direct-read" {
			x.report.Stage = "exact-disposal-pending"
			break
		}
	}
	return x.report, nil
}

func (x *exactExecutor) call(op func(context.Context) error) error {
	now := x.now()
	if x.ctx.Err() != nil || !x.grant.current(now) {
		return ErrExecutionUnknown
	}
	if x.report.APICalls >= x.plan.Limits.MaxCalls {
		return ErrExecutionBudget
	}
	c, cancel := context.WithDeadline(x.ctx, minTime(now.Add(executionCallTimeout), x.grant.expiresAt))
	defer cancel()
	if c.Err() != nil {
		return ErrExecutionUnknown
	}
	x.report.APICalls++
	err := op(c)
	if c.Err() != nil || !x.grant.current(x.now()) {
		return ErrExecutionUnknown
	}
	// Do not wrap API errors: they can echo object bodies or sensitive data.
	return err
}

func (x *exactExecutor) account(obj *unstructured.Unstructured) error {
	raw, err := json.Marshal(obj.Object)
	if err != nil || int64(len(raw)) > x.plan.Limits.MaxObjectArchiveBytes ||
		x.report.ObservedBytes+int64(len(raw)) > executionMaxObservedBytes {
		return ErrExecutionBudget
	}
	x.report.ObservedBytes += int64(len(raw))
	return nil
}

func (x *exactExecutor) guard() error {
	for _, pin := range []struct{ name, uid string }{
		{x.plan.Cluster.SystemNamespace, x.plan.Cluster.SystemNamespaceUID},
		{x.plan.Source.Namespace, x.plan.Source.NamespaceUID},
		{x.plan.Source.ReceiptNamespace, x.plan.Source.ReceiptNamespaceUID},
	} {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
		if err := x.call(func(c context.Context) error { return x.api.Get(c, client.ObjectKey{Name: pin.name}, obj) }); err != nil {
			return ErrExecutionUnknown
		}
		if err := x.account(obj); err != nil {
			return err
		}
		phase, _, err := unstructured.NestedString(obj.Object, "status", "phase")
		deleting, deletionErr := executionDeleting(obj)
		old := pin.name != x.plan.Cluster.SystemNamespace
		if err != nil || deletionErr != nil || obj.GetAPIVersion() != "v1" || obj.GetKind() != "Namespace" ||
			obj.GetName() != pin.name || obj.GetNamespace() != "" || string(obj.GetUID()) != pin.uid ||
			!safeID(obj.GetResourceVersion(), maxIDBytes) || (old && (!deleting || phase != "Terminating")) ||
			(!old && (deleting || phase != "Active")) {
			return ErrExecutionUnknown
		}
	}
	return nil
}

func (x *exactExecutor) target(i int) (*unstructured.Unstructured, bool, error) {
	t := x.plan.Targets[i]
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(executionGVK(t.Resource))
	err := x.call(func(c context.Context) error {
		return x.api.Get(c, client.ObjectKey{Namespace: t.Namespace, Name: t.Name}, obj)
	})
	if apierrors.IsNotFound(err) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, ErrExecutionUnknown
	}
	if err := x.account(obj); err != nil {
		return nil, false, err
	}
	projection, err := executionObservedProjection(t, obj, x.refs[i])
	if err != nil || !bytes.Equal(projection, x.refs[i].projection) {
		return nil, false, ErrExecutionUnknown
	}
	return obj, false, nil
}

func (x *exactExecutor) dispose(i int) (string, error) {
	if err := x.guard(); err != nil {
		return "", err
	}
	obj, absent, err := x.target(i)
	if err != nil {
		return "", err
	}
	if absent {
		return "absent-at-direct-read", x.guard()
	}
	deleting, _ := executionDeleting(obj) // target qualification already checked it
	if !deleting {
		if err := x.guard(); err != nil {
			return "", err
		}
		uid, rv := obj.GetUID(), obj.GetResourceVersion()
		propagation := metav1.DeletePropagationBackground
		err := x.call(func(c context.Context) error {
			x.report.MutationAttempts++
			return x.api.Delete(c, obj, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
				PropagationPolicy: &propagation})
		})
		if err != nil {
			return "", ErrExecutionUnknown
		}
		if err := x.guard(); err != nil {
			return "", err
		}
		obj, absent, err = x.target(i)
		if err != nil {
			return "", err
		}
		if absent {
			return "absent-at-direct-read", x.guard()
		}
		deleting, _ = executionDeleting(obj)
		if !deleting {
			return "", ErrExecutionUnknown
		}
	}
	before, err := executionFinalizers(obj)
	if err != nil {
		return "", ErrExecutionUnknown
	}
	after := make([]string, 0, len(before))
	for _, f := range before {
		if !slices.Contains(x.plan.Targets[i].AllowedKovaFinalizers, f) {
			after = append(after, f)
		}
	}
	if len(after) != len(before) {
		if err := x.guard(); err != nil {
			return "", err
		}
		patch, _ := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": string(obj.GetUID())},
			{"op": "test", "path": "/metadata/resourceVersion", "value": obj.GetResourceVersion()},
			{"op": "test", "path": "/metadata/finalizers", "value": before},
			{"op": "replace", "path": "/metadata/finalizers", "value": after},
		})
		err := x.call(func(c context.Context) error {
			x.report.MutationAttempts++
			return x.api.Patch(c, obj, client.RawPatch(types.JSONPatchType, patch))
		})
		if err != nil {
			return "", ErrExecutionUnknown
		}
		// controller-runtime decodes the full Patch response back into obj.
		// Qualify that response too, not only the subsequent direct GET.
		if err := x.account(obj); err != nil {
			return "", err
		}
		projection, qualificationErr := executionObservedProjection(x.plan.Targets[i], obj, x.refs[i])
		remaining, finalizerErr := executionFinalizers(obj)
		deleting, _ = executionDeleting(obj)
		if qualificationErr != nil || finalizerErr != nil || !bytes.Equal(projection, x.refs[i].projection) ||
			!deleting || !slices.Equal(remaining, after) {
			return "", ErrExecutionUnknown
		}
		if err := x.guard(); err != nil {
			return "", err
		}
		obj, absent, err = x.target(i)
		if err != nil {
			return "", err
		}
		if absent {
			return "absent-at-direct-read", x.guard()
		}
		remaining, err = executionFinalizers(obj)
		deleting, _ = executionDeleting(obj)
		if err != nil || !deleting || !slices.Equal(remaining, after) {
			return "", ErrExecutionUnknown
		}
	}
	if err := x.guard(); err != nil {
		return "", err
	}
	if len(after) != 0 {
		return "deleting-foreign-finalizers-preserved", nil
	}
	return "deleting-no-authorized-kova-finalizers-observed", nil
}

// DigestExecutionQualification defines the executor's deterministic archived
// projection qualification. It binds exact identity, role, disposition and the
// narrower finalizer permission; it does not establish archive durability or
// independently validate a role's external evidence. The signed execution plan
// still supplies the closed role/resource/disposition qualification. Legacy
// opaque qualification hashes cannot be substituted for this computed record.
func DigestExecutionQualification(target ExecutionTarget, archived json.RawMessage) (string, error) {
	_, digest, err := qualifyArchive(target, archived)
	return digest, err
}

func qualifyArchive(t ExecutionTarget, raw []byte) (qualifiedArchive, string, error) {
	if len(raw) == 0 || len(raw) > maxExecutionObjectBytes || int64(len(raw)) != t.ArchiveBytes ||
		digestBytes(raw) != t.ArchiveDigest || t.QualificationVersion != ExecutionQualificationVersion {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	obj := &unstructured.Unstructured{}
	strict, err := kjson.UnmarshalStrict(raw, &obj.Object, kjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	canonical, err := json.Marshal(obj.Object)
	if err != nil || !bytes.Equal(raw, canonical) || obj.GetResourceVersion() != t.ArchiveResourceVersion {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	projection, err := executionProjection(t, obj)
	if err != nil {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	generation, hasGeneration, err := unstructured.NestedInt64(obj.Object, "metadata", "generation")
	deleting, deletionErr := executionDeleting(obj)
	if err != nil || deletionErr != nil || generation < 0 {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	record := struct {
		Version           string            `json:"version"`
		Resource          ExecutionResource `json:"resource"`
		Namespace         string            `json:"namespace"`
		NamespaceUID      string            `json:"namespaceUid"`
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		Role              string            `json:"role"`
		DispositionID     string            `json:"dispositionId"`
		ArchiveDigest     string            `json:"archiveDigest"`
		ProjectionDigest  string            `json:"projectionDigest"`
		AllowedFinalizers []string          `json:"allowedFinalizers"`
	}{t.QualificationVersion, t.Resource, t.Namespace, t.NamespaceUID, t.Name, t.UID, t.Role,
		t.DispositionID, t.ArchiveDigest, digestBytes(projection), t.AllowedKovaFinalizers}
	encoded, err := json.Marshal(record)
	if err != nil {
		return qualifiedArchive{}, "", ErrUnqualified
	}
	return qualifiedArchive{projection: projection, generation: generation, hasGeneration: hasGeneration, deleting: deleting}, digestBytes(encoded), nil
}

// Kubernetes increments generation once when a positive-generation object
// first enters deletion (generic Store.markAsDeleting / rest.BeforeDelete).
// Normalize only that exact transition, never spec/status changes, a second
// increment, undeleting, or drift before deletion. All other projection fields
// still have to equal the qualified archive.
func executionObservedProjection(t ExecutionTarget, obj *unstructured.Unstructured, archive qualifiedArchive) ([]byte, error) {
	deleting, err := executionDeleting(obj)
	if err != nil || archive.deleting && !deleting {
		return nil, ErrUnqualified
	}
	projection, err := executionProjection(t, obj)
	if err != nil || bytes.Equal(projection, archive.projection) {
		return projection, err
	}
	generation, present, err := unstructured.NestedInt64(obj.Object, "metadata", "generation")
	if err != nil || archive.deleting || !deleting || !archive.hasGeneration || !present ||
		archive.generation <= 0 || archive.generation == 9223372036854775807 || generation != archive.generation+1 {
		return nil, ErrUnqualified
	}
	copy := obj.DeepCopy()
	copy.SetGeneration(archive.generation)
	return executionProjection(t, copy)
}

func executionProjection(t ExecutionTarget, obj *unstructured.Unstructured) ([]byte, error) {
	gvk := executionGVK(t.Resource)
	if gvk.Empty() || obj.GroupVersionKind() != gvk || obj.GetNamespace() != t.Namespace || obj.GetName() != t.Name ||
		string(obj.GetUID()) != t.UID || !safeID(obj.GetResourceVersion(), maxIDBytes) {
		return nil, ErrUnqualified
	}
	deleting, err := executionDeleting(obj)
	if err != nil {
		return nil, err
	}
	finalizers, err := executionFinalizers(obj)
	if err != nil {
		return nil, err
	}
	// Background propagation itself removes these Kubernetes finalizers.
	// Refuse every non-deleting archive/current target before any mutation;
	// the executor must not promise foreign-finalizer preservation then erase
	// one as an unintended side effect of Delete.
	if !deleting && (slices.Contains(finalizers, metav1.FinalizerOrphanDependents) || slices.Contains(finalizers, metav1.FinalizerDeleteDependents)) {
		return nil, ErrUnqualified
	}
	copy := obj.DeepCopy()
	for _, field := range []string{"resourceVersion", "managedFields", "deletionTimestamp", "deletionGracePeriodSeconds"} {
		unstructured.RemoveNestedField(copy.Object, "metadata", field)
	}
	foreign := make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if !slices.Contains(t.AllowedKovaFinalizers, f) {
			foreign = append(foreign, f)
		}
	}
	if len(foreign) == 0 {
		unstructured.RemoveNestedField(copy.Object, "metadata", "finalizers")
	} else if err := unstructured.SetNestedStringSlice(copy.Object, foreign, "metadata", "finalizers"); err != nil {
		return nil, ErrUnqualified
	}
	raw, err := json.Marshal(copy.Object)
	if err != nil {
		return nil, ErrUnqualified
	}
	return raw, nil
}

func executionFinalizers(obj *unstructured.Unstructured) ([]string, error) {
	finalizers, _, err := unstructured.NestedStringSlice(obj.Object, "metadata", "finalizers")
	if err != nil {
		return nil, ErrUnqualified
	}
	seen := map[string]bool{}
	for _, f := range finalizers {
		if !safeID(f, maxIDBytes) || seen[f] {
			return nil, ErrUnqualified
		}
		seen[f] = true
	}
	return finalizers, nil
}

func executionDeleting(obj *unstructured.Unstructured) (bool, error) {
	value, exists, err := unstructured.NestedString(obj.Object, "metadata", "deletionTimestamp")
	if err != nil {
		return false, ErrUnqualified
	}
	if !exists {
		return false, nil
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || timestamp.IsZero() {
		return false, ErrUnqualified
	}
	return true, nil
}

func executionGVK(r ExecutionResource) schema.GroupVersionKind {
	switch r {
	case ExecutionResource{Version: "v1", Resource: "configmaps"}:
		return schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	case ExecutionResource{Version: "v1", Resource: "pods"}:
		return schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	case ExecutionResource{Group: kovav1.Group, Version: kovav1.Version, Resource: "kovabuilds"}:
		return schema.GroupVersionKind{Group: kovav1.Group, Version: kovav1.Version, Kind: "KovaBuild"}
	default:
		return schema.GroupVersionKind{}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Compile-time interface compatibility only; Execute never creates a client.
var _ ExecutionAPI = (client.Client)(nil)
