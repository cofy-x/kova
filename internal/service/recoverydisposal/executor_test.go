package recoverydisposal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

type executorFixture struct {
	plan     VerifiedExecutionPlan
	grant    VerifiedMutationGrant
	archives []json.RawMessage
	api      *executorAPI
}

type executorAPI struct {
	t             *testing.T
	objects       map[string]*unstructured.Unstructured
	calls         []string
	writes        []string
	patches       [][]byte
	before        func(op, key string, api *executorAPI) error
	after         func(op, key string, api *executorAPI) error
	patchResponse func(obj *unstructured.Unstructured)
	maxDeadline   time.Time
}

func executorObjectKey(gvk schema.GroupVersionKind, namespace, name string) string {
	return strings.Join([]string{gvk.Group, gvk.Version, gvk.Kind, namespace, name}, "/")
}

func executionNamespace(name, uid string, old bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": name, "uid": uid, "resourceVersion": "500"},
		"status":   map[string]any{"phase": "Active"},
	}}
	if old {
		obj.Object["status"] = map[string]any{"phase": "Terminating"}
		obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now().Add(-time.Minute).UTC()})
	}
	return obj
}

func newExecutorFixture(t *testing.T) executorFixture {
	t.Helper()
	f := newExecutionFixture(t)
	selected := []ExecutionTarget{}
	for _, target := range f.payload.Targets {
		if (target.Role == "original-build" && target.DispositionID == "admin") || target.Role == "original-pod" || target.Role == "genesis" {
			selected = append(selected, target)
		}
	}
	f.payload.Targets = selected
	f.payload.Limits.MaxCalls = 256
	api := &executorAPI{t: t, objects: map[string]*unstructured.Unstructured{}}
	for _, obj := range []*unstructured.Unstructured{
		executionNamespace("kube-system", f.payload.Cluster.SystemNamespaceUID, false),
		executionNamespace(f.payload.Source.Namespace, f.payload.Source.NamespaceUID, true),
		executionNamespace(f.payload.Source.ReceiptNamespace, f.payload.Source.ReceiptNamespaceUID, true),
	} {
		api.objects[executorObjectKey(obj.GroupVersionKind(), "", obj.GetName())] = obj
	}
	archives := make([]json.RawMessage, len(selected))
	var total int64
	for i := range f.payload.Targets {
		target := &f.payload.Targets[i]
		gvk := executionGVK(target.Resource)
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(), "kind": gvk.Kind,
			"metadata": map[string]any{"namespace": target.Namespace, "name": target.Name, "uid": target.UID,
				"resourceVersion": target.ArchiveResourceVersion, "labels": map[string]any{"fixed": "yes"}},
		}}
		obj.SetFinalizers(slices.Clone(target.AllowedKovaFinalizers))
		switch target.Resource.Resource {
		case "configmaps":
			obj.Object["immutable"] = true
			obj.Object["data"] = map[string]any{"evidence": "do-not-log-body"}
		case "kovabuilds":
			obj.SetGeneration(1)
			obj.SetFinalizers(append(obj.GetFinalizers(), "example.com/foreign"))
			obj.Object["spec"] = map[string]any{"executionMode": "archived"}
			obj.Object["status"] = map[string]any{"phase": "Failed", "qualified": true}
		case "pods":
			obj.SetGeneration(1)
			obj.Object["spec"] = map[string]any{"containers": []any{map[string]any{"name": "runner", "image": "registry.test/runner@" + hash('a')}}}
			obj.Object["status"] = map[string]any{"phase": "Failed"}
		}
		raw, err := json.Marshal(obj.Object)
		if err != nil {
			t.Fatal(err)
		}
		target.ArchiveDigest, target.ArchiveBytes = digestBytes(raw), int64(len(raw))
		target.QualificationDigest, err = DigestExecutionQualification(*target, raw)
		if err != nil {
			t.Fatal(err)
		}
		archives[i] = raw
		total += int64(len(raw))
		current := obj.DeepCopy()
		current.SetResourceVersion("600") // fresh RV must not reuse archive's token
		api.objects[executorObjectKey(gvk, target.Namespace, target.Name)] = current
	}
	f.payload.Evidence.ArchiveBytes = total + 2048
	planRaw := f.raw(t, executionPlanDomain)
	plan, err := VerifyExecutionPlan(planRaw, executionExpectation(t, f.payload, planRaw), f.roots)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	p := MutationGrantPayload{
		Version: MutationGrantVersion, Audience: MutationGrantAudience, Action: MutationGrantAction,
		Issuer: f.payload.Issuer, KeyID: f.payload.KeyID, IncidentID: f.payload.IncidentID,
		Cluster: f.payload.Cluster, Source: f.payload.Source, PlanDigest: plan.PlanDigest(), PlanEnvelopeDigest: plan.EnvelopeDigest(),
		ReviewDigest: hash('f'), NotBefore: now.Add(-time.Minute).Format(time.RFC3339Nano), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(MutationGrantEnvelope{Payload: p, Signature: sign(t, p, mutationGrantDomain, f.private)})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := VerifyMutationGrant(raw, MutationGrantExpectation{EnvelopeDigest: digestBytes(raw), Payload: p}, f.roots, plan, now)
	if err != nil {
		t.Fatal(err)
	}
	api.maxDeadline = grant.expiresAt
	return executorFixture{plan, grant, archives, api}
}

func (a *executorAPI) call(ctx context.Context, op string, obj client.Object) (string, error) {
	a.t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(time.Now().Add(executionCallTimeout+time.Second)) || deadline.After(a.maxDeadline) {
		a.t.Fatal("every API call must have a bounded grant-limited context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	key := executorObjectKey(obj.GetObjectKind().GroupVersionKind(), obj.GetNamespace(), obj.GetName())
	a.calls = append(a.calls, op+":"+key)
	if a.before != nil {
		if err := a.before(op, key, a); err != nil {
			return key, err
		}
	}
	return key, nil
}

func (a *executorAPI) Get(ctx context.Context, key client.ObjectKey, out client.Object, _ ...client.GetOption) error {
	out.SetNamespace(key.Namespace)
	out.SetName(key.Name)
	identity, err := a.call(ctx, "GET", out)
	if err != nil {
		return err
	}
	obj := a.objects[identity]
	if obj == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: out.GetObjectKind().GroupVersionKind().Kind}, key.Name)
	}
	out.(*unstructured.Unstructured).Object = obj.DeepCopy().Object
	if a.after != nil {
		return a.after("GET", identity, a)
	}
	return nil
}

func (a *executorAPI) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	key, err := a.call(ctx, "DELETE", obj)
	if err != nil {
		return err
	}
	if obj.GetObjectKind().GroupVersionKind().Kind == "Namespace" {
		a.t.Fatal("namespace mutation forbidden")
	}
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	current := a.objects[key]
	if current == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, obj.GetName())
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil ||
		string(*options.Preconditions.UID) != string(current.GetUID()) || *options.Preconditions.ResourceVersion != current.GetResourceVersion() {
		return apierrors.NewConflict(schema.GroupResource{Resource: "objects"}, obj.GetName(), errors.New("UID/RV condition failed"))
	}
	if *options.Preconditions.ResourceVersion == "123" {
		a.t.Fatal("archive RV reused")
	}
	if options.GracePeriodSeconds != nil || options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationBackground {
		a.t.Fatal("unexpected Delete semantics")
	}
	a.writes = append(a.writes, "DELETE:"+key)
	if len(current.GetFinalizers()) == 0 {
		delete(a.objects, key)
	} else {
		if current.GetGeneration() > 0 {
			current.SetGeneration(current.GetGeneration() + 1)
		}
		current.SetDeletionTimestamp(&metav1.Time{Time: time.Now().UTC()})
		current.SetResourceVersion(nextExecutionRV(current.GetResourceVersion()))
	}
	if a.after != nil {
		return a.after("DELETE", key, a)
	}
	return nil
}

func (a *executorAPI) Patch(ctx context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	key, err := a.call(ctx, "PATCH", obj)
	if err != nil {
		return err
	}
	if obj.GetObjectKind().GroupVersionKind().Kind == "Namespace" || patch.Type() != types.JSONPatchType {
		a.t.Fatal("only object JSONPatch allowed")
	}
	raw, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var ops []struct {
		Op, Path string
		Value    any
	}
	if err := json.Unmarshal(raw, &ops); err != nil {
		return err
	}
	if len(ops) != 4 || ops[0].Op != "test" || ops[0].Path != "/metadata/uid" || ops[1].Op != "test" || ops[1].Path != "/metadata/resourceVersion" ||
		ops[2].Op != "test" || ops[2].Path != "/metadata/finalizers" || ops[3].Op != "replace" || ops[3].Path != "/metadata/finalizers" {
		a.t.Fatal("patch must test UID/RV/exact old finalizers and replace only finalizers")
	}
	current := a.objects[key]
	if current == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, obj.GetName())
	}
	encoded, _ := json.Marshal(current.Object)
	p, err := jsonpatch.DecodePatch(raw)
	if err != nil {
		return err
	}
	updated, err := p.Apply(encoded)
	if err != nil {
		return apierrors.NewConflict(schema.GroupResource{Resource: "objects"}, obj.GetName(), errors.New("JSONPatch test failed"))
	}
	result := &unstructured.Unstructured{}
	if err := kjson.UnmarshalCaseSensitivePreserveInts(updated, &result.Object); err != nil {
		return err
	}
	result.SetResourceVersion(nextExecutionRV(current.GetResourceVersion()))
	a.writes = append(a.writes, "PATCH:"+key)
	a.patches = append(a.patches, bytes.Clone(raw))
	obj.(*unstructured.Unstructured).Object = result.DeepCopy().Object
	if a.patchResponse != nil {
		a.patchResponse(obj.(*unstructured.Unstructured))
	}
	if len(result.GetFinalizers()) == 0 {
		delete(a.objects, key)
	} else {
		a.objects[key] = result
	}
	if a.after != nil {
		return a.after("PATCH", key, a)
	}
	return nil
}

func nextExecutionRV(rv string) string {
	value, _ := strconv.Atoi(rv)
	return strconv.Itoa(value + 1)
}

func executorTargetKey(f executorFixture, role string) string {
	for _, target := range f.plan.payload.Targets {
		if target.Role == role {
			return executorObjectKey(executionGVK(target.Resource), target.Namespace, target.Name)
		}
	}
	panic("missing fixture target")
}

func TestExecuteConditionalDisposalPreservesForeignFinalizers(t *testing.T) {
	f := newExecutorFixture(t)
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if err != nil || r.Stage != "exact-disposal-pending" || len(r.Observations) != 3 || r.APICalls != len(f.api.calls) || r.MutationAttempts != 5 {
		t.Fatalf("finite disposal report: %#v err=%v", r, err)
	}
	build := f.api.objects[executorTargetKey(f, "original-build")]
	if build == nil || !slices.Equal(build.GetFinalizers(), []string{"example.com/foreign"}) || build.GetDeletionTimestamp() == nil {
		t.Fatal("foreign finalizer must remain while target deletion is pending")
	}
	if f.api.objects[executorTargetKey(f, "original-pod")] != nil || f.api.objects[executorTargetKey(f, "genesis")] != nil {
		t.Fatal("all deletable exact targets should be absent at direct readback")
	}
	encoded, _ := json.Marshal(r)
	if strings.Contains(string(encoded), "do-not-log-body") {
		t.Fatal("report leaked archive body")
	}
	beforeWrites := len(f.api.writes)
	r, err = Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if err != nil || r.Stage != "exact-disposal-pending" || len(f.api.writes) != beforeWrites {
		t.Fatal("resume must use fresh observations without repeating settled writes")
	}
}

func TestExecuteAllAbsentIsObservationNotClosure(t *testing.T) {
	f := newExecutorFixture(t)
	for _, target := range f.plan.payload.Targets {
		delete(f.api.objects, executorObjectKey(executionGVK(target.Resource), target.Namespace, target.Name))
	}
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if err != nil || r.Stage != executionObservedStage || len(f.api.writes) != 0 || len(r.Observations) != 3 {
		t.Fatalf("absence-only observation: %#v %v", r, err)
	}
}

func TestExecuteRejectsAllUnqualifiedInputsBeforeAnyWrite(t *testing.T) {
	cases := map[string]func(*executorFixture){
		"zero plan":                     func(f *executorFixture) { f.plan = VerifiedExecutionPlan{} },
		"zero grant":                    func(f *executorFixture) { f.grant = VerifiedMutationGrant{} },
		"expired":                       func(f *executorFixture) { f.grant.expiresAt = time.Now().Add(-time.Second) },
		"wrong plan grant":              func(f *executorFixture) { f.grant.payload.PlanDigest = hash('9') },
		"missing archive":               func(f *executorFixture) { f.archives = f.archives[:2] },
		"changed archive":               func(f *executorFixture) { f.archives[2] = append(bytes.Clone(f.archives[2]), ' ') },
		"bad qualification":             func(f *executorFixture) { f.plan.payload.Targets[2].QualificationDigest = hash('9') },
		"insufficient worst-case calls": func(f *executorFixture) { f.plan.payload.Limits.MaxCalls = 9 + 24*len(f.archives) - 1 },
		"object bytes":                  func(f *executorFixture) { f.plan.payload.Limits.MaxObjectArchiveBytes = 1 },
		"total archive bytes":           func(f *executorFixture) { f.plan.payload.Limits.MaxArchiveBytes = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newExecutorFixture(t)
			mutate(&f)
			r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if err == nil || r.Stage != "not-started" || len(f.api.calls) != 0 || len(f.api.writes) != 0 {
				t.Fatalf("unqualified input reached API: %#v %v", r, err)
			}
		})
	}
}

func TestExecuteWholeCurrentInventoryPreflightBeforeAnyWrite(t *testing.T) {
	cases := map[string]func(*executorFixture){
		"namespace Active": func(f *executorFixture) {
			obj := f.api.objects[executorObjectKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", f.plan.payload.Source.Namespace)]
			obj.Object["status"] = map[string]any{"phase": "Active"}
		},
		"namespace replacement": func(f *executorFixture) {
			f.api.objects[executorObjectKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", f.plan.payload.Source.ReceiptNamespace)].SetUID("replacement-ns")
		},
		"system replacement": func(f *executorFixture) {
			f.api.objects[executorObjectKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", "kube-system")].SetUID("replacement-system")
		},
		"last object UID": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "original-build")].SetUID("replacement-object")
		},
		"last object spec": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "original-build")].Object["spec"] = map[string]any{"executionMode": "changed"}
		},
		"status": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "original-pod")].Object["status"] = map[string]any{"phase": "Succeeded"}
		},
		"foreign finalizers": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "original-build")].SetFinalizers([]string{"example.com/changed"})
		},
		"annotation": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "original-pod")].SetAnnotations(map[string]string{"changed": "yes"})
		},
		"wrong kind":      func(f *executorFixture) { f.api.objects[executorTargetKey(*f, "genesis")].SetKind("Secret") },
		"wrong namespace": func(f *executorFixture) { f.api.objects[executorTargetKey(*f, "genesis")].SetNamespace("another") },
		"missing RV":      func(f *executorFixture) { f.api.objects[executorTargetKey(*f, "genesis")].SetResourceVersion("") },
		"oversized current body": func(f *executorFixture) {
			f.api.objects[executorTargetKey(*f, "genesis")].Object["data"] = map[string]any{"oversized": strings.Repeat("a", int(f.plan.payload.Limits.MaxObjectArchiveBytes))}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newExecutorFixture(t)
			mutate(&f)
			r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if err == nil || r.Stage != "unknown" || len(f.api.writes) != 0 {
				t.Fatalf("preflight allowed writes: %#v %v", r, err)
			}
		})
	}
}

func TestExecuteLostMutationResponsesStopAndResumeByFreshRead(t *testing.T) {
	for _, op := range []string{"DELETE", "PATCH"} {
		t.Run(op, func(t *testing.T) {
			f := newExecutorFixture(t)
			f.api.after = func(operation, key string, a *executorAPI) error {
				if operation == op {
					return errors.New("lost response containing secret-body")
				}
				return nil
			}
			r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" || strings.Contains(err.Error(), "secret-body") || len(f.api.writes) == 0 {
				t.Fatalf("lost response: %#v %v", r, err)
			}
			last := f.api.calls[len(f.api.calls)-1]
			if !strings.HasPrefix(last, op+":") {
				t.Fatal("executor continued after unknown mutation response")
			}
			writes := slices.Clone(f.api.writes)
			f.api.after = nil
			r, err = Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if err != nil || r.Stage != "exact-disposal-pending" {
				t.Fatalf("fresh qualified resume: %#v %v", r, err)
			}
			for _, settled := range writes {
				if slices.Contains(f.api.writes[len(writes):], settled) {
					t.Fatalf("settled write repeated after fresh observation: %s", settled)
				}
			}
		})
	}
}

func TestExecuteRejectsFreshCASConflictsAndReplacementUID(t *testing.T) {
	for _, op := range []string{"DELETE", "PATCH"} {
		t.Run(op, func(t *testing.T) {
			f := newExecutorFixture(t)
			f.api.before = func(operation, key string, a *executorAPI) error {
				if operation == op {
					a.objects[key].SetResourceVersion(nextExecutionRV(a.objects[key].GetResourceVersion()))
				}
				return nil
			}
			r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" {
				t.Fatalf("CAS conflict not stopped: %#v %v", r, err)
			}
			if !strings.HasPrefix(f.api.calls[len(f.api.calls)-1], op+":") {
				t.Fatal("retry after CAS conflict")
			}
		})
	}
	f := newExecutorFixture(t)
	f.api.after = func(op, key string, a *executorAPI) error {
		if op == "DELETE" && a.objects[key] != nil {
			a.objects[key].SetUID("late-replacement")
		}
		return nil
	}
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" || len(f.api.patches) != 0 {
		t.Fatal("replacement UID was adopted after Delete")
	}
}

func TestExecuteRechecksNamespaceGuardsAroundMutations(t *testing.T) {
	for _, op := range []string{"DELETE", "PATCH"} {
		t.Run(op, func(t *testing.T) {
			f := newExecutorFixture(t)
			f.api.after = func(operation, key string, a *executorAPI) error {
				if operation == op {
					a.objects[executorObjectKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", f.plan.payload.Source.Namespace)].SetUID("replacement-ns")
				}
				return nil
			}
			r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
			if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" {
				t.Fatal("namespace change must block later writes")
			}
			if op == "DELETE" && len(f.api.patches) != 0 {
				t.Fatal("Patch proceeded after namespace replacement")
			}
		})
	}
}

func TestExecuteDeadlineRecheckedAtEachStep(t *testing.T) {
	f := newExecutorFixture(t)
	now := time.Now()
	f.api.after = func(op, key string, a *executorAPI) error {
		if op == "DELETE" {
			now = f.grant.expiresAt
		}
		return nil
	}
	r, err := execute(context.Background(), f.api, f.plan, f.grant, f.archives, func() time.Time { return now })
	if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" || len(f.api.writes) != 1 {
		t.Fatalf("expired grant after write: %#v %v", r, err)
	}
	if !strings.HasPrefix(f.api.calls[len(f.api.calls)-1], "DELETE:") {
		t.Fatal("API call after grant expiry")
	}
}

func TestQualificationLocksBodyAndForeignFinalizersButNotServerMetadata(t *testing.T) {
	f := newExecutorFixture(t)
	for i, target := range f.plan.payload.Targets {
		obj := f.api.objects[executorObjectKey(executionGVK(target.Resource), target.Namespace, target.Name)].DeepCopy()
		obj.SetResourceVersion("900")
		obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
		obj.Object["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "namespace-controller"}}
		projection, err := executionProjection(target, obj)
		archived, _, archiveErr := qualifyArchive(target, f.archives[i])
		if err != nil || archiveErr != nil || !bytes.Equal(projection, archived.projection) {
			t.Fatalf("server metadata should not change body projection: %v %v", err, archiveErr)
		}
	}
	for _, raw := range [][]byte{[]byte(`{"metadata":{"name":"first","name":"last"}}`), []byte(`{"metadata":null}`), []byte(`[]`)} {
		target := f.plan.payload.Targets[0]
		target.ArchiveBytes = int64(len(raw))
		target.ArchiveDigest = digestBytes(raw)
		if _, err := DigestExecutionQualification(target, raw); err == nil {
			t.Fatal("malformed/noncanonical archive accepted")
		}
	}
}

func TestExecutorAPIFaultsAreFiniteAndNeverEchoPayload(t *testing.T) {
	f := newExecutorFixture(t)
	f.api.before = func(op, key string, a *executorAPI) error { return fmt.Errorf("api error with %s", "do-not-log-body") }
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if !errors.Is(err, ErrExecutionUnknown) || len(f.api.calls) != 1 || len(f.api.writes) != 0 || strings.Contains(err.Error(), "do-not-log-body") || r.Stage != "unknown" {
		t.Fatal("API error leaked or was retried")
	}
}

func TestExecuteRejectsPatchResponseBodyDriftBeforeLaterCalls(t *testing.T) {
	f := newExecutorFixture(t)
	f.api.patchResponse = func(obj *unstructured.Unstructured) {
		obj.Object["status"] = map[string]any{"phase": "tampered-response"}
	}
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" || !strings.HasPrefix(f.api.calls[len(f.api.calls)-1], "PATCH:") {
		t.Fatal("full Patch response body was not qualified before later calls")
	}
}

func TestExecuteCancelledOrExpiredArchivePreflightDoesNotReachAPI(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprint("expire=", expire), func(t *testing.T) {
			f := newExecutorFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := 0
			now := time.Now()
			r, err := execute(ctx, f.api, f.plan, f.grant, f.archives, func() time.Time {
				checks++
				if checks == 6 { // after qualification of the first archive body
					if expire {
						now = f.grant.expiresAt
					} else {
						cancel()
					}
				}
				return now
			})
			if !errors.Is(err, ErrUnqualified) || r.Stage != "not-started" || len(f.api.calls) != 0 || len(f.api.writes) != 0 {
				t.Fatalf("archive preflight cancellation reached API: %#v %v", r, err)
			}
		})
	}
}

func TestExecuteTargetArchivesHaveIndependentHardByteCap(t *testing.T) {
	f := newExecutorFixture(t)
	// Exercise size admission without allocating five large bodies, parsing
	// an unbounded archive or deriving a trusted plan from untrusted data.
	large := json.RawMessage(make([]byte, maxExecutionObjectBytes))
	f.archives = []json.RawMessage{large, large, large, large, large}
	for len(f.plan.payload.Targets) < len(f.archives) {
		f.plan.payload.Targets = append(f.plan.payload.Targets, f.plan.payload.Targets[0])
	}
	f.plan.payload.Limits.MaxObjectArchiveBytes = maxExecutionObjectBytes
	f.plan.payload.Limits.MaxArchiveBytes = maxExecutionArchiveBytes
	f.plan.payload.Evidence.ArchiveBytes = maxExecutionArchiveBytes
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if !errors.Is(err, ErrExecutionBudget) || r.Stage != "not-started" || len(f.api.calls) != 0 {
		t.Fatal("target projections could exceed the independent 64 MiB hard limit")
	}
}

func TestExecuteNoBlindRetryAfterSuccessfulButUnobservedDelete(t *testing.T) {
	f := newExecutorFixture(t)
	f.api.after = func(op, key string, api *executorAPI) error {
		if op == "DELETE" && api.objects[key] == nil {
			// A success response without the expected absence/deletion evidence
			// is unknown, even for an independently authorized original UID.
			for i, target := range f.plan.payload.Targets {
				if executorObjectKey(executionGVK(target.Resource), target.Namespace, target.Name) == key {
					obj := &unstructured.Unstructured{}
					_ = json.Unmarshal(f.archives[i], &obj.Object)
					obj.SetResourceVersion("901")
					api.objects[key] = obj
				}
			}
		}
		return nil
	}
	r, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
	if !errors.Is(err, ErrExecutionUnknown) || r.Stage != "unknown" || len(f.api.writes) != 1 {
		t.Fatal("Delete success without qualified deletion/absence was blindly retried")
	}
}
