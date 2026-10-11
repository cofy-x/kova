package recoveryoperator

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/recoverydrain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

const recoveryHold = "kova.cofy.dev/recovery-hold"
const tombstoneMarker = "kova.cofy.dev/recovery-tombstone"

type stageScope struct {
	namespaces         map[string]string
	reads              map[string]string // exact path -> expected UID, or empty for a receipt-qualified name
	targets            map[string]bool
	attempts           map[string]map[string]string
	lists              map[string]bool
	incident, epochUID string
}

func collection(g schema.GroupVersionKind, ns string) string {
	resource := map[string]string{"ConfigMap": "configmaps", "Pod": "pods", "KovaBuild": "kovabuilds"}[g.Kind]
	prefix := "/api/" + g.Version
	if g.Group != "" {
		prefix = "/apis/" + g.Group + "/" + g.Version
	}
	return prefix + "/namespaces/" + ns + "/" + resource
}

func stageGVK(obj any) (schema.GroupVersionKind, error) {
	var g schema.GroupVersionKind
	switch v := obj.(type) {
	case *corev1.Namespace:
		g = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	case *corev1.ConfigMap, *corev1.ConfigMapList:
		g = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	case *corev1.Pod:
		g = schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	case *kovav1.KovaBuild:
		g = schema.GroupVersionKind{Group: kovav1.Group, Version: kovav1.Version, Kind: "KovaBuild"}
	case *unstructured.Unstructured:
		g = v.GroupVersionKind()
	case *unstructured.UnstructuredList:
		g = v.GroupVersionKind()
		g.Kind = strings.TrimSuffix(g.Kind, "List")
	default:
		return g, ErrInput
	}
	if g != (schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}) &&
		g != (schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}) &&
		g != (schema.GroupVersionKind{Version: "v1", Kind: "Pod"}) &&
		g != (schema.GroupVersionKind{Group: kovav1.Group, Version: kovav1.Version, Kind: "KovaBuild"}) {
		return g, ErrInput
	}
	return g, nil
}

func newStageScope(p StagePins) (*stageScope, error) {
	s := &stageScope{namespaces: map[string]string{"kube-system": p.Connection.SystemNamespaceUID}, reads: map[string]string{}, targets: map[string]bool{}, attempts: map[string]map[string]string{}, lists: map[string]bool{}}
	addNS := func(name, uid string) error {
		if name == "kube-system" || len(validation.IsDNS1123Label(name)) != 0 || !safeToken(uid) || uid == p.Connection.SystemNamespaceUID {
			return ErrInput
		}
		if _, ok := s.namespaces[name]; ok {
			return ErrInput
		}
		for _, old := range s.namespaces {
			if old == uid {
				return ErrInput
			}
		}
		s.namespaces[name] = uid
		return nil
	}
	if p.Action == "collect-exact" && p.Inventory != nil {
		if addNS(p.Inventory.Runner.Name, p.Inventory.Runner.UID) != nil || addNS(p.Inventory.Receipts.Name, p.Inventory.Receipts.UID) != nil {
			return nil, ErrInput
		}
		for _, ns := range []string{p.Inventory.Runner.Name, p.Inventory.Receipts.Name} {
			for _, g := range []schema.GroupVersionKind{{Version: "v1", Kind: "ConfigMap"}, {Version: "v1", Kind: "Pod"}, {Group: kovav1.Group, Version: kovav1.Version, Kind: "KovaBuild"}} {
				s.lists[collection(g, ns)] = true
			}
		}
	} else if p.Action == "drain-exact" && p.Drain != nil {
		e := p.Drain.Expected
		if addNS(e.Epoch.Namespace, e.Epoch.NamespaceUID) != nil || addNS(e.Epoch.ReceiptNamespace, e.Epoch.ReceiptNamespaceUID) != nil {
			return nil, ErrInput
		}
		s.incident, s.epochUID = e.IncidentID, e.Epoch.NamespaceUID
		cmGVK := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
		if e.StopIntent.Namespace != e.Epoch.Namespace || len(validation.IsDNS1123Subdomain(e.StopIntent.Name)) != 0 {
			return nil, ErrInput
		}
		s.reads[collection(cmGVK, e.StopIntent.Namespace)+"/"+e.StopIntent.Name] = e.StopIntent.UID
		s.lists[collection(cmGVK, e.Epoch.ReceiptNamespace)] = true
		for _, r := range e.Receipts {
			if r.Namespace != e.Epoch.ReceiptNamespace || len(validation.IsDNS1123Subdomain(r.Name)) != 0 {
				return nil, ErrInput
			}
			path := collection(cmGVK, r.Namespace) + "/" + r.Name
			if _, ok := s.reads[path]; ok {
				return nil, ErrInput
			}
			s.reads[path] = r.UID
		}
		addTarget := func(kind, name string) error {
			if len(validation.IsDNS1123Subdomain(name)) != 0 {
				return ErrInput
			}
			g := schema.GroupVersionKind{Version: "v1", Kind: kind}
			if kind == "KovaBuild" {
				g.Group, g.Version = kovav1.Group, kovav1.Version
			}
			path := collection(g, e.Epoch.Namespace) + "/" + name
			s.targets[path] = true
			s.reads[path] = ""
			attempt := recoverydrain.OccupancyAttemptName(e.Epoch, kind, name)
			path = collection(cmGVK, e.Epoch.Namespace) + "/" + attempt
			s.reads[path] = ""
			s.attempts[path] = map[string]string{"version": "1", "namespaceUid": e.Epoch.NamespaceUID, "genesisUid": e.Epoch.GenesisUID, "kind": kind, "name": name}
			return nil
		}
		for _, q := range p.Drain.Queues {
			if addTarget("KovaBuild", q.BuildName) != nil {
				return nil, ErrInput
			}
		}
		for _, g := range p.Drain.Grants {
			if addTarget("KovaBuild", g.Build.BuildName) != nil {
				return nil, ErrInput
			}
		}
		for _, v := range p.Drain.PodCreates {
			if addTarget("Pod", v.PodName) != nil {
				return nil, ErrInput
			}
		}
	} else {
		return nil, ErrInput
	}
	for name, uid := range s.namespaces {
		s.reads["/api/v1/namespaces/"+name] = uid
	}
	return s, nil
}

// Each unknown wire or local qualification failure poisons the whole adapter.
// In particular, the library's post-Create readback cannot retrospectively
// qualify an unknown response and trigger another write.
type stageAPI struct {
	base            *exactAPI
	scope           *stageScope
	journal         *stageJournal
	calls, maxCalls int
	failed          bool
	observed        map[string]*unstructured.Unstructured
}

func newStageAPI(p StagePins, caPath, credPath string, j *stageJournal) (*stageAPI, error) {
	s, err := newStageScope(p)
	if err != nil {
		return nil, err
	}
	a, err := newConnectionAPI(p.Connection, maxResponseBytes, caPath, credPath)
	if err != nil {
		return nil, err
	}
	return &stageAPI{base: a, scope: s, journal: j, maxCalls: p.MaxAPICalls, observed: map[string]*unstructured.Unstructured{}}, nil
}

func (a *stageAPI) stop() error { a.failed = true; return errAPI }

func (a *stageAPI) wire(ctx context.Context, method, path, contentType string, body []byte, out *unstructured.Unstructured) error {
	if a.failed || ctx == nil || ctx.Err() != nil || a.calls >= a.maxCalls || len(body) > maxResponseBytes {
		return a.stop()
	}
	if a.journal.append(struct{ Stage, Method, Path, BodyDigest string }{"wire-attempt", method, path, digest(body)}) != nil {
		return a.stop()
	}
	a.calls++
	err := a.base.request(ctx, method, path, contentType, body, out)
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsAlreadyExists(err) {
		return a.stop()
	}
	return err
}

func stageAddress(g schema.GroupVersionKind, key client.ObjectKey) (string, error) {
	if len(validation.IsDNS1123Subdomain(key.Name)) != 0 {
		return "", ErrInput
	}
	if g.Kind == "Namespace" {
		if key.Namespace != "" {
			return "", ErrInput
		}
		return "/api/v1/namespaces/" + key.Name, nil
	}
	if len(validation.IsDNS1123Label(key.Namespace)) != 0 {
		return "", ErrInput
	}
	return collection(g, key.Namespace) + "/" + key.Name, nil
}

func qualifiedStageObject(u *unstructured.Unstructured, g schema.GroupVersionKind, key client.ObjectKey, uid string) bool {
	if len(validation.IsDNS1123Subdomain(key.Name)) != 0 || u.GroupVersionKind() != g || u.GetNamespace() != key.Namespace || u.GetName() != key.Name || !safeToken(string(u.GetUID())) ||
		!safeToken(u.GetResourceVersion()) || uid != "" && string(u.GetUID()) != uid {
		return false
	}
	_, err := deletionState(u)
	return err == nil
}

func copyStageObject(u *unstructured.Unstructured, out any) bool {
	if v, ok := out.(*unstructured.Unstructured); ok {
		v.Object = u.DeepCopy().Object
		return true
	}
	raw, err := json.Marshal(u.Object)
	if err != nil {
		return false
	}
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return false
	}
	// Decode into a fresh value. Reusing a nonzero receiver would retain fields
	// omitted by the API, including a caller's attempted spec/status edit.
	fresh := reflect.New(v.Elem().Type())
	strict, err := kjson.UnmarshalStrict(raw, fresh.Interface(), kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return false
	}
	v.Elem().Set(fresh.Elem())
	return true
}

func (a *stageAPI) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	if len(opts) != 0 {
		return a.stop()
	}
	g, err := stageGVK(out)
	if err != nil {
		return a.stop()
	}
	path, err := stageAddress(g, key)
	if err != nil {
		return a.stop()
	}
	uid, ok := a.scope.reads[path]
	if !ok {
		return a.stop()
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(g)
	u.SetName(key.Name)
	u.SetNamespace(key.Namespace)
	if err := a.wire(ctx, http.MethodGet, path, "", nil, u); err != nil {
		return err
	}
	if !qualifiedStageObject(u, g, key, uid) || !copyStageObject(u, out) {
		return a.stop()
	}
	a.observed[path] = u.DeepCopy()
	return nil
}

func (a *stageAPI) guardSystem(ctx context.Context) error {
	var ns corev1.Namespace
	if err := a.Get(ctx, client.ObjectKey{Name: "kube-system"}, &ns); err != nil || ns.DeletionTimestamp != nil || ns.Status.Phase != corev1.NamespaceActive {
		return a.stop()
	}
	return nil
}

func (a *stageAPI) List(ctx context.Context, out client.ObjectList, opts ...client.ListOption) error {
	o := &client.ListOptions{}
	for _, option := range opts {
		option.ApplyToList(o)
	}
	if o.Limit < 1 || o.Limit > 8192 || !reflect.DeepEqual(*o, client.ListOptions{Namespace: o.Namespace, Limit: o.Limit}) {
		return a.stop()
	}
	g, err := stageGVK(out)
	if err != nil || g.Kind == "Namespace" {
		return a.stop()
	}
	path := collection(g, o.Namespace)
	if !a.scope.lists[path] {
		return a.stop()
	}
	u := &unstructured.Unstructured{}
	if err := a.wire(ctx, http.MethodGet, path+"?limit="+strconv.FormatInt(o.Limit, 10), "", nil, u); err != nil {
		return err
	}
	if u.GetAPIVersion() != g.GroupVersion().String() || u.GetKind() != g.Kind+"List" || !safeToken(u.GetResourceVersion()) {
		return a.stop()
	}
	continuation, _, continueErr := unstructured.NestedString(u.Object, "metadata", "continue")
	remaining, _, remainingErr := unstructured.NestedInt64(u.Object, "metadata", "remainingItemCount")
	if continueErr != nil || continuation != "" || remainingErr != nil || remaining != 0 {
		return a.stop()
	}
	items, exists, err := unstructured.NestedSlice(u.Object, "items")
	if err != nil || !exists || items == nil || int64(len(items)) > o.Limit {
		return a.stop()
	}
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return a.stop()
		}
		v := &unstructured.Unstructured{Object: object}
		if !qualifiedStageObject(v, g, client.ObjectKey{Namespace: o.Namespace, Name: v.GetName()}, "") {
			return a.stop()
		}
	}
	raw, err := json.Marshal(u.Object)
	if err != nil {
		return a.stop()
	}
	if v, ok := out.(*unstructured.UnstructuredList); ok {
		if v.UnmarshalJSON(raw) != nil {
			return a.stop()
		}
		return nil
	}
	if !copyStageObject(u, out) {
		return a.stop()
	}
	return nil
}

func objectStageMap(obj client.Object, g schema.GroupVersionKind) (*unstructured.Unstructured, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, ErrInput
	}
	var m map[string]any
	strict, err := kjson.UnmarshalStrict(raw, &m, kjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 {
		return nil, ErrInput
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetGroupVersionKind(g)
	return u, nil
}

func (a *stageAPI) validCreate(path string, obj client.Object) bool {
	if obj.GetUID() != "" || obj.GetResourceVersion() != "" || obj.GetDeletionTimestamp() != nil || obj.GetGenerateName() != "" || len(obj.GetOwnerReferences()) != 0 || !slices.Equal(obj.GetFinalizers(), []string{recoveryHold}) {
		return false
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok {
		data, allowed := a.scope.attempts[path]
		return allowed && reflect.DeepEqual(cm.Data, data) && cm.Immutable != nil && *cm.Immutable && len(cm.BinaryData) == 0 &&
			reflect.DeepEqual(cm.Labels, map[string]string{"kova.cofy.dev/recovery-occupancy-attempt": "v1"}) && len(cm.Annotations) == 0
	}
	if !a.scope.targets[path] || !reflect.DeepEqual(obj.GetLabels(), map[string]string{tombstoneMarker: "v1"}) ||
		!reflect.DeepEqual(obj.GetAnnotations(), map[string]string{tombstoneMarker: a.scope.incident, "kova.cofy.dev/recovery-epoch-uid": a.scope.epochUID}) {
		return false
	}
	if b, ok := obj.(*kovav1.KovaBuild); ok {
		return recoverydrain.IsInertBuildTombstone(b)
	}
	p, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	no := false
	expected := corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, SchedulingGates: []corev1.PodSchedulingGate{{Name: recoveryHold}}, NodeSelector: map[string]string{tombstoneMarker: "never"}, AutomountServiceAccountToken: &no,
		Containers: []corev1.Container{{Name: "inert", Image: "registry.invalid/kova-recovery-tombstone:never", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/false"}}}}
	return reflect.DeepEqual(p.Spec, expected)
}

// Server defaulting may add object fields, but cannot remove or change any
// proposed field, insert array elements or substitute address/identity.
func containsProposal(proposed, current any) bool {
	switch p := proposed.(type) {
	case map[string]any:
		c, ok := current.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range p {
			if !containsProposal(v, c[k]) {
				return false
			}
		}
		return true
	case []any:
		c, ok := current.([]any)
		if !ok || len(p) != len(c) {
			return false
		}
		for i, v := range p {
			if !containsProposal(v, c[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(proposed, current)
	}
}

func (a *stageAPI) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if len(opts) != 0 {
		return a.stop()
	}
	g, err := stageGVK(obj)
	if err != nil {
		return a.stop()
	}
	key := client.ObjectKeyFromObject(obj)
	path, err := stageAddress(g, key)
	if err != nil || !a.validCreate(path, obj) {
		return a.stop()
	}
	proposed, err := objectStageMap(obj, g)
	if err != nil {
		return a.stop()
	}
	// These are unset server-assigned fields, not part of the Create proposal.
	unstructured.RemoveNestedField(proposed.Object, "metadata", "creationTimestamp")
	if status, ok := proposed.Object["status"].(map[string]any); ok && len(status) == 0 {
		delete(proposed.Object, "status")
	}
	raw, err := json.Marshal(proposed.Object)
	if err != nil {
		return a.stop()
	}
	if err := a.guardSystem(ctx); err != nil {
		return err
	}
	observed := proposed.DeepCopy()
	if err := a.wire(ctx, http.MethodPost, collection(g, key.Namespace), "application/json", raw, observed); err != nil {
		return err
	}
	if !qualifiedStageObject(observed, g, key, "") || !containsProposal(proposed.Object, observed.Object) || !copyStageObject(observed, obj) {
		return a.stop()
	}
	a.observed[path] = observed.DeepCopy()
	return nil
}

func (a *stageAPI) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if len(opts) != 0 {
		return a.stop()
	}
	g, err := stageGVK(obj)
	if err != nil {
		return a.stop()
	}
	key := client.ObjectKeyFromObject(obj)
	path, err := stageAddress(g, key)
	old := a.observed[path]
	if err != nil || !a.scope.targets[path] || old == nil || old.GetUID() != obj.GetUID() || old.GetResourceVersion() != obj.GetResourceVersion() ||
		slices.Contains(old.GetFinalizers(), recoveryHold) || !slices.Equal(obj.GetFinalizers(), append(slices.Clone(old.GetFinalizers()), recoveryHold)) {
		return a.stop()
	}
	proposed, err := objectStageMap(obj, g)
	if err != nil {
		return a.stop()
	}
	beforeTyped := obj.DeepCopyObject().(client.Object)
	if !copyStageObject(old, beforeTyped) {
		return a.stop()
	}
	beforeTyped.SetFinalizers(obj.GetFinalizers())
	if digestCanonical(beforeTyped) != digestCanonical(obj) {
		return a.stop()
	}
	// A JSON Patch preserves fields unknown to this API version. A fresh direct
	// observation, not a full-object PUT, supplies the exact UID/RV CAS.
	operations := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": string(old.GetUID())}, {"op": "test", "path": "/metadata/resourceVersion", "value": old.GetResourceVersion()}}
	if old.GetFinalizers() == nil {
		operations = append(operations, map[string]any{"op": "add", "path": "/metadata/finalizers", "value": obj.GetFinalizers()})
	} else {
		operations = append(operations, map[string]any{"op": "test", "path": "/metadata/finalizers", "value": old.GetFinalizers()}, map[string]any{"op": "replace", "path": "/metadata/finalizers", "value": obj.GetFinalizers()})
	}
	raw, err := json.Marshal(operations)
	if err != nil {
		return a.stop()
	}
	if err := a.guardSystem(ctx); err != nil {
		return err
	}
	observed := proposed.DeepCopy()
	if err := a.wire(ctx, http.MethodPatch, path, "application/json-patch+json", raw, observed); err != nil {
		return err
	}
	if !qualifiedStageObject(observed, g, key, string(old.GetUID())) {
		return a.stop()
	}
	left, right := old.DeepCopy(), observed.DeepCopy()
	left.SetFinalizers(obj.GetFinalizers())
	for _, field := range []string{"resourceVersion", "managedFields"} {
		unstructured.RemoveNestedField(left.Object, "metadata", field)
		unstructured.RemoveNestedField(right.Object, "metadata", field)
	}
	if digestCanonical(left.Object) != digestCanonical(right.Object) || !copyStageObject(observed, obj) {
		return a.stop()
	}
	a.observed[path] = observed.DeepCopy()
	return nil
}
