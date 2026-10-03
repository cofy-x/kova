// Package recoverydrain establishes monotonic name occupancy for a stopped,
// receipt-instrumented Kova epoch. It never deletes an original, removes a
// finalizer, frees a reservation, starts a runner, or treats absence as proof
// that an earlier Kubernetes Create cannot still arrive.
package recoverydrain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Each liability may also retain a recovery-attempt ConfigMap. Keep the
	// accepted receipt count below half of the bounded inventory List so the
	// attempt set and fixed Genesis/ledger objects cannot exhaust that read.
	maxLiabilities = 2048
	maxConfigMaps  = 8192
	markerKey      = "kova.cofy.dev/recovery-tombstone"
	markerValue    = "v1"
	holdFinalizer  = "kova.cofy.dev/recovery-hold"
	attemptPrefix  = "kova-recovery-occupancy-"
	attemptLabel   = "kova.cofy.dev/recovery-occupancy-attempt"
	podAttemptKey  = "kova.cofy.dev/create-attempt"
	metricDigest   = "kova.cofy.dev/pod-template-digest"
)

var (
	ErrPreflight = errors.New("recovery drain preflight is not qualified")
	ErrUnknown   = errors.New("recovery occupancy outcome is unknown")
)

// DirectAPI must be backed by an uncached Kubernetes API client. This narrow
// interface deliberately exposes no Delete, Patch, Exec, or Status writer.
type DirectAPI interface {
	Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error
	List(context.Context, client.ObjectList, ...client.ListOption) error
	Create(context.Context, client.Object, ...client.CreateOption) error
	Update(context.Context, client.Object, ...client.UpdateOption) error
}

// Input is supplied from an independently pinned operator record. The intent
// values are not trusted merely because they arrived in this call: preflight
// matches every one against its immutable, directly read API object and the
// exact signed receipt inventory before any occupancy write.
type Input struct {
	Permit         []byte
	Expected       recoverypermit.Expectation
	TrustRoots     recoverypermit.TrustRoots
	MaxLiabilities int
	Queues         []recoveryreceipt.QueueIntent
	Grants         []recoveryreceipt.GrantIntent
	PodCreates     []recoveryreceipt.PodCreateIntent
}

type Disposition struct {
	Kind string
	Name string
	UID  string
	// "original-retained" or "tombstone-retained". Neither is settlement.
	State string
}

type Report struct {
	Stage        string // preflight-blocked, occupancy-unknown, occupied-not-drained
	PermitDigest string
	Dispositions []Disposition
}

type target struct {
	kind  string
	name  string
	queue *recoveryreceipt.QueueIntent
	grant *recoveryreceipt.GrantIntent
	pods  []recoveryreceipt.PodCreateIntent
}

// Drain qualifies the old stopped epoch, then occupies every exact name. A
// successful return means only that old object names are durably occupied;
// runner execution, registry publication, and capacity remain unsettled.
func Drain(ctx context.Context, api DirectAPI, in Input) (Report, error) {
	report := Report{Stage: "preflight-blocked"}
	if api == nil || in.MaxLiabilities < 1 || in.MaxLiabilities > maxLiabilities ||
		len(in.Expected.Receipts) > in.MaxLiabilities {
		return report, ErrPreflight
	}
	evidence, err := recoverypermit.VerifyDrainPermit(in.Permit, in.Expected, in.TrustRoots)
	if err != nil {
		return report, fmt.Errorf("%w: permit: %v", ErrPreflight, err)
	}
	report.PermitDigest = evidence.PermitDigest
	if err := qualifyStopAndNamespace(ctx, api, in.Expected); err != nil {
		return report, fmt.Errorf("%w: %v", ErrPreflight, err)
	}
	targets, err := qualifyInventory(ctx, api, in)
	if err != nil {
		return report, fmt.Errorf("%w: %v", ErrPreflight, err)
	}
	report.Stage = "occupancy-unknown"
	for _, item := range targets {
		disposition, err := occupy(ctx, api, in.Expected, item)
		if err != nil {
			return report, fmt.Errorf("%w: %s/%s: %v", ErrUnknown, item.kind, item.name, err)
		}
		report.Dispositions = append(report.Dispositions, disposition)
	}
	if err := qualifyStopAndNamespace(ctx, api, in.Expected); err != nil {
		return report, fmt.Errorf("%w: final namespace/stop readback: %v", ErrUnknown, err)
	}
	report.Stage = "occupied-not-drained"
	return report, nil
}

func qualifyStopAndNamespace(ctx context.Context, api DirectAPI, exp recoverypermit.Expectation) error {
	var namespace corev1.Namespace
	if err := api.Get(ctx, client.ObjectKey{Name: exp.Epoch.Namespace}, &namespace); err != nil ||
		string(namespace.UID) != exp.Epoch.NamespaceUID || namespace.DeletionTimestamp != nil ||
		namespace.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("old namespace UID is unavailable or changed: %v", err)
	}
	var receipts corev1.Namespace
	if err := api.Get(ctx, client.ObjectKey{Name: exp.Epoch.ReceiptNamespace}, &receipts); err != nil ||
		string(receipts.UID) != exp.Epoch.ReceiptNamespaceUID || receipts.DeletionTimestamp != nil ||
		receipts.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("old receipt namespace UID is unavailable or changed: %v", err)
	}
	var stop corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: exp.StopIntent.Namespace, Name: exp.StopIntent.Name}, &stop); err != nil {
		return fmt.Errorf("stop intent direct read: %w", err)
	}
	digest, err := recoveryreceipt.DigestData(stop.Data)
	if err != nil || stop.Namespace != exp.Epoch.Namespace || string(stop.UID) != exp.StopIntent.UID ||
		stop.Name != exp.StopIntent.Name || stop.ResourceVersion == "" || stop.DeletionTimestamp != nil ||
		stop.Immutable == nil || !*stop.Immutable || len(stop.OwnerReferences) != 0 ||
		len(stop.BinaryData) != 0 || digest != exp.StopIntent.DataDigest {
		return errors.New("stop intent identity or immutable payload changed")
	}
	return nil
}

func refKey(kind, name string) string { return kind + "/" + name }

func qualifyInventory(ctx context.Context, api DirectAPI, in Input) ([]target, error) {
	exp := in.Expected
	provided := make(map[string]any, len(in.Queues)+len(in.Grants)+len(in.PodCreates))
	add := func(kind string, cm *corev1.ConfigMap, value any) error {
		if cm == nil || cm.Namespace != exp.Epoch.ReceiptNamespace {
			return ErrPreflight
		}
		key := refKey(kind, cm.Name)
		if _, exists := provided[key]; exists {
			return fmt.Errorf("duplicate receipt intent %s", key)
		}
		provided[key] = value
		return nil
	}
	for _, q := range in.Queues {
		cm, err := recoveryreceipt.NewQueueConfigMap(q)
		if err != nil || add("queue", cm, q) != nil {
			return nil, ErrPreflight
		}
	}
	for _, g := range in.Grants {
		cm, err := recoveryreceipt.NewGrantConfigMap(g)
		if err != nil || add("grant", cm, g) != nil {
			return nil, ErrPreflight
		}
	}
	for _, p := range in.PodCreates {
		cm, err := recoveryreceipt.NewPodCreateConfigMap(p)
		if err != nil || add("pod-create", cm, p) != nil {
			return nil, ErrPreflight
		}
	}
	if len(provided) != len(exp.Receipts) {
		return nil, fmt.Errorf("supplied intent count differs from signed receipt count")
	}
	refs := make(map[string]recoverypermit.ReceiptRef, len(exp.Receipts))
	for _, ref := range exp.Receipts {
		key := refKey(ref.Kind, ref.Name)
		if _, exists := provided[key]; !exists {
			return nil, fmt.Errorf("missing intent for %s", key)
		}
		refs[key] = ref
	}
	// List all ConfigMaps, not only labelled ones: a changed/missing label may
	// never make an old pre-effect receipt disappear from the inventory.
	var all corev1.ConfigMapList
	if err := api.List(ctx, &all, client.InNamespace(exp.Epoch.ReceiptNamespace), client.Limit(maxConfigMaps)); err != nil {
		return nil, fmt.Errorf("direct complete ConfigMap list: %w", err)
	}
	if all.Continue != "" || len(all.Items) > maxConfigMaps {
		return nil, errors.New("ConfigMap inventory exceeded one bounded complete page")
	}
	seen := make(map[string]bool, len(refs))
	for i := range all.Items {
		cm := &all.Items[i]
		kind := receiptKind(cm.Name)
		if kind == "" {
			continue
		}
		key := refKey(kind, cm.Name)
		ref, ok := refs[key]
		if !ok || seen[key] || cm.Namespace != ref.Namespace || string(cm.UID) != ref.UID {
			return nil, fmt.Errorf("unlisted, duplicate, or replaced old receipt %s", key)
		}
		seen[key] = true
	}
	if len(seen) != len(refs) {
		return nil, errors.New("direct old-receipt list missed a signed receipt")
	}
	for _, ref := range exp.Receipts {
		var cm corev1.ConfigMap
		if err := api.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &cm); err != nil {
			return nil, fmt.Errorf("direct receipt read %s: %w", ref.Name, err)
		}
		var digest string
		key := refKey(ref.Kind, ref.Name)
		switch value := provided[key].(type) {
		case recoveryreceipt.QueueIntent:
			witness, err := recoveryreceipt.QualifyQueue(value, &cm)
			if err != nil {
				return nil, err
			}
			digest = witness.DataDigest
		case recoveryreceipt.GrantIntent:
			witness, err := recoveryreceipt.QualifyGrant(value, &cm)
			if err != nil {
				return nil, err
			}
			digest = witness.DataDigest
		case recoveryreceipt.PodCreateIntent:
			witness, err := recoveryreceipt.QualifyPodCreate(value, &cm)
			if err != nil {
				return nil, err
			}
			digest = witness.DataDigest
		default:
			return nil, ErrPreflight
		}
		if string(cm.UID) != ref.UID || digest != ref.DataDigest {
			return nil, fmt.Errorf("receipt UID or data digest changed: %s", key)
		}
	}
	return linkTargets(in)
}

func receiptKind(name string) string {
	switch {
	case strings.HasPrefix(name, "kova-admission-intent-"):
		return "queue"
	case strings.HasPrefix(name, "kova-grant-intent-"):
		return "grant"
	case strings.HasPrefix(name, "kova-pod-create-intent-"):
		return "pod-create"
	default:
		return ""
	}
}

func linkTargets(in Input) ([]target, error) {
	epoch := in.Expected.Epoch
	byBuild := make(map[string]*target)
	queueRef := make(map[string]recoverypermit.ReceiptRef)
	grantRef := make(map[string]recoverypermit.ReceiptRef)
	for _, ref := range in.Expected.Receipts {
		switch ref.Kind {
		case "queue":
			queueRef[ref.Name] = ref
		case "grant":
			grantRef[ref.Name] = ref
		}
	}
	for _, q := range in.Queues {
		if q.Namespace != epoch.Namespace || q.NamespaceUID != epoch.NamespaceUID || q.GenesisName != epoch.GenesisName ||
			q.ReceiptNamespace != epoch.ReceiptNamespace || q.ReceiptNamespaceUID != epoch.ReceiptNamespaceUID ||
			q.GenesisUID != epoch.GenesisUID || q.Generation != epoch.Generation ||
			q.ActiveLedgerUID != epoch.ActiveLedgerUID || q.QueueLedgerUID != epoch.QueueLedgerUID || byBuild[q.BuildName] != nil {
			return nil, errors.New("queue receipt epoch or build identity conflicts")
		}
		qCopy := q
		byBuild[q.BuildName] = &target{kind: "KovaBuild", name: q.BuildName, queue: &qCopy}
	}
	for _, g := range in.Grants {
		b := g.Build
		if b.Namespace != epoch.Namespace || b.NamespaceUID != epoch.NamespaceUID || b.GenesisName != epoch.GenesisName ||
			b.ReceiptNamespace != epoch.ReceiptNamespace || b.ReceiptNamespaceUID != epoch.ReceiptNamespaceUID ||
			b.GenesisUID != epoch.GenesisUID || b.Generation != epoch.Generation || b.ActiveLedgerUID != epoch.ActiveLedgerUID ||
			b.QueueLedgerUID != epoch.QueueLedgerUID || b.WorkerPoolID != epoch.WorkerPoolID {
			return nil, errors.New("grant receipt epoch conflicts")
		}
		item := byBuild[b.BuildName]
		if item == nil {
			item = &target{kind: "KovaBuild", name: b.BuildName}
			byBuild[b.BuildName] = item
		}
		if item.grant != nil || g.AdmissionMode == "queued" && item.queue == nil ||
			g.AdmissionMode == "direct" && item.queue != nil {
			return nil, errors.New("grant/queue topology conflicts")
		}
		if item.queue != nil {
			q := item.queue
			ref := queueRef["kova-admission-intent-"+q.QueueNonce]
			if g.QueueReceipt == nil || g.QueueReceipt.Name != ref.Name || g.QueueReceipt.UID != ref.UID ||
				g.QueueReceipt.DataDigest != ref.DataDigest || q.RequesterName != b.RequesterName ||
				q.RequesterHash != b.RequesterHash || q.RequesterUID != b.RequesterUID ||
				q.RequestDigest != b.RequestDigest || q.SourceDigest != b.SourceDigest {
				return nil, errors.New("grant does not link exact queue receipt")
			}
		}
		gCopy := g
		item.grant = &gCopy
	}
	byPod := make(map[string]*target)
	for _, p := range in.PodCreates {
		b := p.Build
		item := byBuild[b.BuildName]
		ref := grantRef[p.GrantReceipt.Name]
		if item == nil || item.grant == nil || !sameBuild(item.grant.Build, b) ||
			p.GrantReceipt.Name != ref.Name || p.GrantReceipt.UID != ref.UID ||
			p.GrantReceipt.DataDigest != ref.DataDigest || p.PodName != "kova-job-"+b.BuildName {
			return nil, errors.New("Pod attempt does not link exact grant/build")
		}
		podTarget := byPod[p.PodName]
		if podTarget == nil {
			podTarget = &target{kind: "Pod", name: p.PodName, grant: item.grant}
			byPod[p.PodName] = podTarget
		}
		podTarget.pods = append(podTarget.pods, p)
	}
	targets := make([]target, 0, len(byBuild)+len(byPod))
	for _, item := range byBuild {
		targets = append(targets, *item)
	}
	for _, item := range byPod {
		targets = append(targets, *item)
	}
	slices.SortFunc(targets, func(a, b target) int {
		if a.kind != b.kind {
			return strings.Compare(a.kind, b.kind)
		}
		return strings.Compare(a.name, b.name)
	})
	return targets, nil
}

func sameBuild(a, b recoveryreceipt.PinnedBuild) bool { return a == b }

func occupiedMarker(meta metav1.ObjectMeta, exp recoverypermit.Expectation, kind, name string) bool {
	return meta.Labels[markerKey] == markerValue && meta.Annotations[markerKey] == exp.IncidentID &&
		meta.Annotations["kova.cofy.dev/recovery-epoch-uid"] == exp.Epoch.NamespaceUID &&
		meta.Namespace == exp.Epoch.Namespace && meta.Name == name &&
		meta.UID != "" && meta.ResourceVersion != "" && meta.DeletionTimestamp == nil &&
		slices.Contains(meta.Finalizers, holdFinalizer) && len(meta.OwnerReferences) == 0
}

func occupy(ctx context.Context, api DirectAPI, exp recoverypermit.Expectation, item target) (Disposition, error) {
	observed, err := getTarget(ctx, api, exp.Epoch.Namespace, item)
	if err == nil {
		return retain(ctx, api, exp, item, observed)
	}
	if !apierrors.IsNotFound(err) {
		return Disposition{}, err
	}
	// A durable, name-specific attempt is written before the only possible
	// tombstone Create. If its response is lost, we do not try that Create.
	created, err := recordAttemptOnce(ctx, api, exp, item)
	if err != nil || !created {
		if err == nil {
			err = errors.New("old attempt exists; no second tombstone Create")
		}
		return Disposition{}, err
	}
	var proposed client.Object
	if item.kind == "KovaBuild" {
		proposed = newBuildTombstone(exp, item.name)
	} else {
		proposed = newPodTombstone(exp, item.name)
	}
	if err := qualifyStopAndNamespace(ctx, api, exp); err != nil {
		return Disposition{}, fmt.Errorf("namespace/stop changed before tombstone Create: %w", err)
	}
	createErr := api.Create(ctx, proposed) // exactly one wire attempt by caller's transport
	observed, readErr := getTarget(ctx, api, exp.Epoch.Namespace, item)
	if readErr != nil {
		return Disposition{}, fmt.Errorf("tombstone Create %v; direct read %w", createErr, readErr)
	}
	if createErr == nil && proposed.GetUID() == "" {
		return Disposition{}, errors.New("successful Create response lacked UID")
	}
	if createErr == nil && proposed.GetUID() != observed.GetUID() {
		return Disposition{}, errors.New("Create response and direct occupant UID differ")
	}
	return retain(ctx, api, exp, item, observed)
}

func getTarget(ctx context.Context, api DirectAPI, namespace string, item target) (client.Object, error) {
	key := client.ObjectKey{Namespace: namespace, Name: item.name}
	if item.kind == "KovaBuild" {
		value := &kovav1.KovaBuild{}
		err := api.Get(ctx, key, value)
		return value, err
	}
	value := &corev1.Pod{}
	err := api.Get(ctx, key, value)
	return value, err
}

func retain(ctx context.Context, api DirectAPI, exp recoverypermit.Expectation, item target, observed client.Object) (Disposition, error) {
	if observed.GetDeletionTimestamp() != nil || observed.GetUID() == "" || observed.GetResourceVersion() == "" {
		return Disposition{}, errors.New("occupant is deleting or lacks exact identity")
	}
	state := "original-retained"
	if observed.GetLabels()[markerKey] == markerValue {
		state = "tombstone-retained"
		if err := qualifyTombstone(exp, item, observed); err != nil {
			return Disposition{}, err
		}
	} else if err := qualifyOriginal(item, observed); err != nil {
		return Disposition{}, err
	}
	if !slices.Contains(observed.GetFinalizers(), holdFinalizer) {
		if err := qualifyStopAndNamespace(ctx, api, exp); err != nil {
			return Disposition{}, fmt.Errorf("namespace/stop changed before hold: %w", err)
		}
		observed.SetFinalizers(append(slices.Clone(observed.GetFinalizers()), holdFinalizer))
		if err := api.Update(ctx, observed); err != nil {
			return Disposition{}, fmt.Errorf("UID/RV-bounded hold: %w", err)
		}
	}
	readback, err := getTarget(ctx, api, exp.Epoch.Namespace, item)
	if err != nil || readback.GetUID() != observed.GetUID() || readback.GetDeletionTimestamp() != nil ||
		!slices.Contains(readback.GetFinalizers(), holdFinalizer) {
		return Disposition{}, errors.New("held occupant direct readback changed")
	}
	if state == "tombstone-retained" {
		if err := qualifyTombstone(exp, item, readback); err != nil {
			return Disposition{}, err
		}
	} else if err := qualifyOriginal(item, readback); err != nil {
		return Disposition{}, err
	}
	if err := qualifyStopAndNamespace(ctx, api, exp); err != nil {
		return Disposition{}, fmt.Errorf("namespace/stop changed after occupant readback: %w", err)
	}
	return Disposition{Kind: item.kind, Name: item.name, UID: string(readback.GetUID()), State: state}, nil
}

func qualifyOriginal(item target, observed client.Object) error {
	switch value := observed.(type) {
	case *kovav1.KovaBuild:
		expectedNamespace := ""
		if item.queue != nil {
			expectedNamespace = item.queue.Namespace
		}
		if item.grant != nil {
			expectedNamespace = item.grant.Build.Namespace
		}
		if item.kind != "KovaBuild" || expectedNamespace == "" || value.Namespace != expectedNamespace ||
			value.Name != item.name || value.DeletionTimestamp != nil ||
			value.Labels[markerKey] != "" {
			return errors.New("foreign KovaBuild occupant")
		}
		digest, err := queueadmission.DigestBuild(value)
		if err != nil {
			return err
		}
		if item.queue != nil {
			q := item.queue
			if digest != q.RequestDigest || value.Spec.Requester.Username != q.RequesterName ||
				value.Spec.Requester.UID != q.RequesterUID || value.Spec.Source.Digest != q.SourceDigest {
				return errors.New("KovaBuild differs from queue receipt")
			}
		}
		if item.grant != nil {
			b := item.grant.Build
			if string(value.UID) != b.BuildUID || digest != b.RequestDigest ||
				value.Spec.Requester.Username != b.RequesterName || value.Spec.Requester.UID != b.RequesterUID ||
				value.Spec.Source.Digest != b.SourceDigest {
				return errors.New("KovaBuild differs from grant receipt")
			}
		}
		return nil
	case *corev1.Pod:
		if item.kind != "Pod" || value.Namespace != item.grant.Build.Namespace || value.Name != item.name ||
			value.Labels[markerKey] != "" || len(value.OwnerReferences) != 1 ||
			value.OwnerReferences[0].APIVersion != kovav1.Group+"/"+kovav1.Version ||
			value.OwnerReferences[0].Kind != "KovaBuild" || value.OwnerReferences[0].Name != item.grant.Build.BuildName ||
			string(value.OwnerReferences[0].UID) != item.grant.Build.BuildUID ||
			value.OwnerReferences[0].Controller == nil || !*value.OwnerReferences[0].Controller ||
			value.Labels["kova.cofy.dev/build-id"] != item.grant.Build.BuildName ||
			value.Spec.RestartPolicy != corev1.RestartPolicyNever || len(value.Spec.Containers) != 1 ||
			value.Spec.Containers[0].Name != "runner" ||
			!slices.Equal(value.Spec.Containers[0].Command, []string{"kovad", "daemon"}) {
			return errors.New("foreign Pod occupant")
		}
		for _, p := range item.pods {
			computed, digestErr := recoveryreceipt.CanonicalPodTemplateDigest(value)
			if value.Annotations[podAttemptKey] == p.PodAttemptNonce &&
				value.Annotations[metricDigest] == p.PodTemplateDigest &&
				digestErr == nil && computed == p.PodTemplateDigest &&
				strings.HasSuffix(value.Spec.Containers[0].Image, "@"+p.RunnerImageDigest) &&
				podFetchesDigest(value, p.Build.SourceDigest) {
				return nil
			}
		}
		return errors.New("Pod does not match a pre-effect attempt/source/image receipt")
	default:
		return ErrPreflight
	}
}

func podFetchesDigest(pod *corev1.Pod, digest string) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.Name != "source-fetch" {
			continue
		}
		for i := 0; i+1 < len(c.Command); i++ {
			if c.Command[i] == "--digest" && c.Command[i+1] == digest {
				return true
			}
		}
	}
	return false
}

func qualifyTombstone(exp recoverypermit.Expectation, item target, observed client.Object) error {
	meta := metav1.ObjectMeta{Namespace: observed.GetNamespace(), Name: observed.GetName(), UID: observed.GetUID(),
		ResourceVersion: observed.GetResourceVersion(), DeletionTimestamp: observed.GetDeletionTimestamp(),
		Labels: observed.GetLabels(), Annotations: observed.GetAnnotations(), Finalizers: observed.GetFinalizers(),
		OwnerReferences: observed.GetOwnerReferences()}
	if !occupiedMarker(meta, exp, item.kind, item.name) {
		return errors.New("tombstone identity changed")
	}
	if item.kind == "KovaBuild" {
		value, ok := observed.(*kovav1.KovaBuild)
		if !ok || !IsInertBuildTombstone(value) {
			return errors.New("KovaBuild tombstone is not inert")
		}
		return nil
	}
	value, ok := observed.(*corev1.Pod)
	if !ok || len(value.Spec.SchedulingGates) != 1 || value.Spec.SchedulingGates[0].Name != holdFinalizer ||
		value.Spec.NodeSelector["kova.cofy.dev/recovery-tombstone"] != "never" ||
		value.Spec.AutomountServiceAccountToken == nil || *value.Spec.AutomountServiceAccountToken ||
		value.Spec.NodeName != "" || value.Spec.HostNetwork || value.Spec.HostPID || value.Spec.HostIPC ||
		len(value.Spec.InitContainers) != 0 || len(value.Spec.EphemeralContainers) != 0 ||
		len(value.Spec.Containers) != 1 || value.Spec.Containers[0].ImagePullPolicy != corev1.PullNever ||
		value.Spec.Containers[0].Name != "inert" || !slices.Equal(value.Spec.Containers[0].Command, []string{"/bin/false"}) ||
		value.Spec.Containers[0].Image != "registry.invalid/kova-recovery-tombstone:never" ||
		value.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return errors.New("Pod tombstone is not inert")
	}
	return nil
}

func newMeta(exp recoverypermit.Expectation, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: exp.Epoch.Namespace, Name: name,
		Labels:      map[string]string{markerKey: markerValue},
		Annotations: map[string]string{markerKey: exp.IncidentID, "kova.cofy.dev/recovery-epoch-uid": exp.Epoch.NamespaceUID},
		Finalizers:  []string{holdFinalizer}}
}

func newBuildTombstone(exp recoverypermit.Expectation, name string) *kovav1.KovaBuild {
	return &kovav1.KovaBuild{TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: newMeta(exp, name),
		Spec:       inertBuildSpec()}
}

func inertBuildSpec() kovav1.KovaBuildSpec {
	return kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "recovery-tombstone"},
		Targets: []kovav1.KovaBuildTargetSpec{{Target: "recovery.invalid/tombstone:never", Platform: "linux/amd64"}},
		Source:  kovav1.KovaBuildSourceSpec{URI: "https://recovery.invalid/inert", Digest: "sha256:" + strings.Repeat("0", 64)},
		// The serving CRD's CEL rule reads self.spec.build.concurrency;
		// omitting it is rejected even though the Go field is omitempty.
		Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1}}
}

// IsInertBuildTombstone is the controller's strict skip predicate. A marker
// label on a real KovaBuild cannot suppress its ordinary reconcile/cleanup.
func IsInertBuildTombstone(build *kovav1.KovaBuild) bool {
	return build != nil && build.Labels[markerKey] == markerValue &&
		build.Annotations[markerKey] != "" && build.Annotations["kova.cofy.dev/recovery-epoch-uid"] != "" &&
		slices.Contains(build.Finalizers, holdFinalizer) && len(build.OwnerReferences) == 0 &&
		reflect.DeepEqual(build.Spec, inertBuildSpec())
}

func newPodTombstone(exp recoverypermit.Expectation, name string) *corev1.Pod {
	no := false
	return &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: newMeta(exp, name),
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
			SchedulingGates:              []corev1.PodSchedulingGate{{Name: holdFinalizer}},
			NodeSelector:                 map[string]string{"kova.cofy.dev/recovery-tombstone": "never"},
			AutomountServiceAccountToken: &no,
			Containers: []corev1.Container{{Name: "inert", Image: "registry.invalid/kova-recovery-tombstone:never",
				ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/false"}}}}}
}

func attemptName(exp recoverypermit.Expectation, item target) string {
	raw, _ := json.Marshal([]string{exp.Epoch.NamespaceUID, exp.Epoch.GenesisUID, item.kind, item.name})
	sum := sha256.Sum256(raw)
	return attemptPrefix + hex.EncodeToString(sum[:16])
}

func newAttempt(exp recoverypermit.Expectation, item target) *corev1.ConfigMap {
	immutable := true
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: exp.Epoch.Namespace, Name: attemptName(exp, item),
		Labels: map[string]string{attemptLabel: "v1"}, Finalizers: []string{holdFinalizer}}, Immutable: &immutable,
		Data: map[string]string{"version": "1", "namespaceUid": exp.Epoch.NamespaceUID,
			"genesisUid": exp.Epoch.GenesisUID, "kind": item.kind, "name": item.name}}
}

func sameAttempt(a, b *corev1.ConfigMap) bool {
	return a != nil && b != nil && a.Namespace == b.Namespace && a.Name == b.Name && a.UID != "" &&
		a.ResourceVersion != "" && a.DeletionTimestamp == nil && a.Immutable != nil && *a.Immutable &&
		len(a.OwnerReferences) == 0 && len(a.BinaryData) == 0 && slices.Contains(a.Finalizers, holdFinalizer) &&
		len(a.Data) == len(b.Data) && a.Labels[attemptLabel] == "v1" &&
		a.Data["version"] == b.Data["version"] && a.Data["namespaceUid"] == b.Data["namespaceUid"] &&
		a.Data["genesisUid"] == b.Data["genesisUid"] && a.Data["kind"] == b.Data["kind"] && a.Data["name"] == b.Data["name"]
}

func recordAttemptOnce(ctx context.Context, api DirectAPI, exp recoverypermit.Expectation, item target) (bool, error) {
	expected := newAttempt(exp, item)
	var existing corev1.ConfigMap
	err := api.Get(ctx, client.ObjectKeyFromObject(expected), &existing)
	if err == nil {
		if !sameAttempt(&existing, expected) {
			return false, errors.New("existing attempt changed")
		}
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, err
	}
	if err := qualifyStopAndNamespace(ctx, api, exp); err != nil {
		return false, fmt.Errorf("namespace/stop changed before occupancy attempt: %w", err)
	}
	created := expected.DeepCopy()
	createErr := api.Create(ctx, created)
	var observed corev1.ConfigMap
	readErr := api.Get(ctx, client.ObjectKeyFromObject(expected), &observed)
	if readErr != nil || !sameAttempt(&observed, expected) || createErr != nil ||
		created.UID == "" || created.UID != observed.UID {
		return false, fmt.Errorf("attempt Create %v; direct read %v; fresh-UID proof failed", createErr, readErr)
	}
	return true, nil
}
