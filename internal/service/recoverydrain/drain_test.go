package recoverydrain

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeDirect struct {
	client.Client
	creates        int
	updates        int
	attempts       int
	builds         int
	pods           int
	intercept      func(client.Object) (bool, error)
	namespaceReads int
	stopReads      int
	getIntercept   func(client.Object) error
}

func (f *fakeDirect) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := f.Client.Get(ctx, key, obj, opts...)
	if _, ok := obj.(*corev1.Namespace); ok {
		f.namespaceReads++
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == "stop-incident-one" {
		f.stopReads++
	}
	if err == nil && f.getIntercept != nil {
		return f.getIntercept(obj)
	}
	return err
}

func (f *fakeDirect) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	f.creates++
	switch obj.(type) {
	case *corev1.ConfigMap:
		f.attempts++
	case *kovav1.KovaBuild:
		f.builds++
	case *corev1.Pod:
		f.pods++
	}
	if f.intercept != nil {
		if handled, err := f.intercept(obj); handled {
			return err
		}
	}
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(fmt.Sprintf("created-%d", f.creates)))
	}
	return f.Client.Create(ctx, obj, opts...)
}

func (f *fakeDirect) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	f.updates++
	return f.Client.Update(ctx, obj, opts...)
}

func fixture(t *testing.T, existingBuild bool, withPod bool) (Input, *fakeDirect, *kovav1.KovaBuild) {
	t.Helper()
	ctx := context.Background()
	epoch := recoverypermit.EpochIdentity{Namespace: "runner-old", NamespaceUID: "namespace-uid",
		ReceiptNamespace: "receipts-old", ReceiptNamespaceUID: "receipts-uid",
		GenesisName: "kova-service-admission-genesis", GenesisUID: "genesis-uid",
		Generation: strings.Repeat("a", 32), ActiveLedgerUID: "active-uid", QueueLedgerUID: "queue-uid", WorkerPoolID: "pool-old"}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: epoch.Namespace, Name: "build-one", UID: "build-original-uid", ResourceVersion: "1"},
		Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "alice", UID: "requester-uid"},
			Targets: []kovav1.KovaBuildTargetSpec{{Target: "registry.test/app:v1", Platform: "linux/amd64"}},
			Source:  kovav1.KovaBuildSourceSpec{URI: "https://source.test/archive", Digest: "sha256:" + strings.Repeat("b", 64)},
			Build:   kovav1.KovaBuildOptions{Format: "oci"}}}
	digest, err := queueadmission.DigestBuild(build)
	if err != nil {
		t.Fatal(err)
	}
	q := recoveryreceipt.QueueIntent{Namespace: epoch.Namespace, NamespaceUID: epoch.NamespaceUID,
		ReceiptNamespace: epoch.ReceiptNamespace, ReceiptNamespaceUID: epoch.ReceiptNamespaceUID,
		GenesisName: epoch.GenesisName, GenesisUID: epoch.GenesisUID, Generation: epoch.Generation,
		ActiveLedgerUID: epoch.ActiveLedgerUID, QueueLedgerUID: epoch.QueueLedgerUID,
		BuildName: build.Name, RequesterName: "alice", RequesterHash: queueadmission.HashRequester("alice"),
		RequesterUID: "requester-uid", RequestDigest: digest, SourceDigest: build.Spec.Source.Digest,
		QueueNonce: strings.Repeat("1", 32)}
	queueCM, err := recoveryreceipt.NewQueueConfigMap(q)
	if err != nil {
		t.Fatal(err)
	}
	queueCM.UID, queueCM.ResourceVersion = "queue-receipt-uid", "1"
	queueDigest, err := recoveryreceipt.DigestData(queueCM.Data)
	if err != nil {
		t.Fatal(err)
	}
	refs := []recoverypermit.ReceiptRef{{Kind: "queue", Namespace: epoch.ReceiptNamespace, Name: queueCM.Name, UID: string(queueCM.UID), DataDigest: queueDigest}}
	objects := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: epoch.Namespace, UID: types.UID(epoch.NamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: epoch.ReceiptNamespace, UID: types.UID(epoch.ReceiptNamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		queueCM,
	}
	input := Input{MaxLiabilities: 8, Queues: []recoveryreceipt.QueueIntent{q}}
	if existingBuild {
		objects = append(objects, build)
	}
	if withPod {
		pin := recoveryreceipt.PinnedBuild{Namespace: epoch.Namespace, NamespaceUID: epoch.NamespaceUID,
			ReceiptNamespace: epoch.ReceiptNamespace, ReceiptNamespaceUID: epoch.ReceiptNamespaceUID,
			GenesisName: epoch.GenesisName, GenesisUID: epoch.GenesisUID, Generation: epoch.Generation,
			ActiveLedgerUID: epoch.ActiveLedgerUID, QueueLedgerUID: epoch.QueueLedgerUID,
			BuildName: build.Name, BuildUID: string(build.UID), RequesterName: "alice", RequesterUID: "requester-uid",
			RequesterHash: q.RequesterHash, RequestDigest: digest, SourceDigest: q.SourceDigest, WorkerPoolID: epoch.WorkerPoolID}
		grant := recoveryreceipt.GrantIntent{Build: pin, AdmissionMode: "queued",
			QueueReceipt: &recoveryreceipt.ReceiptLink{Name: queueCM.Name, UID: string(queueCM.UID), DataDigest: queueDigest},
			GrantNonce:   strings.Repeat("2", 32), ActiveLedgerFence: 1, ActiveLedgerRV: "ledger-rv", AllocatedWorkerSlots: 1}
		grantCM, err := recoveryreceipt.NewGrantConfigMap(grant)
		if err != nil {
			t.Fatal(err)
		}
		grantCM.UID, grantCM.ResourceVersion = "grant-receipt-uid", "1"
		grantDigest, err := recoveryreceipt.DigestData(grantCM.Data)
		if err != nil {
			t.Fatal(err)
		}
		podIntent := recoveryreceipt.PodCreateIntent{Build: pin,
			GrantReceipt: recoveryreceipt.ReceiptLink{Name: grantCM.Name, UID: string(grantCM.UID), DataDigest: grantDigest},
			PodName:      "kova-job-" + build.Name, PodAttemptNonce: strings.Repeat("3", 32),
			PodTemplateDigest: "sha256:" + strings.Repeat("c", 64), RunnerImageDigest: "sha256:" + strings.Repeat("d", 64),
			BuildRequestID: string(build.UID)}
		podCM, err := recoveryreceipt.NewPodCreateConfigMap(podIntent)
		if err != nil {
			t.Fatal(err)
		}
		podCM.UID, podCM.ResourceVersion = "pod-receipt-uid", "1"
		podDigest, err := recoveryreceipt.DigestData(podCM.Data)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs,
			recoverypermit.ReceiptRef{Kind: "grant", Namespace: epoch.ReceiptNamespace, Name: grantCM.Name, UID: string(grantCM.UID), DataDigest: grantDigest},
			recoverypermit.ReceiptRef{Kind: "pod-create", Namespace: epoch.ReceiptNamespace, Name: podCM.Name, UID: string(podCM.UID), DataDigest: podDigest})
		objects = append(objects, grantCM, podCM)
		input.Grants = []recoveryreceipt.GrantIntent{grant}
		input.PodCreates = []recoveryreceipt.PodCreateIntent{podIntent}
	}
	// Permit hashing requires strict kind/name order.
	for i := 0; i < len(refs); i++ {
		for j := i + 1; j < len(refs); j++ {
			if refs[j].Kind < refs[i].Kind {
				refs[i], refs[j] = refs[j], refs[i]
			}
		}
	}
	stop := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "stop-incident-one", Namespace: epoch.Namespace,
		UID: "stop-uid", ResourceVersion: "1"}, Data: map[string]string{"version": "1", "incident": "incident-one"}}
	immutable := true
	stop.Immutable = &immutable
	stopDigest, err := recoveryreceipt.DigestData(stop.Data)
	if err != nil {
		t.Fatal(err)
	}
	objects = append(objects, stop)
	stopRef := recoverypermit.StopIntentRef{Namespace: epoch.Namespace, Name: stop.Name, UID: string(stop.UID), DataDigest: stopDigest}
	setDigest, err := recoverypermit.DigestReceiptSet(epoch, refs)
	if err != nil {
		t.Fatal(err)
	}
	routes := []recoverypermit.KubeObjectRef{{Namespace: "control", Name: "route-old", UID: "route-uid"}}
	deployments := []recoverypermit.KubeObjectRef{{Namespace: "control", Name: "service-old", UID: "deployment-uid"}}
	processes := []recoverypermit.ServiceProcessRef{{Pod: recoverypermit.KubeObjectRef{Namespace: "control", Name: "service-old-a", UID: "service-pod-uid"}, ContainerID: "containerd://old", ProcessID: "process-old"}}
	payload := recoverypermit.DrainPayload{Version: "1", Audience: "kova-recovery-drain-v1", Action: "drain-discard-only",
		Issuer: "operator", KeyID: "key-one", IncidentID: "incident-one", StopIntent: stopRef,
		Epoch: epoch, ReceiptCount: len(refs), ReceiptSetDigest: setDigest,
		WorkerRetirement: recoverypermit.WorkerPoolRetired{Type: "worker-pool-retired-v1", PoolID: epoch.WorkerPoolID,
			Epoch: epoch.Generation, WorkerIDs: []string{"worker-a"}, CapacitySlots: 1, Disposition: "terminated",
			EvidenceDigest: "sha256:" + strings.Repeat("e", 64), Assertion: "no-old-buildkit-execution-or-network-path-can-resume-v1",
			CutoffAt: "2020-01-01T00:00:00Z"},
		OldWritersStopped: recoverypermit.OldWritersStopped{Type: "old-writers-stopped-v1", StopIntent: stopRef,
			IngressRoutes: routes, ControlDeployments: deployments, ServiceProcesses: processes,
			EvidenceDigest: "sha256:" + strings.Repeat("f", 64),
			Assertion:      "ingress-frozen-old-processes-joined-no-uninstrumented-writers-or-post-confirmation-effects-v1",
			RetiredAt:      "2020-01-01T00:00:00Z"}, IssuedAt: "2020-01-01T00:00:01Z"}
	input.Expected = recoverypermit.Expectation{IncidentID: payload.IncidentID, StopIntent: stopRef, Epoch: epoch,
		Receipts: refs, WorkerIDs: []string{"worker-a"}, CapacitySlots: 1,
		IngressRoutes: routes, ControlDeployments: deployments, ServiceProcesses: processes}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{3}, ed25519.SeedSize))
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	sig := ed25519.Sign(private, append([]byte("KOVA-RECOVERY-DRAIN-PERMIT-V1\x00"), rawPayload...))
	raw, err := json.Marshal(recoverypermit.Envelope{Payload: payload, Signature: base64.StdEncoding.EncodeToString(sig)})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	input.Permit = raw
	input.Expected.PermitDigest = "sha256:" + hex.EncodeToString(sum[:])
	input.TrustRoots = recoverypermit.TrustRoots{"operator": {"key-one": private.Public().(ed25519.PublicKey)}}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api := &fakeDirect{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	// The fake client starts with the exact old evidence but no live API calls.
	if err := api.Get(ctx, client.ObjectKeyFromObject(stop), &corev1.ConfigMap{}); err != nil {
		t.Fatal(err)
	}
	return input, api, build
}

func TestDrainRetainsExactOriginalWithoutDeletingOrReleasing(t *testing.T) {
	in, api, build := fixture(t, true, false)
	report, err := Drain(context.Background(), api, in)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "occupied-not-drained" || len(report.Dispositions) != 1 ||
		report.Dispositions[0].State != "original-retained" || report.Dispositions[0].UID != string(build.UID) || api.creates != 0 {
		t.Fatalf("wrong conservative disposition: %#v, creates=%d", report, api.creates)
	}
	var held kovav1.KovaBuild
	if err := api.Get(context.Background(), client.ObjectKeyFromObject(build), &held); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(held.Finalizers, ","), holdFinalizer) {
		t.Fatal("original name was not held")
	}
}

func TestDrainOccupiesMissingBuildAndPodNamesOnce(t *testing.T) {
	in, api, _ := fixture(t, false, true)
	report, err := Drain(context.Background(), api, in)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "occupied-not-drained" || len(report.Dispositions) != 2 || api.attempts != 2 || api.builds != 1 || api.pods != 1 {
		t.Fatalf("unexpected occupancy: %#v, writes=%d/%d/%d", report, api.attempts, api.builds, api.pods)
	}
	for _, got := range report.Dispositions {
		if got.State != "tombstone-retained" {
			t.Fatalf("unexpected %v", got)
		}
	}
	if _, err := Drain(context.Background(), api, in); err != nil {
		t.Fatal(err)
	}
	if api.attempts != 2 || api.builds != 1 || api.pods != 1 {
		t.Fatal("rerun repeated a Create")
	}
}

func TestDrainUnknownAttemptNeverReplaysTombstone(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.intercept = func(obj client.Object) (bool, error) {
		if _, ok := obj.(*kovav1.KovaBuild); ok {
			return true, errors.New("lost tombstone Create response")
		}
		return false, nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.attempts != 1 || api.builds != 1 {
		t.Fatalf("expected unknown one-shot attempt: %#v, %v", report, err)
	}
	_, err = Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || api.attempts != 1 || api.builds != 1 {
		t.Fatalf("replayed unknown Create: %v", err)
	}
}

func TestDrainLostAttemptReceiptResponseNeverStartsTombstone(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.intercept = func(obj client.Object) (bool, error) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || !strings.HasPrefix(cm.Name, attemptPrefix) {
			return false, nil
		}
		cm.UID = "attempt-persisted-uid"
		if err := api.Client.Create(context.Background(), cm); err != nil {
			t.Fatal(err)
		}
		return true, errors.New("lost attempt receipt response")
	}
	_, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || api.attempts != 1 || api.builds != 0 {
		t.Fatalf("lost receipt armed a Create: %v, writes=%d/%d", err, api.attempts, api.builds)
	}
	_, err = Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || api.attempts != 1 || api.builds != 0 {
		t.Fatalf("second attempt replayed: %v, writes=%d/%d", err, api.attempts, api.builds)
	}
}

func TestDrainRetainsOnlyReceiptMatchedOriginalPod(t *testing.T) {
	for _, wrongOwner := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrong-owner-%t", wrongOwner), func(t *testing.T) {
			in, api, build := fixture(t, true, true)
			p := in.PodCreates[0]
			pod := matchingOriginalPod(p, build.Name)
			if wrongOwner {
				pod.OwnerReferences[0].UID = "replacement-build-uid"
			}
			if err := api.Client.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			report, err := Drain(context.Background(), api, in)
			if wrongOwner {
				if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.pods != 0 {
					t.Fatalf("foreign Pod admitted: %#v, %v", report, err)
				}
				return
			}
			if err != nil || report.Stage != "occupied-not-drained" || report.Dispositions[1].State != "original-retained" || api.pods != 0 {
				t.Fatalf("original Pod not held: %#v, %v", report, err)
			}
		})
	}
}

func matchingOriginalPod(p recoveryreceipt.PodCreateIntent, buildName string) *corev1.Pod {
	yes := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: p.Build.Namespace, Name: p.PodName,
		UID: "original-pod-uid", Labels: map[string]string{"kova.cofy.dev/build-id": buildName},
		Annotations: map[string]string{podAttemptKey: p.PodAttemptNonce, metricDigest: p.PodTemplateDigest},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: kovav1.Group + "/" + kovav1.Version,
			Kind: "KovaBuild", Name: buildName, UID: types.UID(p.Build.BuildUID), Controller: &yes}}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
			Containers:     []corev1.Container{{Name: "runner", Image: "registry.test/runner@" + p.RunnerImageDigest, Command: []string{"kovad", "daemon"}}},
			InitContainers: []corev1.Container{{Name: "source-fetch", Command: []string{"kovad", "source", "fetch", "--digest", p.Build.SourceDigest}}}}}
}

func TestWorkerRetirementPermitDoesNotSettleRunningRunner(t *testing.T) {
	in, api, build := fixture(t, true, true)
	pod := matchingOriginalPod(in.PodCreates[0], build.Name)
	pod.Status.Phase = corev1.PodRunning
	if err := api.Client.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	report, err := Drain(context.Background(), api, in)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "occupied-not-drained" || len(report.Dispositions) != 2 ||
		report.Dispositions[1].State != "original-retained" {
		t.Fatalf("worker retirement falsely settled runner: %#v", report)
	}
	var stillHere corev1.Pod
	if err := api.Get(context.Background(), client.ObjectKeyFromObject(pod), &stillHere); err != nil {
		t.Fatal(err)
	}
	if stillHere.UID != pod.UID || !strings.Contains(strings.Join(stillHere.Finalizers, ","), holdFinalizer) {
		t.Fatalf("runner Pod was not retained: %#v", stillHere.ObjectMeta)
	}
}

func TestNoCurrentCRorPodDoesNotProveReceiptCompleteness(t *testing.T) {
	in, api, _ := fixture(t, false, false) // no current KovaBuild or runner Pod exists.
	in.Queues = nil                        // signed receipt is present, but caller omitted its facts.
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrPreflight) || report.Stage != "preflight-blocked" || api.creates != 0 {
		t.Fatalf("absence inferred completeness: %#v, %v", report, err)
	}
}

func TestDrainMissingReceiptOrChangedNamespaceBlocksAllWrites(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(Input, *fakeDirect) Input
	}{
		{"omitted intent", func(in Input, _ *fakeDirect) Input { in.Queues = nil; return in }},
		{"wrong namespace UID", func(in Input, _ *fakeDirect) Input { in.Expected.Epoch.NamespaceUID = "replacement-uid"; return in }},
		{"wrong receipt namespace UID", func(in Input, _ *fakeDirect) Input {
			in.Expected.Epoch.ReceiptNamespaceUID = "replacement-uid"
			return in
		}},
		{"low liability cap", func(in Input, _ *fakeDirect) Input { in.MaxLiabilities = 0; return in }},
		{"changed signed digest", func(in Input, _ *fakeDirect) Input {
			in.Expected.Receipts[0].DataDigest = "sha256:" + strings.Repeat("0", 64)
			return in
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			in, api, _ := fixture(t, false, false)
			in = change.edit(in, api)
			report, err := Drain(context.Background(), api, in)
			if !errors.Is(err, ErrPreflight) || report.Stage != "preflight-blocked" || api.creates != 0 {
				t.Fatalf("bad preflight wrote: %#v, %v, creates=%d", report, err, api.creates)
			}
		})
	}
}

func TestDrainRejectsUnlistedOldReceipt(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	extra := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: in.Expected.Epoch.ReceiptNamespace,
		Name: "kova-grant-intent-" + strings.Repeat("f", 32), UID: "extra-uid"}}
	if err := api.Client.Create(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrPreflight) || report.Stage != "preflight-blocked" || api.creates != 0 {
		t.Fatalf("unlisted receipt wrote: %#v, %v", report, err)
	}
}

func TestDrainLateOriginalWinsAtomicNameRace(t *testing.T) {
	in, api, build := fixture(t, false, false)
	api.intercept = func(obj client.Object) (bool, error) {
		if _, ok := obj.(*kovav1.KovaBuild); !ok {
			return false, nil
		}
		late := build.DeepCopy()
		late.ResourceVersion = ""
		if err := api.Client.Create(context.Background(), late); err != nil {
			t.Fatal(err)
		}
		return true, apierrors.NewAlreadyExists(kovav1.SchemeGroupVersion.WithResource("kovabuilds").GroupResource(), obj.GetName())
	}
	report, err := Drain(context.Background(), api, in)
	if err != nil {
		t.Fatal(err)
	}
	if report.Dispositions[0].State != "original-retained" || report.Dispositions[0].UID != string(build.UID) || api.builds != 1 {
		t.Fatalf("late original was not retained: %#v", report)
	}
}

func TestNamespaceUIDReplacementBeforeFirstEffectIsUnknownAndDoesNotWrite(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.getIntercept = func(obj client.Object) error {
		if namespace, ok := obj.(*corev1.Namespace); ok && namespace.Name == in.Expected.Epoch.Namespace && api.namespaceReads >= 3 {
			namespace.UID = "replacement-namespace-uid"
		}
		return nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.creates != 0 {
		t.Fatalf("replacement namespace allowed write: %#v, %v, writes=%d", report, err, api.creates)
	}
}

func TestReceiptNamespaceUIDReplacementBeforeFirstEffectIsUnknownAndDoesNotWrite(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.getIntercept = func(obj client.Object) error {
		if namespace, ok := obj.(*corev1.Namespace); ok && namespace.Name == in.Expected.Epoch.ReceiptNamespace && api.namespaceReads >= 3 {
			namespace.UID = "replacement-receipt-namespace-uid"
		}
		return nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.creates != 0 {
		t.Fatalf("replacement receipt namespace allowed write: %#v, %v, writes=%d", report, err, api.creates)
	}
}

func TestNamespaceUIDReplacementBeforeOriginalHoldDoesNotUpdate(t *testing.T) {
	in, api, _ := fixture(t, true, false)
	api.getIntercept = func(obj client.Object) error {
		if namespace, ok := obj.(*corev1.Namespace); ok && namespace.Name == in.Expected.Epoch.Namespace && api.namespaceReads >= 3 {
			namespace.UID = "replacement-namespace-uid"
		}
		return nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.updates != 0 || api.creates != 0 {
		t.Fatalf("replacement namespace allowed hold: %#v, %v, updates=%d", report, err, api.updates)
	}
}

func TestOriginalBuildFromWrongNamespaceCannotBeHeld(t *testing.T) {
	in, api, _ := fixture(t, true, false)
	api.getIntercept = func(obj client.Object) error {
		if build, ok := obj.(*kovav1.KovaBuild); ok {
			build.Namespace = "replacement-namespace"
		}
		return nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.updates != 0 || api.creates != 0 {
		t.Fatalf("cross-namespace Build was held: %#v, %v, writes=%d/%d", report, err, api.updates, api.creates)
	}
}

func TestStopLossAfterAttemptPreventsTombstoneCreate(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.intercept = func(obj client.Object) (bool, error) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || !strings.HasPrefix(cm.Name, attemptPrefix) {
			return false, nil
		}
		cm.UID = "attempt-original-uid"
		if err := api.Client.Create(context.Background(), cm); err != nil {
			t.Fatal(err)
		}
		stop := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: in.Expected.StopIntent.Namespace, Name: in.Expected.StopIntent.Name}}
		if err := api.Client.Delete(context.Background(), stop); err != nil {
			t.Fatal(err)
		}
		return true, nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || api.attempts != 1 || api.builds != 0 {
		t.Fatalf("lost stop allowed tombstone Create: %#v, %v, writes=%d/%d", report, err, api.attempts, api.builds)
	}
}

func TestNamespaceReplacementAtFinalReadbackCannotReportOccupied(t *testing.T) {
	in, api, _ := fixture(t, false, false)
	api.getIntercept = func(obj client.Object) error {
		if namespace, ok := obj.(*corev1.Namespace); ok && namespace.Name == in.Expected.Epoch.Namespace && api.namespaceReads >= 9 {
			namespace.UID = "replacement-namespace-uid"
		}
		return nil
	}
	report, err := Drain(context.Background(), api, in)
	if !errors.Is(err, ErrUnknown) || report.Stage != "occupancy-unknown" || len(report.Dispositions) != 1 ||
		api.attempts != 1 || api.builds != 1 {
		t.Fatalf("replacement namespace falsely reported occupied: %#v, %v", report, err)
	}
}
