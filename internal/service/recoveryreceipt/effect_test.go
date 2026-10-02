package recoveryreceipt

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func examplePinnedBuild() PinnedBuild {
	q := exampleQueueIntent()
	return PinnedBuild{
		Namespace: q.Namespace, NamespaceUID: q.NamespaceUID,
		GenesisName: q.GenesisName, GenesisUID: q.GenesisUID, Generation: q.Generation,
		ActiveLedgerUID: q.ActiveLedgerUID, QueueLedgerUID: q.QueueLedgerUID,
		BuildName: q.BuildName, BuildUID: "build-original-uid",
		RequesterName: q.RequesterName, RequesterUID: q.RequesterUID, RequesterHash: q.RequesterHash,
		RequestDigest: q.RequestDigest, SourceDigest: q.SourceDigest,
		WorkerPoolID: "worker-pool-original",
	}
}

func exampleGrantIntent(t *testing.T) GrantIntent {
	t.Helper()
	q := exampleQueueIntent()
	queueCM := serverReceipt(t, q, "queue-receipt-original-uid")
	digest, err := DigestData(queueCM.Data)
	if err != nil {
		t.Fatal(err)
	}
	return GrantIntent{
		Build: examplePinnedBuild(), AdmissionMode: "queued",
		QueueReceipt: &ReceiptLink{Name: queueCM.Name, UID: string(queueCM.UID), DataDigest: digest},
		GrantNonce:   "11111111111111111111111111111111", ActiveLedgerFence: 7,
		ActiveLedgerRV: "ledger-rv-7", AllocatedWorkerSlots: 2,
	}
}

func examplePodCreateIntent(t *testing.T) PodCreateIntent {
	t.Helper()
	g := exampleGrantIntent(t)
	grantCM, err := NewGrantConfigMap(g)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestData(grantCM.Data)
	if err != nil {
		t.Fatal(err)
	}
	return PodCreateIntent{
		Build:        g.Build,
		GrantReceipt: ReceiptLink{Name: grantCM.Name, UID: "grant-receipt-original-uid", DataDigest: digest},
		PodName:      "kova-build-pod-original", PodAttemptNonce: "22222222222222222222222222222222",
		PodTemplateDigest: "sha256:" + strings.Repeat("a", 64),
		RunnerImageDigest: "sha256:" + strings.Repeat("b", 64),
		BuildRequestID:    g.Build.BuildUID,
	}
}

func serverEffect(t *testing.T, cm *corev1.ConfigMap, uid string) *corev1.ConfigMap {
	t.Helper()
	copy := cm.DeepCopy()
	copy.UID = types.UID(uid)
	copy.ResourceVersion = "rv-opaque"
	return copy
}

func TestEffectReceiptsAreImmutableBoundedAndUnowned(t *testing.T) {
	g := exampleGrantIntent(t)
	p := examplePodCreateIntent(t)
	for _, tc := range []struct {
		name string
		new  func() (*corev1.ConfigMap, error)
		kind string
	}{
		{name: "grant", new: func() (*corev1.ConfigMap, error) { return NewGrantConfigMap(g) }, kind: "grant"},
		{name: "pod-create", new: func() (*corev1.ConfigMap, error) { return NewPodCreateConfigMap(p) }, kind: "pod-create"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm, err := tc.new()
			if err != nil {
				t.Fatal(err)
			}
			if cm.Namespace != g.Build.Namespace || cm.Immutable == nil || !*cm.Immutable ||
				len(cm.OwnerReferences) != 0 || len(cm.Data) != 3 || cm.Data["kind"] != tc.kind ||
				len(cm.Data["payload"]) > maxEffectJSONBytes || len(cm.Labels) != 1 {
				t.Fatalf("unqualified receipt proposal: %#v", cm)
			}
			if strings.Contains(cm.Data["payload"], "https://") || strings.Contains(cm.Data["payload"], "token=") {
				t.Fatal("receipt payload unexpectedly contains source URI or token")
			}
		})
	}
}

func TestEffectReceiptCreateAndDirectReadback(t *testing.T) {
	g := exampleGrantIntent(t)
	p := examplePodCreateIntent(t)
	for _, tc := range []struct {
		name   string
		new    func() (*corev1.ConfigMap, error)
		record func(ConfigMaps) (EffectWitness, error)
	}{
		{name: "grant", new: func() (*corev1.ConfigMap, error) { return NewGrantConfigMap(g) },
			record: func(api ConfigMaps) (EffectWitness, error) { return RecordGrantOnce(context.Background(), api, g) }},
		{name: "pod-create", new: func() (*corev1.ConfigMap, error) { return NewPodCreateConfigMap(p) },
			record: func(api ConfigMaps) (EffectWitness, error) { return RecordPodCreateOnce(context.Background(), api, p) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proposal, err := tc.new()
			if err != nil {
				t.Fatal(err)
			}
			stored := serverEffect(t, proposal, "original-receipt-uid")
			api := &scriptedConfigMaps{
				create: func(cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
					if cm.Name != proposal.Name || cm.Data["payload"] != proposal.Data["payload"] {
						t.Fatalf("wrong Create: %#v", cm)
					}
					return stored.DeepCopy(), nil
				},
				get: func(name string) (*corev1.ConfigMap, error) {
					if name != proposal.Name {
						t.Fatalf("wrong direct GET name %q", name)
					}
					return stored.DeepCopy(), nil
				},
			}
			witness, err := tc.record(api)
			if err != nil || api.puts != 1 || api.reads != 1 || witness.ReceiptUID != string(stored.UID) || witness.DataDigest == "" {
				t.Fatalf("receipt=%#v err=%v calls=%d/%d", witness, err, api.puts, api.reads)
			}
		})
	}
}

func TestEffectReceiptLostResponseNeverReplaysAndRejectsChangedIdentity(t *testing.T) {
	g := exampleGrantIntent(t)
	proposal, err := NewGrantConfigMap(g)
	if err != nil {
		t.Fatal(err)
	}
	stored := serverEffect(t, proposal, "original-receipt-uid")
	api := &scriptedConfigMaps{
		create: func(*corev1.ConfigMap) (*corev1.ConfigMap, error) { return nil, errors.New("response lost") },
		get:    func(string) (*corev1.ConfigMap, error) { return stored.DeepCopy(), nil },
	}
	if _, err := RecordGrantOnce(context.Background(), api, g); err != nil || api.puts != 1 || api.reads != 1 {
		t.Fatalf("lost response qualified=%v calls=%d/%d", err, api.puts, api.reads)
	}
	api.get = func(string) (*corev1.ConfigMap, error) { return nil, errors.New("read failed") }
	if _, err := RecordGrantOnce(context.Background(), api, g); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("missing direct read incorrectly qualified: %v", err)
	}
	if _, err := ObserveGrant(context.Background(), api, g); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("restart read incorrectly treated absence as no effect: %v", err)
	}
	api.create = func(*corev1.ConfigMap) (*corev1.ConfigMap, error) { return stored.DeepCopy(), nil }
	replaced := serverEffect(t, proposal, "replacement-receipt-uid")
	api.get = func(string) (*corev1.ConfigMap, error) { return replaced.DeepCopy(), nil }
	if _, err := RecordGrantOnce(context.Background(), api, g); !errors.Is(err, ErrChanged) {
		t.Fatalf("same-name replacement qualified: %v", err)
	}
}

func TestEffectReceiptTransportCannotRewriteExpectedPayload(t *testing.T) {
	g := exampleGrantIntent(t)
	api := &scriptedConfigMaps{
		create: func(proposal *corev1.ConfigMap) (*corev1.ConfigMap, error) {
			proposal.Data["payload"] = `{"replacement":true}`
			return nil, errors.New("response lost")
		},
		get: func(string) (*corev1.ConfigMap, error) {
			proposal, err := NewGrantConfigMap(g)
			if err != nil {
				t.Fatal(err)
			}
			proposal.Data["payload"] = `{"replacement":true}`
			return serverEffect(t, proposal, "changed-receipt-uid"), nil
		},
	}
	if _, err := RecordGrantOnce(context.Background(), api, g); !errors.Is(err, ErrChanged) {
		t.Fatalf("mutated transport payload qualified: %v", err)
	}
}

func TestEffectReceiptRejectsFactMutation(t *testing.T) {
	g := exampleGrantIntent(t)
	p := examplePodCreateIntent(t)
	grant, _ := NewGrantConfigMap(g)
	pod, _ := NewPodCreateConfigMap(p)
	for _, tc := range []struct {
		name    string
		base    *corev1.ConfigMap
		qualify func(*corev1.ConfigMap) (EffectWitness, error)
		mutate  func(*corev1.ConfigMap)
	}{
		{name: "grant wrong namespace UID", base: grant, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyGrant(g, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.Data["payload"] = strings.Replace(cm.Data["payload"], g.Build.NamespaceUID, "replacement-uid", 1)
			}},
		{name: "grant wrong source", base: grant, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyGrant(g, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.Data["payload"] = strings.Replace(cm.Data["payload"], g.Build.SourceDigest, "sha256:"+strings.Repeat("f", 64), 1)
			}},
		{name: "grant wrong worker pool", base: grant, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyGrant(g, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.Data["payload"] = strings.Replace(cm.Data["payload"], g.Build.WorkerPoolID, "other-pool", 1)
			}},
		{name: "pod wrong attempt", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.Data["payload"] = strings.Replace(cm.Data["payload"], p.PodAttemptNonce, strings.Repeat("f", 32), 1)
			}},
		{name: "pod wrong grant UID", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.Data["payload"] = strings.Replace(cm.Data["payload"], p.GrantReceipt.UID, "replacement-uid", 1)
			}},
		{name: "pod changed name", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) { cm.Name = "other" }},
		{name: "pod owner reference", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) {
				cm.OwnerReferences = []metav1.OwnerReference{{UID: "build-uid", Name: p.Build.BuildName}}
			}},
		{name: "pod extra data", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) { cm.Data["other"] = "value" }},
		{name: "pod mutable", base: pod, qualify: func(cm *corev1.ConfigMap) (EffectWitness, error) { return QualifyPodCreate(p, cm) },
			mutate: func(cm *corev1.ConfigMap) { cm.Immutable = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := serverEffect(t, tc.base, "original-receipt-uid")
			tc.mutate(observed)
			if _, err := tc.qualify(observed); !errors.Is(err, ErrChanged) {
				t.Fatalf("mutated receipt qualified: %v", err)
			}
		})
	}
}

func TestInvalidEffectReceiptsNeverReachAPI(t *testing.T) {
	g := exampleGrantIntent(t)
	g.AdmissionMode = "queued"
	g.QueueReceipt = nil
	api := &scriptedConfigMaps{}
	if _, err := RecordGrantOnce(context.Background(), api, g); !errors.Is(err, ErrInvalid) || api.puts != 0 {
		t.Fatalf("invalid grant called API: %v, puts=%d", err, api.puts)
	}
	p := examplePodCreateIntent(t)
	p.BuildRequestID = "other-build-uid"
	if _, err := RecordPodCreateOnce(context.Background(), api, p); !errors.Is(err, ErrInvalid) || api.puts != 0 {
		t.Fatalf("invalid pod attempt called API: %v, puts=%d", err, api.puts)
	}
}

func TestDigestDataCanonicalMapOrder(t *testing.T) {
	a, err := DigestData(map[string]string{"a": "one", "b": "two"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := DigestData(map[string]string{"b": "two", "a": "one"})
	if err != nil || a != b {
		t.Fatalf("canonical data digest mismatch: %q / %q / %v", a, b, err)
	}
}
