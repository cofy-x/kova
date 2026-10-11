package recoveryoperator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoverydrain"
	"github.com/cofy-x/kova/internal/service/recoveryinventory"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type stageFixture struct {
	*operatorFixture
	p              StagePins
	path, pin, out string
	intercept      func(http.ResponseWriter, *http.Request) bool
}

func newStageFixture(t *testing.T, drain bool) *stageFixture {
	t.Helper()
	f := &stageFixture{operatorFixture: newOperatorFixture(t)}
	f.objects = map[string]map[string]any{}
	f.p = StagePins{Version: "1", Action: "collect-exact", Connection: f.pins.Connection, MaxAPICalls: 128}
	e := f.pins.Plan.Payload.Source
	for _, n := range []struct{ name, uid string }{{"kube-system", f.p.Connection.SystemNamespaceUID}, {e.Namespace, e.NamespaceUID}, {e.ReceiptNamespace, e.ReceiptNamespaceUID}} {
		f.objects["/api/v1/namespaces/"+n.name] = map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": n.name, "uid": n.uid, "resourceVersion": "1"}, "status": map[string]any{"phase": "Active"}}
	}
	f.p.Inventory = &recoveryinventory.Input{IncidentID: "fixture-incident", SystemNamespaceUID: f.p.Connection.SystemNamespaceUID,
		Runner: recoveryinventory.NamespacePin{Name: e.Namespace, UID: e.NamespaceUID}, Receipts: recoveryinventory.NamespacePin{Name: e.ReceiptNamespace, UID: e.ReceiptNamespaceUID},
		BarrierEvidenceDigest: fixtureHash("a"), ObjectLimit: 16, ObjectByteLimit: 4096, ArchiveByteLimit: 32768}
	if drain {
		f.makeDrain(t, e)
	}
	f.before = func(w http.ResponseWriter, r *http.Request) bool {
		if f.intercept != nil && f.intercept(w, r) {
			return true
		}
		parts := strings.Split(r.URL.Path, "/")
		resource := parts[len(parts)-1]
		kind := map[string]string{"configmaps": "ConfigMap", "pods": "Pod", "kovabuilds": "KovaBuild"}[resource]
		if kind != "" {
			if r.Method == http.MethodGet {
				items := []any{}
				for path, obj := range f.objects {
					if strings.HasPrefix(path, r.URL.Path+"/") {
						items = append(items, obj)
					}
				}
				apiVersion := "v1"
				if kind == "KovaBuild" {
					apiVersion = kovav1.Group + "/" + kovav1.Version
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": apiVersion, "kind": kind + "List", "metadata": map[string]any{"resourceVersion": "2"}, "items": items})
				return true
			}
			if r.Method == http.MethodPost {
				var obj map[string]any
				if json.NewDecoder(r.Body).Decode(&obj) != nil {
					t.Error("bad create body")
					w.WriteHeader(400)
					return true
				}
				m := obj["metadata"].(map[string]any)
				m["uid"] = "new-" + kind
				m["resourceVersion"] = "3"
				m["creationTimestamp"] = "2026-10-11T00:00:00Z"
				f.objects[r.URL.Path+"/"+m["name"].(string)] = obj
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(obj)
				return true
			}
		}
		if r.Method == http.MethodPatch {
			var operations []map[string]any
			if json.NewDecoder(r.Body).Decode(&operations) != nil || len(operations) < 3 {
				t.Error("bad hold patch")
				w.WriteHeader(400)
				return true
			}
			obj := f.objects[r.URL.Path]
			m := obj["metadata"].(map[string]any)
			if operations[0]["value"] != m["uid"] || operations[1]["value"] != m["resourceVersion"] {
				t.Error("wrong UID/RV")
				w.WriteHeader(409)
				return true
			}
			if len(operations) == 4 && !reflect.DeepEqual(operations[2]["value"], m["finalizers"]) {
				t.Error("wrong original finalizers")
				w.WriteHeader(409)
				return true
			}
			m["finalizers"] = operations[len(operations)-1]["value"]
			m["resourceVersion"] = "4"
			_ = json.NewEncoder(w).Encode(obj)
			return true
		}
		if _, exists := f.objects[r.URL.Path]; !exists {
			name := parts[len(parts)-1]
			resource = parts[len(parts)-2]
			group := ""
			if strings.HasPrefix(r.URL.Path, "/apis/") {
				group = kovav1.Group
			}
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: 404,
				Details: &metav1.StatusDetails{Name: name, Group: group, Kind: resource}})
			return true
		}
		return false
	}
	dir := t.TempDir()
	if os.Chmod(dir, 0700) != nil {
		t.Fatal("private directory")
	}
	f.path = filepath.Join(dir, "stage-pins.json")
	f.out = filepath.Join(dir, "journal.jsonl")
	f.save(t)
	return f
}

func (f *stageFixture) save(t *testing.T) { f.pin = digest(writeFixture(t, f.path, f.p)) }

// All signing below is mock-only; no physical assertion is proven by this fixture.
func (f *stageFixture) makeDrain(t *testing.T, e recoverypermit.EpochIdentity) {
	b := &kovav1.KovaBuild{TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"}, ObjectMeta: metav1.ObjectMeta{Namespace: e.Namespace, Name: "build-one", UID: "build-original", ResourceVersion: "1", Finalizers: []string{"foreign.example/retain"}},
		Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "alice", UID: "alice-uid"}, Targets: []kovav1.KovaBuildTargetSpec{{Target: "registry.test/app:v1", Platform: "linux/amd64"}}, Source: kovav1.KovaBuildSourceSpec{URI: "https://source.test/archive", Digest: fixtureHash("b")}, Build: kovav1.KovaBuildOptions{Format: "oci"}}}
	requestDigest, err := queueadmission.DigestBuild(b)
	if err != nil {
		t.Fatal(err)
	}
	q := recoveryreceipt.QueueIntent{Namespace: e.Namespace, NamespaceUID: e.NamespaceUID, ReceiptNamespace: e.ReceiptNamespace, ReceiptNamespaceUID: e.ReceiptNamespaceUID,
		GenesisName: e.GenesisName, GenesisUID: e.GenesisUID, Generation: e.Generation, ActiveLedgerUID: e.ActiveLedgerUID, QueueLedgerUID: e.QueueLedgerUID,
		BuildName: b.Name, RequesterName: "alice", RequesterHash: queueadmission.HashRequester("alice"), RequesterUID: "alice-uid", RequestDigest: requestDigest, SourceDigest: b.Spec.Source.Digest, QueueNonce: strings.Repeat("1", 32)}
	cm, err := recoveryreceipt.NewQueueConfigMap(q)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID, cm.ResourceVersion = "receipt-original", "1"
	cm.APIVersion, cm.Kind = "v1", "ConfigMap"
	cmDigest, err := recoveryreceipt.DigestData(cm.Data)
	if err != nil {
		t.Fatal(err)
	}
	stop := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Namespace: e.Namespace, Name: "stop-incident", UID: "stop-original", ResourceVersion: "1"}, Immutable: new(true), Data: map[string]string{"incident": "fixture-incident"}}
	stopDigest, err := recoveryreceipt.DigestData(stop.Data)
	if err != nil {
		t.Fatal(err)
	}
	stopRef := recoverypermit.StopIntentRef{Namespace: e.Namespace, Name: stop.Name, UID: string(stop.UID), DataDigest: stopDigest}
	refs := []recoverypermit.ReceiptRef{{Kind: "queue", Namespace: e.ReceiptNamespace, Name: cm.Name, UID: string(cm.UID), DataDigest: cmDigest}}
	setDigest, err := recoverypermit.DigestReceiptSet(e, refs)
	if err != nil {
		t.Fatal(err)
	}
	routes := []recoverypermit.KubeObjectRef{{Namespace: "control", Name: "old-route", UID: "route-original"}}
	deployments := []recoverypermit.KubeObjectRef{{Namespace: "control", Name: "old-service", UID: "deployment-original"}}
	processes := []recoverypermit.ServiceProcessRef{{Pod: recoverypermit.KubeObjectRef{Namespace: "control", Name: "old-service-a", UID: "service-original"}, ContainerID: "containerd://old", ProcessID: "process-original"}}
	payload := recoverypermit.DrainPayload{Version: "1", Audience: "kova-recovery-drain-v1", Action: "drain-discard-only", Issuer: "fixture-authority", KeyID: "fixture-key", IncidentID: "fixture-incident", StopIntent: stopRef, Epoch: e, ReceiptCount: 1, ReceiptSetDigest: setDigest,
		WorkerRetirement:  recoverypermit.WorkerPoolRetired{Type: "worker-pool-retired-v1", PoolID: e.WorkerPoolID, Epoch: e.Generation, WorkerIDs: []string{"worker-original"}, CapacitySlots: 1, Disposition: "terminated", EvidenceDigest: fixtureHash("c"), Assertion: "no-old-buildkit-execution-or-network-path-can-resume-v1", CutoffAt: "2020-01-01T00:00:00Z"},
		OldWritersStopped: recoverypermit.OldWritersStopped{Type: "old-writers-stopped-v1", StopIntent: stopRef, IngressRoutes: routes, ControlDeployments: deployments, ServiceProcesses: processes, EvidenceDigest: fixtureHash("d"), Assertion: "ingress-frozen-old-processes-joined-no-uninstrumented-writers-or-post-confirmation-effects-v1", RetiredAt: "2020-01-01T00:00:00Z"}, IssuedAt: "2020-01-01T00:00:01Z"}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{3}, 32))
	rawPayload, _ := json.Marshal(payload)
	permit, _ := json.Marshal(recoverypermit.Envelope{Payload: payload, Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(private, append([]byte("KOVA-RECOVERY-DRAIN-PERMIT-V1\x00"), rawPayload...)))})
	f.p.Action = "drain-exact"
	f.p.Inventory = nil
	f.p.Drain = &recoverydrain.Input{Permit: permit, Expected: recoverypermit.Expectation{PermitDigest: digest(permit), IncidentID: payload.IncidentID, StopIntent: stopRef, Epoch: e, Receipts: refs, WorkerIDs: []string{"worker-original"}, CapacitySlots: 1, IngressRoutes: routes, ControlDeployments: deployments, ServiceProcesses: processes}, MaxLiabilities: 8, Queues: []recoveryreceipt.QueueIntent{q}}
	f.p.Roots = []PublicKeyPin{{Issuer: "fixture-authority", KeyID: "fixture-key", PublicKeyHex: hex.EncodeToString(private.Public().(ed25519.PublicKey))}}
	for _, o := range []client.Object{b, cm, stop} {
		g, err := stageGVK(o)
		if err != nil {
			t.Fatal(err)
		}
		u, err := objectStageMap(o, g)
		if err != nil {
			t.Fatal(err)
		}
		path, _ := stageAddress(g, client.ObjectKeyFromObject(o))
		f.objects[path] = u.Object
	}
}

func (f *stageFixture) prepared(t *testing.T) preparedStage {
	t.Helper()
	p, err := prepareStage(context.Background(), f.path, f.pin, f.p.Action)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestStageDefaultPlanHasZeroAPIAndNoJournal(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(map[bool]string{false: "collect", true: "drain"}[drain], func(t *testing.T) {
			f := newStageFixture(t, drain)
			var out bytes.Buffer
			app := NewCLIApp()
			app.Writer = &out
			app.ErrWriter = io.Discard
			if err := app.Run([]string{"kova-recovery", f.p.Action, "--stage-pins", f.path, "--stage-pins-digest", f.pin, "--output", f.out}); err != nil {
				t.Fatal(err)
			}
			if len(f.requests) != 0 || !strings.Contains(out.String(), "local-preflight-no-api") {
				t.Fatal("plan connected")
			}
			if _, err := os.Stat(f.out); !os.IsNotExist(err) {
				t.Fatal("plan reserved output")
			}
			if err := app.Run([]string{"kova-recovery", f.p.Action, "--stage-pins", f.path, "--stage-pins-digest", f.pin, "--execute", "--ack", "wrong", "--output", f.out}); err == nil || len(f.requests) != 0 {
				t.Fatal("missing exact ack accepted")
			}
		})
	}
}

func TestCollectStageCallsLibraryAndPersistsPrivateBodies(t *testing.T) {
	f := newStageFixture(t, false)
	f.objects["/api/v1/namespaces/old-runner/configmaps/unlabeled"] = map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "old-runner", "name": "unlabeled", "uid": "evidence-original", "resourceVersion": "4"}, "data": map[string]any{"private": "never-ordinary-output"}, "futureField": map[string]any{"preserved": true}}
	result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
	if err != nil || result.Stage != "inventory-observed-not-retired" || result.APICalls != 14 || result.IncidentClosed || result.CapacityReleased || result.SuccessorCreated {
		t.Fatalf("wrong result: %+v %v", result, err)
	}
	raw, err := os.ReadFile(f.out)
	if err != nil || digest(raw) != result.OutputDigest || !bytes.Contains(raw, []byte("never-ordinary-output")) || !bytes.Contains(raw, []byte("futureField")) {
		t.Fatal("lost private body/readback")
	}
	info, _ := os.Stat(f.out)
	if info.Mode().Perm() != 0600 {
		t.Fatal("public output")
	}
	for _, r := range f.requests {
		if !strings.HasPrefix(r, "GET ") {
			t.Fatal("collector mutated")
		}
	}
	count := len(f.requests)
	if _, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out); err == nil || len(f.requests) != count {
		t.Fatal("existing output replayed")
	}
}

func TestDrainStageCallsLibraryAndOnlyHoldsOriginal(t *testing.T) {
	f := newStageFixture(t, true)
	path := "/apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds/build-one"
	before := digestCanonical(f.objects[path]["spec"])
	result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
	if err != nil || result.Stage != "occupied-not-drained" || result.CapacityReleased {
		t.Fatalf("wrong result: %+v %v", result, err)
	}
	writes := 0
	for _, r := range f.requests {
		if !strings.HasPrefix(r, "GET ") {
			writes++
			if r != "PATCH "+path {
				t.Fatal("unexpected mutation", r)
			}
		}
	}
	if writes != 1 || digestCanonical(f.objects[path]["spec"]) != before {
		t.Fatal("not a hold-only update")
	}
	finalizers := f.objects[path]["metadata"].(map[string]any)["finalizers"].([]any)
	if len(finalizers) != 2 || finalizers[0] != "foreign.example/retain" || finalizers[1] != recoveryHold {
		t.Fatal("foreign finalizer lost")
	}
	raw, _ := os.ReadFile(f.out)
	if digest(raw) != result.OutputDigest || !bytes.Contains(raw, []byte("original-retained")) {
		t.Fatal("report not persisted")
	}
}

func TestDrainAbsentStageCreatesOneAttemptAndOneInertBuild(t *testing.T) {
	f := newStageFixture(t, true)
	delete(f.objects, "/apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds/build-one")
	result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
	if err != nil || result.Stage != "occupied-not-drained" {
		t.Fatalf("%+v %v", result, err)
	}
	writes := []string{}
	for _, r := range f.requests {
		if !strings.HasPrefix(r, "GET ") {
			writes = append(writes, r)
		}
	}
	if !reflect.DeepEqual(writes, []string{"POST /api/v1/namespaces/old-runner/configmaps", "POST /apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds"}) {
		t.Fatal("wrong once-only mutations", writes)
	}
}

func TestStageUnknownCreateStopsWithoutPosthocReadback(t *testing.T) {
	for _, body := range []string{"", "<html>proxy</html>", `{"apiVersion":"v1","kind":"Status","status":"Success"}`} {
		t.Run(body, func(t *testing.T) {
			f := newStageFixture(t, true)
			delete(f.objects, "/apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds/build-one")
			f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost {
					w.WriteHeader(201)
					_, _ = io.WriteString(w, body)
					return true
				}
				return false
			}
			result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
			if err == nil || result.Stage != "unknown-preserve" || result.OutputDigest == "" {
				t.Fatal("unknown was accepted")
			}
			if f.requests[len(f.requests)-1] != "POST /api/v1/namespaces/old-runner/configmaps" {
				t.Fatal("unknown continued", f.requests)
			}
		})
	}
}

func TestStageReadUnknownStopsAndRetainsJournal(t *testing.T) {
	for _, fault := range []string{"html404", "namespace-replaced", "paginated", "remaining-items", "missing-items", "oversize", "duplicate"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, false)
			f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/api/v1/namespaces/old-runner/configmaps" {
					return false
				}
				switch fault {
				case "html404":
					w.WriteHeader(404)
					_, _ = io.WriteString(w, "<html>proxy</html>")
				case "namespace-replaced":
					f.objects["/api/v1/namespaces/old-runner"]["metadata"].(map[string]any)["uid"] = "replacement"
					return false
				case "paginated":
					_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"2","continue":"more"},"items":[]}`)
				case "remaining-items":
					_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"2","remainingItemCount":1},"items":[]}`)
				case "missing-items":
					_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"2"}}`)
				case "oversize":
					w.Header().Set("Content-Length", "20000000")
					_, _ = io.WriteString(w, "{}")
				case "duplicate":
					_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMapList","kind":"ConfigMapList","metadata":{"resourceVersion":"2"},"items":[]}`)
				}
				return true
			}
			result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
			if err == nil || result.Stage != "unknown-preserve" || result.OutputDigest == "" {
				t.Fatal("unknown accepted")
			}
			if fault != "namespace-replaced" && f.requests[len(f.requests)-1] != "GET /api/v1/namespaces/old-runner/configmaps" {
				t.Fatal("read unknown continued", f.requests)
			}
		})
	}
}

func TestStageIndependentPinsAndPrivateOutput(t *testing.T) {
	for _, fault := range []string{"digest", "unknown-field", "duplicate", "namespace-alias", "uid-alias", "roots-missing"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, fault == "roots-missing")
			switch fault {
			case "digest":
				f.pin = fixtureHash("f")
			case "unknown-field", "duplicate":
				raw, _ := os.ReadFile(f.path)
				suffix := `,"foreign":true}`
				if fault == "duplicate" {
					suffix = `,"version":"1"}`
				}
				raw = append(raw[:len(raw)-1], suffix...)
				if os.WriteFile(f.path, raw, 0600) != nil {
					t.Fatal("write")
				}
				f.pin = digest(raw)
			case "namespace-alias":
				f.p.Inventory.Runner.Name = "kube-system"
				f.save(t)
			case "uid-alias":
				f.p.Inventory.Runner.UID = f.p.Inventory.Receipts.UID
				f.save(t)
			case "roots-missing":
				f.p.Roots = nil
				f.save(t)
			}
			if _, err := prepareStage(context.Background(), f.path, f.pin, f.p.Action); err == nil || len(f.requests) != 0 {
				t.Fatal("unqualified pins accepted")
			}
		})
	}
	for _, fault := range []string{"public-parent", "symlink", "existing", "ca-mismatch"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, false)
			switch fault {
			case "public-parent":
				if os.Chmod(filepath.Dir(f.out), 0755) != nil {
					t.Fatal("chmod")
				}
			case "symlink":
				if os.Symlink(f.path, f.out) != nil {
					t.Fatal("symlink")
				}
			case "existing":
				writeFixture(t, f.out, map[string]string{"original": "retain"})
			case "ca-mismatch":
				f.p.Connection.CASHA256 = fixtureHash("f")
				f.save(t)
			}
			result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
			if err == nil || result.Stage != "unknown-preserve" || len(f.requests) != 0 {
				t.Fatal("unsafe output/connection accepted")
			}
		})
	}
}

func TestStageAdapterClosedScopeAndJournalBeforeWire(t *testing.T) {
	for _, fault := range []string{"closed-journal", "budget", "foreign-name", "spec-edit", "uid-edit", "finalizer-remove", "response-drift"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, true)
			p := f.prepared(t)
			j, err := reserveJournal(f.out, p)
			if err != nil {
				t.Fatal(err)
			}
			defer j.f.Close()
			a, err := newStageAPI(p.pins, f.caPath, f.credentialsPath, j)
			if err != nil {
				t.Fatal(err)
			}
			defer a.base.http.CloseIdleConnections()
			var obj kovav1.KovaBuild
			if fault == "closed-journal" {
				_ = j.f.Close()
			}
			if fault == "budget" {
				a.maxCalls = 0
			}
			name := "build-one"
			if fault == "foreign-name" {
				name = "unlisted"
			}
			err = a.Get(context.Background(), client.ObjectKey{Namespace: "old-runner", Name: name}, &obj)
			if fault == "closed-journal" || fault == "budget" || fault == "foreign-name" {
				if err == nil || len(f.requests) != 0 {
					t.Fatal("unqualified request sent")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			obj.Finalizers = append(obj.Finalizers, recoveryHold)
			switch fault {
			case "spec-edit":
				obj.Spec.Build.Concurrency = 9
			case "uid-edit":
				obj.UID = "foreign"
			case "finalizer-remove":
				obj.Finalizers = []string{recoveryHold}
			case "response-drift":
				f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodPatch {
						copy := &unstructured.Unstructured{Object: f.objects[r.URL.Path]}
						copy = copy.DeepCopy()
						copy.SetFinalizers(obj.Finalizers)
						_ = unstructured.SetNestedField(copy.Object, "Changed", "status", "phase")
						_ = json.NewEncoder(w).Encode(copy.Object)
						return true
					}
					return false
				}
			}
			count := len(f.requests)
			if a.Update(context.Background(), &obj) == nil {
				t.Fatal("unsafe hold accepted")
			}
			if fault != "response-drift" && len(f.requests) != count {
				t.Fatal("unsafe hold sent")
			}
			count = len(f.requests)
			if a.Get(context.Background(), client.ObjectKey{Namespace: "old-runner", Name: "build-one"}, &kovav1.KovaBuild{}) == nil || len(f.requests) != count {
				t.Fatal("unknown adapter continued")
			}
		})
	}
}

func TestStageAlreadyExistsRequiresExactQualifiedStatus(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "foreign"}[foreign], func(t *testing.T) {
			f := newStageFixture(t, true)
			p := f.prepared(t)
			j, err := reserveJournal(f.out, p)
			if err != nil {
				t.Fatal(err)
			}
			defer j.f.Close()
			a, err := newStageAPI(p.pins, f.caPath, f.credentialsPath, j)
			if err != nil {
				t.Fatal(err)
			}
			defer a.base.http.CloseIdleConnections()
			e := p.pins.Drain.Expected.Epoch
			name := recoverydrain.OccupancyAttemptName(e, "KovaBuild", "build-one")
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: e.Namespace, Name: name, Labels: map[string]string{"kova.cofy.dev/recovery-occupancy-attempt": "v1"}, Finalizers: []string{recoveryHold}}, Immutable: new(true), Data: map[string]string{"version": "1", "namespaceUid": e.NamespaceUID, "genesisUid": e.GenesisUID, "kind": "KovaBuild", "name": "build-one"}}
			f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodPost {
					return false
				}
				observedName := name
				if foreign {
					observedName = "foreign"
				}
				status := apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, observedName).ErrStatus
				status.APIVersion, status.Kind = "v1", "Status"
				w.WriteHeader(409)
				_ = json.NewEncoder(w).Encode(status)
				return true
			}
			err = a.Create(context.Background(), cm)
			if foreign && (!a.failed || apierrors.IsAlreadyExists(err)) || !foreign && (!apierrors.IsAlreadyExists(err) || a.failed) {
				t.Fatalf("wrong conflict qualification: %v", err)
			}
		})
	}
}

func TestStageJournalIdentityAndFinalReadback(t *testing.T) {
	for _, fault := range []string{"permissions", "same-size-corruption", "replacement"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, false)
			j, err := reserveJournal(f.out, f.prepared(t))
			if err != nil {
				t.Fatal(err)
			}
			defer j.f.Close()
			switch fault {
			case "permissions":
				if os.Chmod(f.out, 0644) != nil {
					t.Fatal("chmod")
				}
			case "same-size-corruption":
				if _, err := j.f.WriteAt([]byte("X"), 0); err != nil {
					t.Fatal(err)
				}
			case "replacement":
				if os.Rename(f.out, f.out+".retained") != nil {
					t.Fatal("rename")
				}
				writeFixture(t, f.out, map[string]string{"foreign": "file"})
			}
			if _, err := j.finish(); err == nil {
				t.Fatal("corrupt output qualified")
			}
			if fault != "same-size-corruption" && j.append(map[string]string{"must": "not append"}) == nil {
				t.Fatal("changed output accepted")
			}
		})
	}
}

func TestStageUnknownPatchStopsAndPreservesChargeReport(t *testing.T) {
	f := newStageFixture(t, true)
	f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPatch {
			return false
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "<html>unknown</html>")
		return true
	}
	result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
	if err == nil || result.Stage != "unknown-preserve" || result.CapacityReleased || result.IncidentClosed || result.OutputDigest == "" {
		t.Fatal("unknown patch qualified")
	}
	if !strings.HasPrefix(f.requests[len(f.requests)-1], "PATCH ") {
		t.Fatal("unknown patch followed by readback")
	}
}

func TestStagePodCreateRequiresInertQualifiedShape(t *testing.T) {
	for _, fault := range []string{"none", "schedule-gate-removed", "foreign-uid", "foreign-namespace", "wrong-gvk", "dropped-command", "extra-container", "extra-env"} {
		t.Run(fault, func(t *testing.T) {
			f := newStageFixture(t, true)
			p := f.prepared(t)
			// Fixture-only adapter scope: authority/receipt linking is separately
			// enforced by Prepare and the existing Drain library before invocation.
			p.pins.Drain.PodCreates = []recoveryreceipt.PodCreateIntent{{PodName: "inert-one"}}
			j, err := reserveJournal(f.out, p)
			if err != nil {
				t.Fatal(err)
			}
			defer j.f.Close()
			a, err := newStageAPI(p.pins, f.caPath, f.credentialsPath, j)
			if err != nil {
				t.Fatal(err)
			}
			defer a.base.http.CloseIdleConnections()
			no := false
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "old-runner", Name: "inert-one", Labels: map[string]string{tombstoneMarker: "v1"}, Annotations: map[string]string{tombstoneMarker: p.pins.Drain.Expected.IncidentID, "kova.cofy.dev/recovery-epoch-uid": p.pins.Drain.Expected.Epoch.NamespaceUID}, Finalizers: []string{recoveryHold}},
				Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, SchedulingGates: []corev1.PodSchedulingGate{{Name: recoveryHold}}, NodeSelector: map[string]string{tombstoneMarker: "never"}, AutomountServiceAccountToken: &no, Containers: []corev1.Container{{Name: "inert", Image: "registry.invalid/kova-recovery-tombstone:never", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/false"}}}}}
			if fault == "extra-env" {
				pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "UNAUTHORIZED", Value: "true"}}
			}
			if fault != "none" && fault != "extra-env" {
				f.intercept = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method != http.MethodPost {
						return false
					}
					var m map[string]any
					if json.NewDecoder(r.Body).Decode(&m) != nil {
						t.Fatal("decode")
					}
					u := &unstructured.Unstructured{Object: m}
					u.SetUID("new-Pod")
					u.SetResourceVersion("3")
					switch fault {
					case "schedule-gate-removed":
						unstructured.RemoveNestedField(u.Object, "spec", "schedulingGates")
					case "foreign-uid":
						u.SetUID("")
					case "foreign-namespace":
						u.SetNamespace("foreign")
					case "wrong-gvk":
						u.SetKind("ConfigMap")
					case "dropped-command", "extra-container":
						containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "containers")
						if fault == "dropped-command" {
							delete(containers[0].(map[string]any), "command")
						} else {
							containers = append(containers, containers[0])
						}
						_ = unstructured.SetNestedSlice(u.Object, containers, "spec", "containers")
					}
					w.WriteHeader(201)
					_ = json.NewEncoder(w).Encode(u.Object)
					return true
				}
			}
			err = a.Create(context.Background(), pod)
			if fault == "none" {
				if err != nil || pod.UID != "new-Pod" {
					t.Fatal("inert create rejected", err)
				}
				return
			}
			if err == nil || !a.failed {
				t.Fatal("unsafe Pod accepted")
			}
			if fault == "extra-env" && len(f.requests) != 0 {
				t.Fatal("unsafe proposal sent")
			}
			count := len(f.requests)
			if a.Get(context.Background(), client.ObjectKey{Namespace: "old-runner", Name: "inert-one"}, &corev1.Pod{}) == nil || count != len(f.requests) {
				t.Fatal("unknown Pod acknowledged by later read")
			}
		})
	}
}

func TestStagePinnedCallBoundsNeverAutoExpand(t *testing.T) {
	for _, calls := range []int{13, 14, maxStageAPICalls, maxStageAPICalls + 1} {
		t.Run(strconv.Itoa(calls), func(t *testing.T) {
			f := newStageFixture(t, false)
			f.p.MaxAPICalls = calls
			f.save(t)
			_, err := prepareStage(context.Background(), f.path, f.pin, f.p.Action)
			if (err == nil) != (calls >= 14 && calls <= maxStageAPICalls) || len(f.requests) != 0 {
				t.Fatal("call ceiling not enforced")
			}
		})
	}
	f := newStageFixture(t, true)
	f.p.MaxAPICalls = 14
	f.save(t)
	result, err := executeStage(context.Background(), f.prepared(t), f.caPath, f.credentialsPath, f.out)
	if err == nil || result.Stage != "unknown-preserve" || result.APICalls != 14 || len(f.requests) != 14 || result.OutputDigest == "" {
		t.Fatal("budget silently expanded", result, err)
	}
}

func TestStageIntentRecordsActualDeadlineAndExplicitTimeout(t *testing.T) {
	f := newStageFixture(t, false)
	p := f.prepared(t)
	p.timeout = 45 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	result, err := executeStage(ctx, p, f.caPath, f.credentialsPath, f.out)
	if err != nil || result.Stage != "inventory-observed-not-retired" {
		t.Fatal("collect", err)
	}
	raw, _ := os.ReadFile(f.out)
	var intent struct {
		Deadline           string
		TimeoutNanoseconds int64
	}
	if json.Unmarshal(bytes.SplitN(raw, []byte("\n"), 2)[0], &intent) != nil || intent.Deadline != deadline.UTC().Format(time.RFC3339Nano) || intent.TimeoutNanoseconds != int64(45*time.Second) {
		t.Fatal("deadline/timeout not pinned to invocation")
	}
}
