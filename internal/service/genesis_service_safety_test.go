package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func validServiceProcessManifestForUnit() serviceProcessManifest {
	yes, no := true, false
	m := serviceProcessManifest{SchemaVersion: 1, SourceCommit: strings.Repeat("a", 40), TestBinarySHA256: strings.Repeat("b", 64),
		Kubeconfig: "/private/service-test.kubeconfig", KubeconfigSHA256: strings.Repeat("c", 64), Context: "kind-kova-genesis-process-service-unit",
		KubeSystemUID: "system-original", ControlPlaneID: strings.Repeat("d", 64), OldWritersStopped: &yes, CleanupOnSuccess: &no}
	for index, name := range serviceProcessCases {
		allowDelete := name == "late-active-create-after-loss"
		m.Cases = append(m.Cases, serviceProcessCase{Name: name, AllowCommittedLedgerDeletion: &allowDelete, Fixture: processCase{
			Namespace: fmt.Sprintf("kova-genesis-svc-unit-%d", index), NamespaceUID: fmt.Sprintf("runner-%d", index),
			GenesisUID: fmt.Sprintf("genesis-%d", index), ReceiptFile: fmt.Sprintf("/private/receipt-%d.json", index), ReceiptSHA256: strings.Repeat("e", 64),
			SecretNamespace: fmt.Sprintf("kova-genesis-svc-control-%d", index), SecretNamespaceUID: fmt.Sprintf("control-%d", index), SecretName: "receipt", SecretUID: fmt.Sprintf("secret-%d", index)}})
	}
	return m
}

func TestActualServiceManifestRejectsUnsafeOrIncompleteFixtures(t *testing.T) {
	if err := validateServiceProcessManifest(validServiceProcessManifestForUnit()); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*serviceProcessManifest){
		"old schema":               func(m *serviceProcessManifest) { m.SchemaVersion = 0 },
		"missing case":             func(m *serviceProcessManifest) { m.Cases = m.Cases[:6] },
		"duplicate case":           func(m *serviceProcessManifest) { m.Cases[1].Name = m.Cases[0].Name },
		"missing deletion consent": func(m *serviceProcessManifest) { m.Cases[6].AllowCommittedLedgerDeletion = nil },
		"extra deletion consent":   func(m *serviceProcessManifest) { yes := true; m.Cases[0].AllowCommittedLedgerDeletion = &yes },
		"missing stopped writers":  func(m *serviceProcessManifest) { m.OldWritersStopped = nil },
		"cleanup enabled":          func(m *serviceProcessManifest) { yes := true; m.CleanupOnSuccess = &yes },
		"unpinned binary":          func(m *serviceProcessManifest) { m.TestBinarySHA256 = "" },
		"short source":             func(m *serviceProcessManifest) { m.SourceCommit = "a890" },
		"relative kubeconfig":      func(m *serviceProcessManifest) { m.Kubeconfig = "local" },
		"default context":          func(m *serviceProcessManifest) { m.Context = "kind-default" },
		"old process namespace":    func(m *serviceProcessManifest) { m.Cases[1].Fixture.Namespace = "kova-genesis-api-proc-old" },
		"control aliases runner":   func(m *serviceProcessManifest) { m.Cases[1].Fixture.SecretNamespace = m.Cases[0].Fixture.Namespace },
		"namespace UID alias": func(m *serviceProcessManifest) {
			m.Cases[1].Fixture.NamespaceUID = m.Cases[0].Fixture.SecretNamespaceUID
		},
		"system UID alias":  func(m *serviceProcessManifest) { m.Cases[1].Fixture.NamespaceUID = m.KubeSystemUID },
		"genesis UID alias": func(m *serviceProcessManifest) { m.Cases[1].Fixture.GenesisUID = m.Cases[0].Fixture.GenesisUID },
		"secret UID alias":  func(m *serviceProcessManifest) { m.Cases[1].Fixture.SecretUID = m.Cases[0].Fixture.SecretUID },
		"old stage":         func(m *serviceProcessManifest) { m.Cases[0].Fixture.Stage = "commit" },
	} {
		t.Run(name, func(t *testing.T) {
			m := validServiceProcessManifestForUnit()
			change(&m)
			if validateServiceProcessManifest(m) == nil {
				t.Fatal("unsafe Service fixture accepted")
			}
		})
	}
}

func TestActualServiceManifestStrictDecoderAndPin(t *testing.T) {
	for _, raw := range []string{`{"context":"a","context":"b"}`, `{"unknown":true}`, `{"cases":[{"fixture":{"stage":"commit"}}]}`} {
		var parsed serviceProcessManifest
		if admissionjson.Decode([]byte(raw), &parsed, serviceProcessManifestField) == nil {
			t.Fatalf("unsafe manifest JSON accepted: %s", raw)
		}
	}
	m := validServiceProcessManifestForUnit()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	// The shared old processCase type has stage/point fields. They are not
	// part of this independent manifest schema and must not be serialized.
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, entry := range document["cases"].([]any) {
		fixture := entry.(map[string]any)["fixture"].(map[string]any)
		delete(fixture, "stage")
		delete(fixture, "point")
	}
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "matrix.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadServiceProcessManifest(path, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadServiceProcessManifest(path, strings.Repeat("f", 64)); err == nil {
		t.Fatal("changed manifest hash accepted")
	}
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadServiceProcessManifest(link, ""); err == nil {
		t.Fatal("symlinked manifest accepted")
	}
}

func unitServiceProxy(t *testing.T, mode string) (*serviceFaultProxy, *corev1.ConfigMap, *atomic.Int32) {
	t.Helper()
	receipt, cfg := genesisTestReceiptAndConfig()
	empty, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	role, key := admissioncontract.Active, admissioncontract.ActiveLedgerDataKey
	if mode == "lost-queue-create" {
		role, key = admissioncontract.Queue, admissioncontract.QueueLedgerDataKey
		empty, err = queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
		if err != nil {
			t.Fatal(err)
		}
	}
	object, err := receipt.NewLedgerObject(role, key, empty, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("proxy capability leaked upstream")
		}
		var received corev1.ConfigMap
		if json.NewDecoder(r.Body).Decode(&received) != nil {
			w.WriteHeader(400)
			return
		}
		received.UID, received.ResourceVersion = "created-original", "1"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(&received)
	}))
	t.Cleanup(backend.Close)
	c := serviceProcessCase{Fixture: processCase{Namespace: receipt.Namespace, SecretNamespace: "control", SecretName: "receipt"}}
	proxy, err := newServiceFaultProxy(&rest.Config{Host: backend.URL}, c, receipt, mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	return proxy, object, &calls
}

func unitServiceProxyClient(t *testing.T, proxy *serviceFaultProxy) kubernetes.Interface {
	t.Helper()
	client, err := kubernetes.NewForConfig(singleAttemptWrites(&rest.Config{Host: proxy.server.URL, BearerToken: proxy.token,
		ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"}}))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestActualServiceProxyDropsAcceptedResponseWithoutWriteRetry(t *testing.T) {
	for _, mode := range []string{"lost-active-create", "lost-queue-create"} {
		t.Run(mode, func(t *testing.T) {
			proxy, object, calls := unitServiceProxy(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := unitServiceProxyClient(t, proxy).CoreV1().ConfigMaps(object.Namespace).Create(ctx, object, metav1.CreateOptions{}); err == nil {
				t.Fatal("accepted API response was not dropped")
			}
			if calls.Load() != 1 {
				t.Fatalf("upstream wire calls=%d", calls.Load())
			}
			select {
			case <-proxy.fired:
			default:
				t.Fatal("accepted-response fault did not fire")
			}
			if err := proxy.check(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestActualServiceProxyDropsQualifiedPinAndCommitResponses(t *testing.T) {
	for _, stage := range []string{"active-pin", "queue-pin", "commit"} {
		for _, explicitFalse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit-false=%t", stage, explicitFalse), func(t *testing.T) {
				receipt, _ := genesisTestReceiptAndConfig()
				old := admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseInitializing}
				if stage != "active-pin" {
					old.ActiveLedgerUID = "active-original"
				}
				if stage == "commit" {
					old.QueueLedgerUID = "queue-original"
				}
				next := old
				switch stage {
				case "active-pin":
					next.ActiveLedgerUID = "active-original"
				case "queue-pin":
					next.QueueLedgerUID = "queue-original"
				case "commit":
					next.Phase = admissioncontract.PhaseCommitted
				}
				oldRaw, _ := json.Marshal(old)
				nextRaw, _ := json.Marshal(next)
				ops := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": receipt.GenesisUID}, {"op": "test", "path": "/metadata/resourceVersion", "value": "1"}, {"op": "test", "path": "/data/genesis.json", "value": string(oldRaw)}}
				if explicitFalse {
					ops = append(ops, map[string]any{"op": "test", "path": "/immutable", "value": false})
				}
				ops = append(ops, map[string]any{"op": "replace", "path": "/data/genesis.json", "value": string(nextRaw)})
				if stage == "commit" {
					ops = append(ops, map[string]any{"op": "add", "path": "/immutable", "value": true})
				}
				body, _ := json.Marshal(ops)
				var calls atomic.Int32
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					immutable := stage == "commit"
					object := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: receipt.Namespace, Name: receipt.GenesisName, UID: types.UID(receipt.GenesisUID), ResourceVersion: "2"}, Immutable: &immutable, Data: map[string]string{admissioncontract.GenesisDataKey: string(nextRaw)}}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(&object)
				}))
				defer backend.Close()
				proxy, err := newServiceFaultProxy(&rest.Config{Host: backend.URL}, serviceProcessCase{Fixture: processCase{SecretNamespace: "control", SecretName: "receipt"}}, receipt, "lost-"+stage)
				if err != nil {
					t.Fatal(err)
				}
				defer proxy.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if _, err := unitServiceProxyClient(t, proxy).CoreV1().ConfigMaps(receipt.Namespace).Patch(ctx, receipt.GenesisName, types.JSONPatchType, body, metav1.PatchOptions{}); err == nil {
					t.Fatal("persisted Patch response was not dropped")
				}
				if calls.Load() != 1 {
					t.Fatalf("Patch wire attempts=%d", calls.Load())
				}
				if err := proxy.check(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestActualServiceProxyHeldCreateSurvivesCanceledCallerAndForwardsOnce(t *testing.T) {
	proxy, object, calls := unitServiceProxy(t, "late-active-create")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	client := unitServiceProxyClient(t, proxy)
	go func() {
		_, err := client.CoreV1().ConfigMaps(object.Namespace).Create(ctx, object, metav1.CreateOptions{})
		done <- err
	}()
	select {
	case <-proxy.fired:
	case <-ctx.Done():
		t.Fatal("held Create not captured")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("held caller completed unexpectedly")
		}
	case <-time.After(time.Second):
		t.Fatal("held caller did not cancel")
	}
	if calls.Load() != 0 {
		t.Fatal("canceling caller auto-forwarded held Create")
	}
	forward, finish := context.WithTimeout(t.Context(), 5*time.Second)
	defer finish()
	code, uid, err := proxy.forwardHeld(forward)
	if err != nil || code != http.StatusCreated || uid != "created-original" || calls.Load() != 1 {
		t.Fatalf("held forward code=%d uid=%s calls=%d error=%v", code, uid, calls.Load(), err)
	}
	if _, _, err := proxy.forwardHeld(forward); err == nil || calls.Load() != 1 {
		t.Fatal("held Create had a second forwarding authority")
	}
	if err := proxy.check(); err != nil {
		t.Fatal(err)
	}
}

func TestActualServiceProxyRejectsUnapprovedEffectsAndBroadReads(t *testing.T) {
	proxy, object, calls := unitServiceProxy(t, "")
	for _, path := range []string{
		"/api/v1/namespaces/" + object.Namespace + "/pods", "/api/v1/namespaces/" + object.Namespace + "/pods/p/exec",
		"/apis/kova.cofy.dev/v1alpha1/namespaces/" + object.Namespace + "/kovabuilds", "/api/v1/namespaces/default/configmaps",
		"/api/v1/namespaces/" + object.Namespace + "/configmaps/" + admissioncontract.ActiveLedgerName,
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			req, _ := http.NewRequest(method, proxy.server.URL+path, bytes.NewBufferString(`{}`))
			if _, allowed := proxy.requestStage(req, []byte(`{}`)); allowed {
				t.Fatalf("unapproved effect allowed: %s %s", method, path)
			}
		}
	}
	for _, path := range []string{"/api/v1/namespaces/" + object.Namespace + "/configmaps", "/api/v1/namespaces/control/secrets", "/api/v1/namespaces/" + object.Namespace + "/pods/p/exec"} {
		req, _ := http.NewRequest(http.MethodGet, proxy.server.URL+path, nil)
		if _, allowed := proxy.requestStage(req, nil); allowed {
			t.Fatalf("unapproved read/exec allowed: %s", path)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, proxy.server.URL+"/api/v1/namespaces/"+object.Namespace+"/pods", bytes.NewBufferString(`{}`))
	req.Header.Set("Authorization", "Bearer "+proxy.token)
	response, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = response.Body.Close()
	}
	if calls.Load() != 0 || proxy.check() == nil {
		t.Fatal("forbidden Pod operation reached upstream or did not fail gate")
	}
}

func TestActualServiceProxyRejectsForgedGenesisPatch(t *testing.T) {
	proxy, _, _ := unitServiceProxy(t, "")
	for _, body := range []string{`[]`, `[{"op":"replace","path":"/data/genesis.json","value":"{}"}]`,
		`[{"op":"test","path":"/metadata/uid","value":"wrong"},{"op":"test","path":"/metadata/resourceVersion","value":"1"},{"op":"test","path":"/data/genesis.json","value":"{}"},{"op":"replace","path":"/data/genesis.json","value":"{}"}]`} {
		if _, allowed := proxy.genesisPatchStage([]byte(body)); allowed {
			t.Fatal("unqualified Genesis mutation accepted")
		}
	}
}
