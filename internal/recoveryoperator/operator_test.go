package recoveryoperator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type operatorFixture struct {
	files                   Files
	pins                    Pins
	caPath, credentialsPath string
	server                  *httptest.Server
	mu                      sync.Mutex
	requests                []string
	objects                 map[string]map[string]any
	before                  func(http.ResponseWriter, *http.Request) bool
}

func fixtureHash(c string) string { return "sha256:" + strings.Repeat(c, 64) }
func writeFixture(t *testing.T, path string, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return raw
}

// Signing is strictly a deterministic fixture helper. No issuer, physical
// retirement or archive durability is established by these mock signatures.
func fixtureSign(t *testing.T, value any, domain string, private ed25519.PrivateKey) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(private, append([]byte(domain), raw...)))
}

func newOperatorFixture(t *testing.T) *operatorFixture {
	t.Helper()
	f := &operatorFixture{objects: map[string]map[string]any{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests = append(f.requests, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer test-private-token" {
			t.Error("missing static auth")
			w.WriteHeader(401)
			return
		}
		if f.before != nil && f.before(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		obj, found := f.objects[r.URL.Path]
		if !found {
			w.WriteHeader(404)
			return
		}
		switch r.Method {
		case http.MethodGet:
		case http.MethodDelete:
			var opts metav1.DeleteOptions
			if json.NewDecoder(r.Body).Decode(&opts) != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil ||
				opts.Preconditions.ResourceVersion == nil || string(*opts.Preconditions.UID) != obj["metadata"].(map[string]any)["uid"] ||
				*opts.Preconditions.ResourceVersion != obj["metadata"].(map[string]any)["resourceVersion"] ||
				opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
				t.Error("unqualified Delete")
				w.WriteHeader(409)
				return
			}
			obj["metadata"].(map[string]any)["deletionTimestamp"] = "2026-10-01T00:00:00Z"
			obj["metadata"].(map[string]any)["resourceVersion"] = "701"
			if generation, ok := obj["metadata"].(map[string]any)["generation"].(float64); ok && generation > 0 {
				obj["metadata"].(map[string]any)["generation"] = generation + 1
			}
		case http.MethodPatch:
			var operations []map[string]any
			if json.NewDecoder(r.Body).Decode(&operations) != nil || len(operations) != 4 {
				t.Error("bad Patch")
				w.WriteHeader(400)
				return
			}
			m := obj["metadata"].(map[string]any)
			if operations[0]["value"] != m["uid"] || operations[1]["value"] != m["resourceVersion"] {
				t.Error("stale Patch")
				w.WriteHeader(409)
				return
			}
			m["finalizers"] = operations[3]["value"]
			m["resourceVersion"] = "702"
		default:
			t.Error("unexpected verb")
			w.WriteHeader(405)
			return
		}
		_ = json.NewEncoder(w).Encode(obj)
	}))
	t.Cleanup(f.server.Close)
	dir := t.TempDir()
	f.caPath = filepath.Join(dir, "ca.pem")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw})
	if err := os.WriteFile(f.caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	f.credentialsPath = filepath.Join(dir, "credentials.json")
	writeFixture(t, f.credentialsPath, Credentials{BearerToken: "test-private-token"})
	connection := ConnectionRecord{Version: "1", ServerURL: f.server.URL, TLSServerName: "127.0.0.1", CASHA256: digest(ca), SystemNamespaceUID: "system-original"}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{3}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	source := recoverypermit.EpochIdentity{Namespace: "old-runner", NamespaceUID: "runner-original", ReceiptNamespace: "old-receipts",
		ReceiptNamespaceUID: "receipts-original", GenesisName: "genesis", GenesisUID: "genesis-original", Generation: strings.Repeat("a", 32),
		ActiveLedgerUID: "active-original", QueueLedgerUID: "queue-original", WorkerPoolID: "old-pool"}
	for _, identity := range []struct{ name, uid string }{{"kube-system", connection.SystemNamespaceUID}, {source.Namespace, source.NamespaceUID}, {source.ReceiptNamespace, source.ReceiptNamespaceUID}} {
		m := map[string]any{"name": identity.name, "uid": identity.uid, "resourceVersion": "500"}
		phase := "Active"
		if identity.name != "kube-system" {
			phase = "Terminating"
			m["deletionTimestamp"] = "2026-10-01T00:00:00Z"
		}
		f.objects["/api/v1/namespaces/"+identity.name] = map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": m, "status": map[string]any{"phase": phase}}
	}
	obj := map[string]any{"apiVersion": "kova.cofy.dev/v1alpha1", "kind": "KovaBuild",
		"metadata": map[string]any{"namespace": source.Namespace, "name": "old-build", "uid": "build-original", "resourceVersion": "700", "generation": int64(1),
			"finalizers": []string{"kova.cofy.dev/cleanup", "kova.cofy.dev/recovery-hold", "external.example/retain"}},
		"spec": map[string]any{"privateEvidence": "do-not-print-target-body"}, "status": map[string]any{"phase": "Failed"}}
	body, _ := json.Marshal(obj)
	var cloned map[string]any
	_ = json.Unmarshal(body, &cloned)
	f.objects["/apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds/old-build"] = cloned
	target := recoverydisposal.ExecutionTarget{Resource: recoverydisposal.ExecutionResource{Group: "kova.cofy.dev", Version: "v1alpha1", Resource: "kovabuilds"},
		Namespace: source.Namespace, NamespaceUID: source.NamespaceUID, Name: "old-build", UID: "build-original", Role: "original-build", DispositionID: "old-build",
		ArchiveResourceVersion: "700", ArchiveDigest: digest(body), ArchiveBytes: int64(len(body)), QualificationVersion: recoverydisposal.ExecutionQualificationVersion,
		AllowedKovaFinalizers: []string{"kova.cofy.dev/cleanup", "kova.cofy.dev/recovery-hold"}}
	var err error
	target.QualificationDigest, err = recoverydisposal.DigestExecutionQualification(target, body)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	p := recoverydisposal.ExecutionPlanPayload{Version: recoverydisposal.ExecutionPlanVersion, Audience: recoverydisposal.ExecutionPlanAudience, Action: recoverydisposal.ExecutionPlanAction,
		Issuer: "fixture-external-authority", KeyID: "fixture-key", IncidentID: "isolated-fixture", Source: source,
		Cluster:             recoverydisposal.ExecutionClusterIdentity{SystemNamespace: "kube-system", SystemNamespaceUID: connection.SystemNamespaceUID, APIIdentityDigest: digestCanonical(connection)},
		AuthorizationDigest: fixtureHash("a"), IssuedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		Evidence: recoverydisposal.ExecutionEvidence{PhysicalRetirement: recoverydisposal.PhysicalRetirement{OldWorkerPoolID: source.WorkerPoolID, OldCapacitySlots: 2,
			ServiceProcessesDigest: fixtureHash("1"), RunnerProcessesDigest: fixtureHash("2"), WorkerPoolDigest: fixtureHash("3"), NetworkIsolationDigest: fixtureHash("4"),
			InflightEffectsDigest: fixtureHash("5"), Assertion: "old-service-runner-workers-network-and-inflight-effects-cannot-resume-v1", RetiredAt: now.Add(-4 * time.Minute).Format(time.RFC3339Nano)},
			StopIntentDigest: fixtureHash("6"), InflightBarrierDigest: fixtureHash("7"), FullArchiveManifestDigest: fixtureHash("8"), StorageIdentityDigest: fixtureHash("9"),
			ArchiveAssertion: recoverydisposal.ExecutionArchiveAssertion, ArchiveObjectCount: 3, ArchiveBytes: int64(len(body)) + 1024,
			InventoryObserverDigest: fixtureHash("a"), InventoryAssertion: recoverydisposal.ExecutionInventoryAssertion, RunnerNoReuseFenceDigest: fixtureHash("b"),
			ReceiptNoReuseFenceDigest: fixtureHash("c"), NoReuseAssertion: recoverydisposal.ExecutionNoReuseAssertion, ObservedAt: now.Add(-3 * time.Minute).Format(time.RFC3339Nano)},
		Limits:       recoverydisposal.ExecutionLimits{MaxObjects: 4, MaxArchiveObjects: 8, MaxDispositions: 4, MaxCalls: 64, MaxObjectArchiveBytes: 4096, MaxArchiveBytes: 8192},
		Dispositions: []recoverydisposal.ExecutionDisposition{{ID: "old-build", BuildName: "old-build", OriginalBuildUID: "build-original", Outcome: "approved-unknown-discard", ApprovalDigest: fixtureHash("d"), NeverReplay: true}},
		Targets:      []recoverydisposal.ExecutionTarget{target}}
	f.files = Files{PinsPath: filepath.Join(dir, "pins.json"), PlanPath: filepath.Join(dir, "plan.json"), GrantPath: filepath.Join(dir, "grant.json"), ArchivesPath: filepath.Join(dir, "targets.json")}
	planRaw := writeFixture(t, f.files.PlanPath, recoverydisposal.ExecutionPlanEnvelope{Payload: p, Signature: fixtureSign(t, p, "KOVA-RECOVERY-DISPOSAL-EXECUTION-PLAN-V1\x00", private)})
	planDigest, err := recoverydisposal.DigestExecutionPlan(p)
	if err != nil {
		t.Fatal(err)
	}
	g := recoverydisposal.MutationGrantPayload{Version: recoverydisposal.MutationGrantVersion, Audience: recoverydisposal.MutationGrantAudience, Action: recoverydisposal.MutationGrantAction,
		Issuer: p.Issuer, KeyID: p.KeyID, IncidentID: p.IncidentID, Cluster: p.Cluster, Source: p.Source, PlanDigest: planDigest, PlanEnvelopeDigest: digest(planRaw), ReviewDigest: fixtureHash("e"),
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339Nano), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339Nano)}
	grantRaw := writeFixture(t, f.files.GrantPath, recoverydisposal.MutationGrantEnvelope{Payload: g, Signature: fixtureSign(t, g, "KOVA-RECOVERY-DISPOSAL-MUTATION-GRANT-V1\x00", private)})
	pubPin := PublicKeyPin{Issuer: p.Issuer, KeyID: p.KeyID, PublicKeyHex: hex.EncodeToString(public)}
	f.pins = Pins{Plan: recoverydisposal.ExecutionPlanExpectation{EnvelopeDigest: digest(planRaw), PlanDigest: planDigest, Payload: p},
		Grant: recoverydisposal.MutationGrantExpectation{EnvelopeDigest: digest(grantRaw), Payload: g}, PlanRoots: []PublicKeyPin{pubPin}, GrantRoots: []PublicKeyPin{pubPin}, Connection: connection}
	f.files.PinsDigest = digest(writeFixture(t, f.files.PinsPath, f.pins))
	writeFixture(t, f.files.ArchivesPath, []json.RawMessage{body})
	return f
}

func TestPrepareIsOfflineAndExecutorPreservesForeignFinalizer(t *testing.T) {
	f := newOperatorFixture(t)
	prepared, err := Prepare(context.Background(), f.files, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 0 || prepared.Report().TargetCount != 1 || prepared.Report().Stage != "exact-target-archives-qualified-not-authorized" {
		t.Fatal("preparation used API or misreported authority")
	}
	report, err := Execute(context.Background(), prepared, f.caPath, f.credentialsPath)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "exact-disposal-pending" || report.MutationAttempts != 2 || report.Observations[0].Observation != "deleting-foreign-finalizers-preserved" {
		t.Fatalf("report=%+v", report)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	finalizers := f.objects["/apis/kova.cofy.dev/v1alpha1/namespaces/old-runner/kovabuilds/old-build"]["metadata"].(map[string]any)["finalizers"]
	raw, _ := json.Marshal(finalizers)
	if string(raw) != `["external.example/retain"]` {
		t.Fatalf("foreign finalizers=%s", raw)
	}
	for _, request := range f.requests {
		if strings.Contains(request, "POST ") || strings.Contains(request, "DELETE /api/v1/namespaces/") {
			t.Fatal("forbidden operation", request)
		}
	}
}

func TestPrepareRejectsPinsGrantBodiesAndBoundedFiles(t *testing.T) {
	for name, change := range map[string]func(*testing.T, *operatorFixture){
		"wrong independent pin": func(t *testing.T, f *operatorFixture) { f.files.PinsDigest = fixtureHash("f") },
		"duplicate pins field": func(t *testing.T, f *operatorFixture) {
			raw, _ := os.ReadFile(f.files.PinsPath)
			raw = append([]byte(`{"connection":{},`), raw[1:]...)
			_ = os.WriteFile(f.files.PinsPath, raw, 0600)
			f.files.PinsDigest = digest(raw)
		},
		"wrong CA binding": func(t *testing.T, f *operatorFixture) {
			f.pins.Connection.CASHA256 = fixtureHash("f")
			f.files.PinsDigest = digest(writeFixture(t, f.files.PinsPath, f.pins))
		},
		"wrong grant root": func(t *testing.T, f *operatorFixture) {
			f.pins.GrantRoots[0].PublicKeyHex = strings.Repeat("f", 64)
			f.files.PinsDigest = digest(writeFixture(t, f.files.PinsPath, f.pins))
		},
		"wrong archive": func(t *testing.T, f *operatorFixture) {
			writeFixture(t, f.files.ArchivesPath, []json.RawMessage{json.RawMessage(`{"private":"wrong-evidence"}`)})
		},
		"extra archive": func(t *testing.T, f *operatorFixture) {
			writeFixture(t, f.files.ArchivesPath, []json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{}`)})
		},
		"public archive": func(t *testing.T, f *operatorFixture) { _ = os.Chmod(f.files.ArchivesPath, 0644) },
		"symlink": func(t *testing.T, f *operatorFixture) {
			link := f.files.PlanPath + ".link"
			if err := os.Symlink(f.files.PlanPath, link); err != nil {
				t.Fatal(err)
			}
			f.files.PlanPath = link
		},
		"large pins array": func(t *testing.T, f *operatorFixture) {
			raw := []byte(`{"planRoots":[` + strings.Repeat(`{},`, 8192) + `{}]}`)
			_ = os.WriteFile(f.files.PinsPath, raw, 0600)
			f.files.PinsDigest = digest(raw)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newOperatorFixture(t)
			change(t, f)
			if _, err := Prepare(context.Background(), f.files, time.Now()); !errors.Is(err, ErrInput) {
				t.Fatalf("err=%v", err)
			}
			if len(f.requests) != 0 {
				t.Fatal("bad input caused API request")
			}
		})
	}
	f := newOperatorFixture(t)
	if _, err := Prepare(context.Background(), f.files, time.Now().Add(time.Hour)); !errors.Is(err, ErrInput) {
		t.Fatal("expired grant qualified")
	}
}

func TestExplicitConnectionRejectsBadTLSCAAndPublicCredentialsBeforeAPI(t *testing.T) {
	for name, change := range map[string]func(*testing.T, *operatorFixture){
		"wrong CA":           func(t *testing.T, f *operatorFixture) { _ = os.WriteFile(f.caPath, []byte("foreign-ca"), 0600) },
		"public credentials": func(t *testing.T, f *operatorFixture) { _ = os.Chmod(f.credentialsPath, 0644) },
		"duplicate credentials": func(t *testing.T, f *operatorFixture) {
			_ = os.WriteFile(f.credentialsPath, []byte(`{"bearerToken":"private","bearerToken":"test-private-token","clientCertificatePem":"","clientKeyPem":""}`), 0600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newOperatorFixture(t)
			p, err := Prepare(context.Background(), f.files, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			change(t, f)
			if _, err := Execute(context.Background(), p, f.caPath, f.credentialsPath); err == nil {
				t.Fatal("unqualified connection succeeded")
			}
			if len(f.requests) != 0 {
				t.Fatal("bad credentials/CA reached API")
			}
		})
	}
	f := newOperatorFixture(t)
	p, err := Prepare(context.Background(), f.files, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.connection.TLSServerName = "wrong.example"
	p.plan = recoverydisposal.VerifiedExecutionPlan{}
	if _, err := newExactAPI(p, f.caPath, f.credentialsPath); err == nil {
		t.Fatal("unsealed connection accepted")
	}
}

func TestSingleWireMutationUnknownAndNoRedirect(t *testing.T) {
	for _, mode := range []string{"retry-after", "redirect", "lost-response", "oversized", "duplicate-json"} {
		t.Run(mode, func(t *testing.T) {
			f := newOperatorFixture(t)
			p, err := Prepare(context.Background(), f.files, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			f.before = func(w http.ResponseWriter, r *http.Request) bool {
				if mode == "oversized" && r.Method == "GET" {
					w.Header().Set("Content-Length", "99999999")
					w.WriteHeader(200)
					return true
				}
				if mode == "duplicate-json" && r.Method == "GET" {
					_, _ = io.WriteString(w, `{"kind":"Namespace","kind":"Namespace"}`)
					return true
				}
				if r.Method != "DELETE" {
					return false
				}
				switch mode {
				case "retry-after":
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(429)
				case "redirect":
					w.Header().Set("Location", f.server.URL+"/foreign")
					w.WriteHeader(307)
				case "lost-response":
					h := w.(http.Hijacker)
					conn, _, err := h.Hijack()
					if err != nil {
						t.Fatal(err)
					}
					_ = conn.Close()
				}
				return true
			}
			report, err := Execute(context.Background(), p, f.caPath, f.credentialsPath)
			if err == nil || report.Stage != "unknown" {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			deletes := 0
			for _, r := range f.requests {
				if strings.HasPrefix(r, "DELETE ") {
					deletes++
				}
				if strings.Contains(r, "foreign") {
					t.Fatal("redirect followed")
				}
			}
			if mode == "oversized" || mode == "duplicate-json" {
				if deletes != 0 {
					t.Fatal("preflight error wrote")
				}
			} else if deletes != 1 {
				t.Fatalf("mutation repeated: %d", deletes)
			}
			if strings.Contains(err.Error(), "test-private-token") || strings.Contains(err.Error(), "do-not-print-target-body") {
				t.Fatal("sensitive output")
			}
		})
	}
}

func TestAdapterRejectsBroadNamespaceAndPatchMutation(t *testing.T) {
	f := newOperatorFixture(t)
	p, err := Prepare(context.Background(), f.files, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a, err := newExactAPI(p, f.caPath, f.credentialsPath)
	if err != nil {
		t.Fatal(err)
	}
	n := &unstructured.Unstructured{}
	n.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
	n.SetName("old-runner")
	if err := a.Delete(context.Background(), n); !errors.Is(err, ErrInput) {
		t.Fatal("namespace Delete accepted")
	}
	if err := a.Get(context.Background(), client.ObjectKey{Name: "other-namespace"}, n); !errors.Is(err, ErrInput) {
		t.Fatal("foreign namespace read accepted")
	}
	b := &unstructured.Unstructured{}
	b.SetGroupVersionKind(schema.GroupVersionKind{Group: "kova.cofy.dev", Version: "v1alpha1", Kind: "KovaBuild"})
	b.SetNamespace("old-runner")
	b.SetName("old-build")
	b.SetUID(types.UID("build-original"))
	b.SetResourceVersion("700")
	if err := a.Patch(context.Background(), b, client.RawPatch(types.JSONPatchType, []byte(`[{"op":"replace","path":"/spec","value":{}}]`))); !errors.Is(err, ErrInput) {
		t.Fatal("spec Patch accepted")
	}
	if len(f.requests) != 0 {
		t.Fatal("forbidden adapter operation reached API")
	}
}

func TestConnectionRecordRejectsImplicitUnsafeRouting(t *testing.T) {
	base := ConnectionRecord{Version: "1", ServerURL: "https://api.example", TLSServerName: "api.example", CASHA256: fixtureHash("a"), SystemNamespaceUID: "original-system"}
	for _, endpoint := range []string{"http://api.example", "https://user:pass@api.example", "https://api.example/path", "https://api.example?proxy=1", "https://api.example#other", "https://api.example:0"} {
		c := base
		c.ServerURL = endpoint
		if validConnection(c) {
			t.Fatalf("unsafe endpoint=%s", endpoint)
		}
	}
}

func TestForeignOrMalformed404IsUnknownBeforeAnyWrite(t *testing.T) {
	for _, mode := range []string{"empty", "html", "oversized", "wrong-name", "wrong-resource", "wrong-group", "wrong-kind", "wrong-reason", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			f := newOperatorFixture(t)
			p, err := Prepare(context.Background(), f.files, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			f.before = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/kovabuilds/") {
					return false
				}
				status := apierrors.NewNotFound(schema.GroupResource{Group: "kova.cofy.dev", Resource: "kovabuilds"}, "old-build").ErrStatus
				status.APIVersion = "v1"
				status.Kind = "Status"
				switch mode {
				case "wrong-name":
					status.Details.Name = "different-build"
				case "wrong-resource":
					status.Details.Kind = "pods"
				case "wrong-group":
					status.Details.Group = "foreign.example"
				case "wrong-kind":
					status.Kind = "ForeignStatus"
				case "wrong-reason":
					status.Reason = metav1.StatusReasonForbidden
				}
				w.WriteHeader(404)
				switch mode {
				case "empty":
				case "html":
					_, _ = io.WriteString(w, "<html>not found</html>")
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", 4097))
				case "duplicate":
					_, _ = io.WriteString(w, `{"kind":"Status","kind":"Status"}`)
				default:
					_ = json.NewEncoder(w).Encode(status)
				}
				return true
			}
			report, err := Execute(context.Background(), p, f.caPath, f.credentialsPath)
			if err == nil || report.Stage != "unknown" || report.MutationAttempts != 0 {
				t.Fatalf("unqualified404 report=%+v err=%v", report, err)
			}
			for _, request := range f.requests {
				if !strings.HasPrefix(request, "GET ") {
					t.Fatal("404 uncertainty caused mutation", request)
				}
			}
		})
	}
}

func TestQualifiedExactKubernetes404IsObservationOnly(t *testing.T) {
	f := newOperatorFixture(t)
	p, err := Prepare(context.Background(), f.files, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/kovabuilds/") {
			return false
		}
		status := apierrors.NewNotFound(schema.GroupResource{Group: "kova.cofy.dev", Resource: "kovabuilds"}, "old-build").ErrStatus
		status.APIVersion = "v1"
		status.Kind = "Status"
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(status)
		return true
	}
	report, err := Execute(context.Background(), p, f.caPath, f.credentialsPath)
	if err != nil || report.Stage != "exact-targets-absent-observed-not-closed" || report.MutationAttempts != 0 {
		t.Fatalf("qualified404 report=%+v err=%v", report, err)
	}
}

func TestSeparateOperatorCommandDefaultsToNoAPIAndRequiresExplicitExecution(t *testing.T) {
	f := newOperatorFixture(t)
	args := []string{"kova-recovery", "dispose-exact", "--pins", f.files.PinsPath, "--pins-digest", f.files.PinsDigest, "--plan", f.files.PlanPath,
		"--grant", f.files.GrantPath, "--target-archives", f.files.ArchivesPath}
	var output bytes.Buffer
	app := NewCLIApp()
	app.Writer = &output
	app.ErrWriter = &output
	if err := app.Run(args); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 0 || !strings.Contains(output.String(), `"mode":"local-preflight-no-api"`) ||
		!strings.Contains(output.String(), `"incidentClosed":false`) || strings.Contains(output.String(), "do-not-print-target-body") ||
		strings.Contains(output.String(), "test-private-token") {
		t.Fatal("default command changed authority or leaked evidence")
	}
	app = NewCLIApp()
	app.Writer = io.Discard
	app.ErrWriter = io.Discard
	if err := app.Run(append(args, "--execute")); err == nil {
		t.Fatal("execution without explicit CA/credentials accepted")
	}
	if len(f.requests) != 0 {
		t.Fatal("incomplete explicit execution called API")
	}
	output.Reset()
	app = NewCLIApp()
	app.Writer = &output
	app.ErrWriter = &output
	if err := app.Run(append(args, "--execute", "--ca-file", f.caPath, "--credentials", f.credentialsPath)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"mode":"explicit-exact-disposal-invocation"`) || !strings.Contains(output.String(), `"capacityReleased":false`) ||
		!strings.Contains(output.String(), `"successorCreated":false`) {
		t.Fatal("execution implied closure or capacity")
	}
}

func TestMalformedDeleteSuccessIsUnknownWithoutPatchOrLaterTarget(t *testing.T) {
	for _, mode := range []string{"empty-200", "empty-202", "html", "oversized", "duplicate", "wrong-uid", "wrong-address", "wrong-gvk", "body-drift", "malformed-deletion-time", "zero-deletion-time", "wrong-status", "wrong-status-uid", "wrong-status-code"} {
		t.Run(mode, func(t *testing.T) {
			f := newOperatorFixture(t)
			p, err := Prepare(context.Background(), f.files, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			f.before = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodDelete {
					return false
				}
				if mode == "empty-202" {
					w.WriteHeader(202)
				} else {
					w.WriteHeader(200)
				}
				switch mode {
				case "empty-200", "empty-202":
				case "html":
					_, _ = io.WriteString(w, "<html>success?</html>")
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", 4097))
				case "duplicate":
					_, _ = io.WriteString(w, `{"kind":"Status","kind":"Status"}`)
				case "wrong-status", "wrong-status-uid", "wrong-status-code":
					status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200,
						Details: &metav1.StatusDetails{Name: "old-build", Group: "kova.cofy.dev", Kind: "kovabuilds", UID: types.UID("build-original")}}
					if mode == "wrong-status" {
						status.Status = metav1.StatusFailure
					}
					if mode == "wrong-status-uid" {
						status.Details.UID = "replacement"
					}
					if mode == "wrong-status-code" {
						status.Code = 409
					}
					_ = json.NewEncoder(w).Encode(status)
				default:
					raw, _ := json.Marshal(f.objects[r.URL.Path])
					var obj map[string]any
					_ = json.Unmarshal(raw, &obj)
					metadata := obj["metadata"].(map[string]any)
					switch mode {
					case "wrong-uid":
						metadata["uid"] = "replacement"
					case "wrong-address":
						metadata["name"] = "other-build"
					case "wrong-gvk":
						obj["kind"] = "Pod"
					case "body-drift":
						obj["spec"] = map[string]any{"new": "unqualified"}
					case "malformed-deletion-time":
						metadata["deletionTimestamp"] = "invalid"
					case "zero-deletion-time":
						metadata["deletionTimestamp"] = "0001-01-01T00:00:00Z"
					}
					_ = json.NewEncoder(w).Encode(obj)
				}
				return true
			}
			report, err := Execute(context.Background(), p, f.caPath, f.credentialsPath)
			if err == nil || report.Stage != "unknown" || report.MutationAttempts != 1 {
				t.Fatalf("Delete response was not unknown: %+v %v", report, err)
			}
			if !strings.HasPrefix(f.requests[len(f.requests)-1], "DELETE ") {
				t.Fatal("unknown Delete was followed by another API call")
			}
			for _, request := range f.requests {
				if strings.HasPrefix(request, "PATCH ") {
					t.Fatal("unknown Delete led to finalizer Patch")
				}
			}
		})
	}
}

func TestQualifiedDeleteStatusRequiresGuardedReadback(t *testing.T) {
	f := newOperatorFixture(t)
	p, err := Prepare(context.Background(), f.files, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodDelete {
			return false
		}
		delete(f.objects, r.URL.Path)
		status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess,
			Details: &metav1.StatusDetails{Name: "old-build", Group: "kova.cofy.dev", Kind: "kovabuilds", UID: types.UID("build-original")}}
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(status)
		return true
	}
	// The direct absent response is also qualified, never an empty 404.
	oldBefore := f.before
	f.before = func(w http.ResponseWriter, r *http.Request) bool {
		if oldBefore(w, r) {
			return true
		}
		if r.Method == "GET" && strings.Contains(r.URL.Path, "/kovabuilds/") && f.objects[r.URL.Path] == nil {
			status := apierrors.NewNotFound(schema.GroupResource{Group: "kova.cofy.dev", Resource: "kovabuilds"}, "old-build").ErrStatus
			status.APIVersion = "v1"
			status.Kind = "Status"
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(status)
			return true
		}
		return false
	}
	report, err := Execute(context.Background(), p, f.caPath, f.credentialsPath)
	if err != nil || report.Stage != "exact-targets-absent-observed-not-closed" || report.MutationAttempts != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if !strings.HasPrefix(f.requests[len(f.requests)-1], "GET ") {
		t.Fatal("Delete Status accepted without fresh guarded reads")
	}
}
