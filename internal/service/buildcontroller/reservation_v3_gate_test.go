package buildcontroller

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type capacityRoundTripper func(*http.Request) (*http.Response, error)

func (f capacityRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type capacityCountingClient struct {
	client.Client
	calls, mutations int
}

type capacityCountingCoreAPI struct {
	admissiongenesis.CoreAPI
	calls, mutations int
}

func (a *capacityCountingCoreAPI) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	a.calls++
	return a.CoreAPI.GetNamespace(ctx, name)
}

func (a *capacityCountingCoreAPI) GetConfigMap(ctx context.Context, ns, name string) (*corev1.ConfigMap, error) {
	a.calls++
	return a.CoreAPI.GetConfigMap(ctx, ns, name)
}

func (a *capacityCountingCoreAPI) CreateConfigMap(ctx context.Context, ns string, object *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	a.calls++
	a.mutations++
	assigned := object.DeepCopy()
	if assigned.UID == "" {
		assigned.UID = types.UID("fake-created-" + assigned.Name)
	}
	return a.CoreAPI.CreateConfigMap(ctx, ns, assigned)
}

func (a *capacityCountingCoreAPI) PatchConfigMap(ctx context.Context, ns, name string, body []byte) (*corev1.ConfigMap, error) {
	a.calls++
	a.mutations++
	return a.CoreAPI.PatchConfigMap(ctx, ns, name, body)
}

func (c *capacityCountingClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	c.calls++
	return c.Client.Get(ctx, key, object, opts...)
}

func (c *capacityCountingClient) Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error {
	c.calls++
	c.mutations++
	return c.Client.Create(ctx, object, opts...)
}

func (c *capacityCountingClient) Patch(ctx context.Context, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.calls++
	c.mutations++
	return c.Client.Patch(ctx, object, patch, opts...)
}

func (c *capacityCountingClient) Update(ctx context.Context, object client.Object, opts ...client.UpdateOption) error {
	c.calls++
	c.mutations++
	return c.Client.Update(ctx, object, opts...)
}

func (c *capacityCountingClient) Delete(ctx context.Context, object client.Object, opts ...client.DeleteOption) error {
	c.calls++
	c.mutations++
	return c.Client.Delete(ctx, object, opts...)
}

type capacityCountingStatus struct {
	client.SubResourceWriter
	owner *capacityCountingClient
}

func (c *capacityCountingClient) Status() client.SubResourceWriter {
	return capacityCountingStatus{SubResourceWriter: c.Client.Status(), owner: c}
}

func (s capacityCountingStatus) Update(ctx context.Context, object client.Object, opts ...client.SubResourceUpdateOption) error {
	s.owner.calls++
	s.owner.mutations++
	return s.SubResourceWriter.Update(ctx, object, opts...)
}

func TestCapacityLifecycleRequestModel(t *testing.T) {
	r, api, build := genesisAdmissionFixtureWithConfig(t, config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 1000})
	build.Finalizers = nil
	if err := api.Update(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	build.UID, build.ResourceVersion = "fake-capacity-build", ""
	bootstrap := r.Genesis.Bootstrap
	initial, err := api.GetConfigMap(t.Context(), r.Cfg.Namespace, bootstrap.Receipt.GenesisName)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(admissioncontract.GenesisData{Contract: bootstrap.Receipt.Contract, Phase: admissioncontract.PhaseInitializing})
	initial.Data = map[string]string{admissioncontract.GenesisDataKey: string(data)}
	no := false
	initial.Immutable = &no
	if err := api.Update(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{admissioncontract.ActiveLedgerName, admissioncontract.QueueLedgerName} {
		if err := api.Delete(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.Cfg.Namespace, Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	counted := &capacityCountingClient{Client: api.Client}
	r.Client, r.APIReader = counted, counted
	core := &capacityCountingCoreAPI{CoreAPI: api}
	bootstrap.API = core
	// Model only the five named GETs in the actual external ReceiptCheck.
	// This is deterministic call accounting, not API or elapsed-time proof.
	external := 5 // harness initial immutable owner/Namespace check
	for _, name := range []string{admissioncontract.ActiveLedgerName, admissioncontract.QueueLedgerName} {
		if _, err := core.GetConfigMap(t.Context(), r.Cfg.Namespace, name); !apierrors.IsNotFound(err) {
			t.Fatal("model lacks fresh absent ledgers")
		}
	}
	if _, err := core.GetConfigMap(t.Context(), r.Cfg.Namespace, bootstrap.Receipt.GenesisName); err != nil {
		t.Fatal(err)
	}
	// Exact live preflight: five owner GETs + 3*4 old-work Lists + one
	// receipt ConfigMap List. The fixture is empty, so no retry is modeled.
	preflights := 0
	bootstrap.Preflight = func(context.Context) error { preflights++; external += 18; return nil }
	bootstrap.BeforeEffect = func(context.Context) error { external += 5; return nil }
	binding, err := bootstrap.EnsureFresh(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r.Genesis, err = admissiongenesis.NewGuard(t.Context(), bootstrap, binding)
	if err != nil {
		t.Fatal(err)
	}
	r.RecoveryReceipts = controllerReceiptConfigMaps{Client: counted, namespace: bootstrap.Receipt.Contract.ReceiptNamespace}
	startup := core.calls + counted.calls + external
	t.Logf("fresh startup model=%d calls (%d CoreAPI+%d direct owner/List checks), mutations=%d", startup, core.calls, external, core.mutations)
	r.Genesis.ReceiptCheck = func(context.Context) error { external += 5; return nil }
	exerciseV3CapacityLifecycle(t, t.Context(), r, build, false, func(stage string) {
		external += 5
		t.Logf("%s: typed CoreAPI=%d controller/receipt=%d external named GETs=%d total model=%d", stage, core.calls, counted.calls, external, core.calls+counted.calls+external)
	})
	total, mutations := core.calls+counted.calls+external, core.mutations+counted.mutations
	if total <= 1500 || total > capacityCallLimit || mutations > 64 {
		t.Fatal("full strict direct-read model does not fit its explicit call/write budget")
	}
	// The old typed CoreV1 REST client alone owns these calls. Exclude the
	// three apps and three CR Lists per preflight, and all controller calls;
	// default per-resource buckets cannot invalidate this lower bound.
	oldCoreCalls := core.calls + external - 6*preflights
	oldFloor := time.Duration(oldCoreCalls-int(rest.DefaultBurst)) * time.Second / time.Duration(rest.DefaultQPS)
	newFloor := time.Duration(total-capacityClientBurst) * time.Second / capacityClientQPS
	if oldFloor <= 3*time.Minute || newFloor >= 3*time.Minute {
		t.Fatal("finite client-rate model does not explain the unchanged deadline")
	}
	t.Logf("complete strict model=%d calls (%d CoreAPI+%d controller/receipt+%d external checks), mutations=%d; zero-latency old CoreV1 floor (%d calls at 5QPS)=%s, shared20QPS floor=%s, not elapsed-time proof", total, core.calls, counted.calls, external, mutations, oldCoreCalls, oldFloor, newFloor)
}

func TestCapacityClientsShareFiniteLimiterAndKeepTLSDeadline(t *testing.T) {
	m := capacityGateManifest()
	ledger := capacityGateLedger(m)
	calls := 0
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		var object any = ledger
		if r.URL.Path == "/api/v1/namespaces/"+m.Runner.Name {
			object = &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: m.Runner.Name, UID: types.UID(m.Runner.UID), ResourceVersion: "1"}}
		}
		raw, _ := json.Marshal(object)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}))
	original := &rest.Config{Host: g.origin, Timeout: 10 * time.Second,
		ContentConfig:   rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"},
		TLSClientConfig: rest.TLSClientConfig{CAData: []byte("synthetic-ca"), CertData: []byte("synthetic-cert"), KeyData: []byte("synthetic-key"), NextProtos: []string{"http/1.1"}}}
	cfg := capacityClientConfig(original)
	if cfg == original || cfg.QPS != 20 || cfg.Burst != 20 || cfg.RateLimiter == nil || cfg.RateLimiter.QPS() != 20 || original.RateLimiter != nil || original.QPS != 0 ||
		cfg.Timeout != 10*time.Second || cfg.Host != original.Host || !reflect.DeepEqual(cfg.TLSClientConfig, original.TLSClientConfig) || !reflect.DeepEqual(cfg.ContentConfig, original.ContentConfig) {
		t.Fatal("finite test-only rate policy changed source TLS/deadline/credentials or became implicit")
	}
	httpClient := &http.Client{Transport: g, Timeout: 10 * time.Second}
	cs, err := kubernetes.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	if cs.CoreV1().RESTClient().GetRateLimiter() != cfg.RateLimiter || cs.AppsV1().RESTClient().GetRateLimiter() != cfg.RateLimiter {
		t.Fatal("typed resource clients minted separate limiters")
	}
	scheme := testScheme(t)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, kovav1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	mapper.Add(kovav1.SchemeGroupVersion.WithKind("KovaBuild"), meta.RESTScopeNamespace)
	for _, gvk := range []schema.GroupVersionKind{corev1.SchemeGroupVersion.WithKind("ConfigMap"), kovav1.SchemeGroupVersion.WithKind("KovaBuild")} {
		restClient, err := apiutil.RESTClientForGVK(gvk, true, false, cfg, serializer.NewCodecFactory(scheme), httpClient)
		if err != nil || restClient.GetRateLimiter() != cfg.RateLimiter {
			t.Fatal("controller-runtime per-GVK config lost shared limiter identity")
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme, Mapper: mapper, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Namespaces().Get(t.Context(), m.Runner.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(ledger), &corev1.ConfigMap{}); err != nil || calls != 2 || g.wireMutations != 0 || httpClient.Timeout != 10*time.Second {
		t.Fatal("actual typed/controller HTTP paths lost timeout or qualification")
	}
	deadline, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := cs.CoreV1().Namespaces().Get(deadline, m.Runner.Name, metav1.GetOptions{}); err == nil || calls != 2 {
		t.Fatal("expired client context reached upstream")
	}
	row := capacityFailureRow(deadline, "untrusted raw error", g, cfg.RateLimiter)
	if row["reason"] != "LOCAL_CONTEXT_DEADLINE" {
		t.Fatal("local deadline did not become a fixed safe enum")
	}
}

func TestCapacityBudgetExactPositiveAndNegativeEdges(t *testing.T) {
	m := capacityGateManifest()
	ns := &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: m.Runner.Name, UID: types.UID(m.Runner.UID), ResourceVersion: "1"}}
	raw, _ := json.Marshal(ns)
	calls := 0
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}))
	g.calls = capacityCallLimit - 1
	path := "/api/v1/namespaces/" + m.Runner.Name
	if _, err := g.RoundTrip(capacityGateRequest("GET", path, nil)); err != nil || g.calls != 3500 || calls != 1 {
		t.Fatal("final declared read inside the finite budget was refused")
	}
	if _, err := g.RoundTrip(capacityGateRequest("GET", path, nil)); err == nil || calls != 1 || g.calls != 3500 {
		t.Fatal("over-budget read reached upstream")
	}
	ledger := capacityGateLedger(m)
	ack, _ := json.Marshal(ledger)
	proposal := ledger.DeepCopy()
	proposal.UID, proposal.ResourceVersion = "", ""
	raw, _ = json.Marshal(proposal)
	g = newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 201, Body: io.NopCloser(bytes.NewReader(ack))}, nil
	}))
	g.mutations = 63
	path = "/api/v1/namespaces/" + m.Runner.Name + "/configmaps"
	if _, err := g.RoundTrip(capacityGateRequest("POST", path, raw)); err != nil || g.mutations != 64 || g.wireMutations != 1 {
		t.Fatal("64th exact-qualified mutation was refused")
	}
	if _, err := g.RoundTrip(capacityGateRequest("POST", path, raw)); err == nil || g.wireMutations != 1 || g.mutations != 64 || g.wirePodPosts != 0 {
		t.Fatal("mutation/zero-Pod budget expanded")
	}
}

func TestCapacityFailureJournalHasOnlySafeOwnedFactsWithinReserve(t *testing.T) {
	g := newCapacityTransport(capacityGateManifest(), admissioncontract.Receipt{}, "https://127.0.0.1:6443", nil)
	g.calls, g.mutations, g.wireCalls, g.wireMutations = 3100, 36, 3099, 35
	for i := 0; i < 6; i++ {
		path := "/api/v1/namespaces/" + strings.Repeat("a", 63) + "/configmaps/" + strings.Repeat(string(rune('a'+i)), 63)
		g.owned[path] = strings.Repeat("u", 256)
		if i < 3 {
			g.receiptDigests[path] = "sha256:" + strings.Repeat("d", 64)
		}
	}
	row := capacityFailureRow(t.Context(), "token/Secret raw exception must not leak", g, nil)
	raw, _ := json.Marshal(row)
	if row["reason"] != "UNCLASSIFIED" || bytes.Contains(raw, []byte("must not leak")) || len(raw)+1 > capacityFailureReserve || row["apiUpstreamCalls"] != 3099 || row["apiUpstreamMutations"] != 35 || row["apiWireAttemptsUpperBound"] != 3099 {
		t.Fatal("failure record leaked raw data, lost counters or exceeded its reserved byte bound")
	}
	for key := range g.owned {
		delete(g.owned, key)
	}
	if len(row["ownedUIDs"].(map[string]string)) != 6 || len(row["receiptDataDigests"].(map[string]string)) != 3 {
		t.Fatal("failure record did not snapshot original owned facts")
	}
	limited := capacityClientConfig(&rest.Config{}).RateLimiter.(*capacityRateLimiter)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if limited.Wait(cancelled) == nil || limited.Wait(t.Context()) == nil || capacityFailureRow(t.Context(), "UNCLASSIFIED", g, limited)["reason"] != "LOCAL_RATE_LIMIT_WAIT_REFUSED" || capacityFailureRow(cancelled, "UNCLASSIFIED", g, limited)["reason"] != "LOCAL_CONTEXT_CANCELLED" {
		t.Fatal("local limiter/cancellation failures lost safe classification")
	}
}

func capacityGateManifest() capacityManifest {
	now := time.Now().UTC()
	yes := true
	return capacityManifest{SchemaVersion: 1, SourceDirectory: "/private/source", SourceCommit: strings.Repeat("a", 40), TestBinarySHA256: strings.Repeat("b", 64),
		Kubeconfig: "/private/config", KubeconfigSHA256: strings.Repeat("c", 64), Context: "kind-kova-v3-capacity-test", KubeSystemUID: "system-original", ControlPlaneID: strings.Repeat("d", 64),
		PreparedAfter: now.Add(-time.Minute), ExpiresAt: now.Add(30 * time.Minute), ReceiptPath: "/private/result.jsonl", OriginalReceipt: "/private/original.json", OriginalSHA256: strings.Repeat("e", 64),
		Runner: capacityNamespace{"kova-v3-capacity-runner", "runner-original"}, Control: capacityNamespace{"kova-v3-capacity-control", "control-original"}, Receipts: capacityNamespace{"kova-v3-capacity-receipts", "receipts-original"},
		SecretName: "kova-v3-capacity-receipt", SecretUID: "secret-original", BuildName: "kova-v3-capacity-build", OldWritersStopped: &yes, RetainOnSuccess: &yes}
}

func TestCapacityManifestRefusesIncompleteOrReusedAuthority(t *testing.T) {
	m := capacityGateManifest()
	if err := validateCapacityManifest(m, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*capacityManifest){
		func(m *capacityManifest) { m.SchemaVersion = 0 },
		func(m *capacityManifest) { m.Control = m.Runner },
		func(m *capacityManifest) { m.Receipts.UID = m.KubeSystemUID },
		func(m *capacityManifest) { m.OldWritersStopped = nil },
		func(m *capacityManifest) { no := false; m.RetainOnSuccess = &no },
		func(m *capacityManifest) { m.ExpiresAt = m.PreparedAfter.Add(2 * time.Hour) },
		func(m *capacityManifest) { m.SourceCommit = "main" },
		func(m *capacityManifest) { m.Context = "kind-shared" },
	} {
		bad := m
		change(&bad)
		if validateCapacityManifest(bad, time.Now()) == nil {
			t.Fatal("incomplete/shared fixture accepted")
		}
	}
	raw, _ := json.Marshal(m)
	for _, suffix := range []string{`,"schemaVersion":1}`, `,"cleanup":true}`} {
		var got capacityManifest
		if admissionjson.Decode(append(raw[:len(raw)-1:len(raw)-1], suffix...), &got, capacityManifestField) == nil {
			t.Fatal("duplicate/unknown manifest field accepted")
		}
	}
}

func capacityGateRequest(method, path string, body []byte) *http.Request {
	r, _ := http.NewRequest(method, "https://127.0.0.1:6443"+path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestCapacityTransportNeverForwardsForeignWritesOrPodCreate(t *testing.T) {
	m := capacityGateManifest()
	calls := 0
	g := newCapacityTransport(m, admissioncontract.Receipt{GenesisName: admissioncontract.GenesisName, GenesisUID: "genesis-original"}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not forward") }))
	for _, r := range []*http.Request{
		capacityGateRequest("POST", "/api/v1/namespaces/"+m.Runner.Name+"/pods", []byte(`{"metadata":{"name":"never","namespace":"`+m.Runner.Name+`"}}`)),
		capacityGateRequest("DELETE", "/api/v1/namespaces/"+m.Runner.Name, []byte(`{}`)),
		capacityGateRequest("POST", "/api/v1/namespaces/"+m.Runner.Name+"/configmaps", []byte(`{"metadata":{"name":"kova-grant-intent-`+strings.Repeat("a", 32)+`","namespace":"`+m.Runner.Name+`"}}`)),
		capacityGateRequest("GET", "/api/v1/namespaces/default/pods", nil),
		capacityGateRequest("GET", "/api/v1/namespaces/"+m.Runner.Name+"/pods?watch=true", nil),
	} {
		if _, err := g.RoundTrip(r); err == nil {
			t.Fatal("foreign operation permitted")
		}
	}
	if calls != 0 {
		t.Fatal("forbidden API operation reached wire")
	}
}

func TestCapacityTransportUnknownIsSpentAndStopsAllFurtherWire(t *testing.T) {
	m := capacityGateManifest()
	calls := 0
	g := newCapacityTransport(m, admissioncontract.Receipt{GenesisName: admissioncontract.GenesisName, GenesisUID: "genesis-original"}, "https://127.0.0.1:6443", capacityRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.GetBody != nil || r.Header.Get("Idempotency-Key") != "" {
			t.Fatal("mutation retains replay authority")
		}
		return nil, errors.New("lost reply")
	}))
	body := []byte(`{"metadata":{"name":"` + admissioncontract.ActiveLedgerName + `","namespace":"` + m.Runner.Name + `"}}`)
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps"
	for i := 0; i < 2; i++ {
		r := capacityGateRequest("POST", path, body)
		r.Header.Set("Idempotency-Key", "attempt")
		if _, err := g.RoundTrip(r); err == nil {
			t.Fatal("unknown outcome accepted")
		}
	}
	if _, err := g.RoundTrip(capacityGateRequest("GET", "/api/v1/namespaces/"+m.Runner.Name, nil)); err == nil {
		t.Fatal("unknown did not stop campaign")
	}
	if calls != 1 {
		t.Fatalf("wire calls=%d", calls)
	}
}

func TestCapacityTransportUsesAssignedReceiptUIDAndQualifiesDelete(t *testing.T) {
	for _, ack := range []string{"status", "object", "empty", "foreign", "unbound-status"} {
		t.Run(ack, func(t *testing.T) {
			m := capacityGateManifest()
			name := "kova-grant-intent-" + strings.Repeat("a", 32)
			original := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.Receipts.Name, UID: "server-original", ResourceVersion: "1"}, Data: map[string]string{"intent": "fixed"}}
			collection := "/api/v1/namespaces/" + m.Receipts.Name + "/configmaps"
			calls := 0
			g := newCapacityTransport(m, admissioncontract.Receipt{GenesisName: admissioncontract.GenesisName, GenesisUID: "genesis-original"}, "https://127.0.0.1:6443", capacityRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				raw, _ := json.Marshal(original)
				if r.Method == "DELETE" {
					switch ack {
					case "status":
						raw = capacityOfficialDeleteStatus(t, original)
					case "empty":
						raw = nil
					case "foreign":
						copy := original.DeepCopy()
						copy.UID = "replacement"
						raw, _ = json.Marshal(copy)
					case "unbound-status":
						raw = []byte(`{"apiVersion":"v1","kind":"Status","status":"Success"}`)
					}
				}
				code := http.StatusOK
				if r.Method == "POST" {
					code = http.StatusCreated
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			proposal := original.DeepCopy()
			proposal.UID = ""
			proposal.ResourceVersion = ""
			raw, _ := json.Marshal(proposal)
			if _, err := g.RoundTrip(capacityGateRequest("POST", collection, raw)); err != nil {
				t.Fatal(err)
			}
			if g.owned[collection+"/"+name] != "server-original" {
				t.Fatal("did not retain API-assigned UID")
			}
			foreign := types.UID("replacement")
			raw, _ = json.Marshal(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &foreign}})
			if _, err := g.RoundTrip(capacityGateRequest("DELETE", collection+"/"+name, raw)); err == nil || calls != 1 {
				t.Fatal("foreign UID DELETE reached wire")
			}
			uid := original.UID
			raw, _ = json.Marshal(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			_, err := g.RoundTrip(capacityGateRequest("DELETE", collection+"/"+name, raw))
			if (err == nil) != (ack == "status" || ack == "object") {
				t.Fatalf("DELETE %s qualification=%v", ack, err)
			}
		})
	}
}

func TestCapacityTransportDeleteACKClosedShapes(t *testing.T) {
	m := capacityGateManifest()
	cm := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "kova-pod-create-intent-" + strings.Repeat("b", 32), Namespace: m.Receipts.Name, UID: "receipt-original", ResourceVersion: "74"},
		Data:       map[string]string{"intent": "fixed"}}
	path := "/api/v1/namespaces/" + cm.Namespace + "/configmaps/" + cm.Name
	status := capacityOfficialDeleteStatus(t, cm)
	object, _ := json.Marshal(cm)
	change := func(raw []byte, edit func(map[string]any)) []byte {
		t.Helper()
		var body map[string]any
		if !capacityDecode(raw, &body) {
			t.Fatal("invalid test body")
		}
		edit(body)
		out, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	cases := []struct {
		name   string
		body   []byte
		code   int
		accept bool
	}{
		{"official-status", status, 200, true},
		{"status-explicit-zero-code", change(status, func(o map[string]any) { o["code"] = 0 }), 200, true},
		{"status-empty-group", change(status, func(o map[string]any) { o["details"].(map[string]any)["group"] = "" }), 200, true},
		{"status-no-details", change(status, func(o map[string]any) { delete(o, "details") }), 200, false},
		{"status-null-details", change(status, func(o map[string]any) { o["details"] = nil }), 200, false},
		{"status-empty-details", change(status, func(o map[string]any) { o["details"] = map[string]any{} }), 200, false},
		{"status-wrong-name", change(status, func(o map[string]any) { o["details"].(map[string]any)["name"] = "foreign" }), 200, false},
		{"status-wrong-uid", change(status, func(o map[string]any) { o["details"].(map[string]any)["uid"] = "foreign" }), 200, false},
		{"status-singular-kind", change(status, func(o map[string]any) { o["details"].(map[string]any)["kind"] = "ConfigMap" }), 200, false},
		{"status-wrong-group", change(status, func(o map[string]any) { o["details"].(map[string]any)["group"] = "foreign" }), 200, false},
		{"status-null-group", change(status, func(o map[string]any) { o["details"].(map[string]any)["group"] = nil }), 200, false},
		{"status-null-causes", change(status, func(o map[string]any) { o["details"].(map[string]any)["causes"] = nil }), 200, false},
		{"status-zero-retry", change(status, func(o map[string]any) { o["details"].(map[string]any)["retryAfterSeconds"] = 0 }), 200, false},
		{"status-null-metadata", change(status, func(o map[string]any) { o["metadata"] = nil }), 200, false},
		{"status-nonempty-metadata", change(status, func(o map[string]any) { o["metadata"] = map[string]any{"resourceVersion": "75"} }), 200, false},
		{"status-empty-message", change(status, func(o map[string]any) { o["message"] = "" }), 200, false},
		{"status-empty-reason", change(status, func(o map[string]any) { o["reason"] = "" }), 200, false},
		{"status-null-code", change(status, func(o map[string]any) { o["code"] = nil }), 200, false},
		{"status-bool-code", change(status, func(o map[string]any) { o["code"] = false }), 200, false},
		{"status-float-code", bytes.Replace(status, []byte(`"status":"Success"`), []byte(`"status":"Success","code":0.0`), 1), 200, false},
		{"status-nonzero-code", change(status, func(o map[string]any) { o["code"] = 200 }), 200, false},
		{"status-extra-key", change(status, func(o map[string]any) { o["unexpected"] = true }), 200, false},
		{"status-duplicate-kind", bytes.Replace(status, []byte(`"kind":"Status"`), []byte(`"kind":"Status","kind":"Status"`), 1), 200, false},
		{"status-pending-202", status, 202, false},
		{"object-exact", object, 200, true},
		{"object-new-opaque-rv", change(object, func(o map[string]any) { o["metadata"].(map[string]any)["resourceVersion"] = "75" }), 200, true},
		{"object-missing-rv", change(object, func(o map[string]any) { delete(o["metadata"].(map[string]any), "resourceVersion") }), 200, false},
		{"object-uid-drift", change(object, func(o map[string]any) { o["metadata"].(map[string]any)["uid"] = "foreign" }), 200, false},
		{"object-data-drift", change(object, func(o map[string]any) { o["data"].(map[string]any)["intent"] = "foreign" }), 200, false},
		{"object-extra-key", change(object, func(o map[string]any) { o["unexpected"] = true }), 200, false},
		{"object-duplicate-kind", bytes.Replace(object, []byte(`"kind":"ConfigMap"`), []byte(`"kind":"ConfigMap","kind":"ConfigMap"`), 1), 200, false},
		{"object-pending-202", object, 202, false},
		{"null", []byte(`null`), 200, false},
		{"malformed", []byte(`{"kind":`), 200, false},
	}
	uid := cm.UID
	options, _ := json.Marshal(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wires := 0
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				wires++
				return &http.Response{StatusCode: tc.code, Body: io.NopCloser(bytes.NewReader(tc.body))}, nil
			}))
			g.owned[path], g.receiptObjects[path] = string(uid), cm
			_, err := g.RoundTrip(capacityGateRequest("DELETE", path, options))
			if (err == nil) != tc.accept || g.unknown != !tc.accept || wires != 1 {
				t.Fatalf("accepted=%t unknown=%t wires=%d error=%v", err == nil, g.unknown, wires, err)
			}
			if !tc.accept {
				_, _ = g.RoundTrip(capacityGateRequest("GET", path, nil))
				if wires != 1 {
					t.Fatal("unknown DELETE ACK allowed another wire")
				}
			}
		})
	}
}

func TestCapacityTransportDeletePrewireNeedsOwnedReceiptAndExactUID(t *testing.T) {
	m := capacityGateManifest()
	cm := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "kova-grant-intent-" + strings.Repeat("c", 32), Namespace: m.Receipts.Name, UID: "receipt-original", ResourceVersion: "74"}}
	path := "/api/v1/namespaces/" + cm.Namespace + "/configmaps/" + cm.Name
	for _, tc := range []struct{ name, owned, precondition string }{
		{"no-owned-uid", "", ""},
		{"owned-mismatch", "foreign", "foreign"},
		{"precondition-mismatch", string(cm.UID), "foreign"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wires := 0
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				wires++
				return nil, errors.New("must not reach wire")
			}))
			g.receiptObjects[path], g.owned[path] = cm, tc.owned
			uid := types.UID(tc.precondition)
			options, _ := json.Marshal(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			if _, err := g.RoundTrip(capacityGateRequest("DELETE", path, options)); err == nil || wires != 0 {
				t.Fatal("unowned or mismatched receipt DELETE reached wire")
			}
		})
	}
}

func TestCapacityTransportRejectsOversizeRedirectThrottleAndBudget(t *testing.T) {
	for _, status := range []int{200, 302, 429, 503} {
		g := newCapacityTransport(capacityGateManifest(), admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 2*1024*1024+1)))}, nil
		}))
		if _, err := g.RoundTrip(capacityGateRequest("GET", "/api/v1/namespaces/kube-system", nil)); err == nil || !g.unknown {
			t.Fatal("unknown response accepted")
		}
	}
	g := newCapacityTransport(capacityGateManifest(), admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) { t.Fatal("exhausted budget forwarded"); return nil, nil }))
	g.calls = capacityCallLimit
	if _, err := g.RoundTrip(capacityGateRequest("GET", "/api/v1/namespaces/kube-system", nil)); err == nil {
		t.Fatal("budget ignored")
	}
}

func TestCapacityTransportJournalFailureStopsAfterOriginalAssignment(t *testing.T) {
	m := capacityGateManifest()
	name := admissioncontract.ActiveLedgerName
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps"
	cm := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.Runner.Name, UID: "assigned-original", ResourceVersion: "1"}}
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
		raw, _ := json.Marshal(cm)
		return &http.Response{StatusCode: 201, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}))
	g.assigned = func(gotPath, uid, digest string) error {
		if gotPath != path+"/"+name || uid != "assigned-original" || digest != "" {
			t.Fatal("assignment differs from response")
		}
		return errors.New("fsync failed")
	}
	proposal := cm.DeepCopy()
	proposal.UID = ""
	proposal.ResourceVersion = ""
	raw, _ := json.Marshal(proposal)
	if _, err := g.RoundTrip(capacityGateRequest("POST", path, raw)); err == nil || !g.unknown {
		t.Fatal("assignment persisted without journal")
	}
	if _, err := g.RoundTrip(capacityGateRequest("GET", path+"/"+name, nil)); err == nil || g.wireCalls != 1 || g.wireMutations != 1 || g.wirePodPosts != 0 {
		t.Fatal("later wire followed failed assignment journal")
	}
}

func TestCapacityTransportPatchOnlyOriginalFixedData(t *testing.T) {
	m := capacityGateManifest()
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
	for _, extra := range []string{"", `,{"op":"add","path":"/metadata/labels/foreign","value":"x"}`} {
		calls := 0
		original := capacityGateLedger(m)
		ack := original.DeepCopy()
		ack.ResourceVersion = "2"
		ack.Data[admissioncontract.ActiveLedgerDataKey] = "new"
		response, _ := json.Marshal(ack)
		g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(response))}, nil
		}))
		g.patches[path] = "original"
		raw, _ := json.Marshal(original)
		g.observed[path], _ = capacityObject(raw, path, "original")
		body := bytes.Replace(capacityGatePatch(), []byte(`}]`), []byte(`}`+extra+`]`), 1)
		r := capacityGateRequest("PATCH", path, body)
		r.Header.Set("Content-Type", "application/json-patch+json")
		_, err := g.RoundTrip(r)
		if (err == nil) != (extra == "") || calls != map[bool]int{true: 1, false: 0}[extra == ""] {
			t.Fatalf("fixed data patch guard failed: %v", err)
		}
	}
}

func capacityGateLedger(m capacityManifest) *corev1.ConfigMap {
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: admissioncontract.ActiveLedgerName, Namespace: m.Runner.Name, UID: "original", ResourceVersion: "1"}, Data: map[string]string{admissioncontract.ActiveLedgerDataKey: "old"}}
}

func capacityGatePatch() []byte {
	return bytes.ReplaceAll([]byte(`[{"op":"test","path":"/metadata/uid","value":"original"},{"op":"test","path":"/metadata/resourceVersion","value":"1"},{"op":"test","path":"/data/state","value":"old"},{"op":"replace","path":"/data/state","value":"new"}]`), []byte("/data/state"), []byte("/data/"+admissioncontract.ActiveLedgerDataKey))
}

func capacityOfficialDeleteStatus(t *testing.T, receipt *corev1.ConfigMap) []byte {
	t.Helper()
	// Mirrors generic Store.finalizeDelete for a core/v1 ConfigMap when
	// ReturnDeletedObject is false. The API serializer supplies TypeMeta.
	status := &metav1.Status{Status: metav1.StatusSuccess, Details: &metav1.StatusDetails{Name: receipt.Name, Kind: "configmaps", UID: receipt.UID}}
	raw, err := runtime.Encode(clientgoscheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion), status)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if !capacityDecode(raw, &object) || object["apiVersion"] != "v1" || object["kind"] != "Status" || object["status"] != metav1.StatusSuccess || object["code"] != nil {
		t.Fatal("official Status serializer changed its success envelope")
	}
	return raw
}

func capacityOfficialGeneric422(t *testing.T) []byte {
	t.Helper()
	status := apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", "synthetic failed test, never a live response", 0, false)
	raw, err := runtime.Encode(clientgoscheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion), &status.ErrStatus)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if !capacityDecode(raw, &decoded) || len(decoded["details"].(map[string]any)) != 0 || len(decoded["metadata"].(map[string]any)) != 0 ||
		decoded["message"] != "the server rejected our request due to an error in our request" {
		t.Fatal("official constructor/serializer changed its generic 422 contract")
	}
	return raw
}

func TestCapacityGenericLedger422OfficialConstructorAndTypedPatch(t *testing.T) {
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		t.Run(string(role), func(t *testing.T) {
			m := capacityGateManifest()
			ledger, key := capacityGateLedger(m), admissioncontract.ActiveLedgerDataKey
			if role == admissioncontract.Queue {
				ledger.Name, key = admissioncontract.QueueLedgerName, admissioncontract.QueueLedgerDataKey
				ledger.Data = map[string]string{key: "old"}
			}
			path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + ledger.Name
			raw := capacityOfficialGeneric422(t)
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPatch || r.URL.Path != path {
					t.Fatal("typed PATCH escaped its declared original ledger")
				}
				return &http.Response{StatusCode: 422, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			g.patches[path], g.owned[path] = "original", "original"
			original, _ := json.Marshal(ledger)
			g.observed[path], _ = capacityObject(original, path, "original")
			cfg := capacityClientConfig(&rest.Config{Host: g.origin, Timeout: 10 * time.Second,
				ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"}})
			cs, err := kubernetes.NewForConfigAndClient(cfg, &http.Client{Transport: g, Timeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			body := bytes.ReplaceAll(capacityGatePatch(), []byte("/data/"+admissioncontract.ActiveLedgerDataKey), []byte("/data/"+key))
			_, err = cs.CoreV1().ConfigMaps(m.Runner.Name).Patch(t.Context(), ledger.Name, types.JSONPatchType, body, metav1.PatchOptions{})
			if !apierrors.IsInvalid(err) || apierrors.IsConflict(err) || g.unknown || g.wireCalls != 1 || g.wireMutations != 1 || g.wirePodPosts != 0 {
				t.Fatal("official generic 422 was lost, misclassified or sent more than once")
			}
			if _, err := cs.CoreV1().ConfigMaps(m.Runner.Name).Patch(t.Context(), ledger.Name, types.JSONPatchType, body, metav1.PatchOptions{}); err == nil || g.wireCalls != 1 {
				t.Fatal("the rejected original PATCH body was replayed")
			}
		})
	}
}

func TestCapacityGenericLedger422RejectsForeignStatusAndStopsWire(t *testing.T) {
	for _, scenario := range []string{"causes", "empty-causes", "retry", "zero-retry", "name", "group", "kind", "uid", "unknown-detail", "foreign-metadata", "null-metadata", "missing-metadata", "message", "code", "reason", "gvk", "success", "unknown-top", "duplicate", "non-owned", "changed-owned", "genesis", "get"} {
		t.Run(scenario, func(t *testing.T) {
			m := capacityGateManifest()
			ledger := capacityGateLedger(m)
			key := admissioncontract.ActiveLedgerDataKey
			if scenario == "genesis" {
				ledger.Name, key = admissioncontract.GenesisName, admissioncontract.GenesisDataKey
				ledger.Data = map[string]string{key: "old"}
			}
			path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + ledger.Name
			var status map[string]any
			if !capacityDecode(capacityOfficialGeneric422(t), &status) {
				t.Fatal("invalid test fixture")
			}
			details := status["details"].(map[string]any)
			switch scenario {
			case "causes":
				details["causes"] = []any{map[string]any{"reason": "FieldValueInvalid", "field": "patch", "message": "synthetic"}}
			case "empty-causes":
				details["causes"] = []any{}
			case "retry":
				details["retryAfterSeconds"] = 1
			case "zero-retry":
				details["retryAfterSeconds"] = 0
			case "name", "group", "kind", "uid", "unknown-detail":
				details[scenario] = "foreign"
			case "foreign-metadata":
				status["metadata"] = map[string]any{"resourceVersion": "foreign"}
			case "null-metadata":
				status["metadata"] = nil
			case "missing-metadata":
				delete(status, "metadata")
			case "message":
				status["message"] = "another rejection"
			case "code":
				status["code"] = 409
			case "reason":
				status["reason"] = "Conflict"
			case "gvk":
				status["apiVersion"] = "foreign/v1"
			case "success":
				status["status"] = "Success"
			case "unknown-top":
				status["foreign"] = true
			}
			raw, _ := json.Marshal(status)
			if scenario == "duplicate" {
				raw = bytes.Replace(raw, []byte(`"code":422`), []byte(`"code":422,"code":422`), 1)
			}
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 422, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			g.patches[path], g.owned[path] = "original", "original"
			if scenario == "non-owned" {
				delete(g.owned, path)
			} else if scenario == "changed-owned" {
				g.owned[path] = "replacement"
			}
			original, _ := json.Marshal(ledger)
			g.observed[path], _ = capacityObject(original, path, "original")
			body := bytes.ReplaceAll(capacityGatePatch(), []byte("/data/"+admissioncontract.ActiveLedgerDataKey), []byte("/data/"+key))
			request := capacityGateRequest(http.MethodPatch, path, body)
			request.Header.Set("Content-Type", "application/json-patch+json")
			if scenario == "get" {
				request = capacityGateRequest(http.MethodGet, path, nil)
			}
			if _, err := g.RoundTrip(request); err == nil || !g.unknown || g.wireCalls != 1 {
				t.Fatal("foreign/non-owned generic 422 qualified")
			}
			if _, err := g.RoundTrip(capacityGateRequest(http.MethodGet, path, nil)); err == nil || g.wireCalls != 1 || g.wirePodPosts != 0 {
				t.Fatal("wire continued after unknown error response")
			}
		})
	}
}

func TestCapacityGenericLedger422DoesNotChangeLegacyNilOrConflictContracts(t *testing.T) {
	m := capacityGateManifest()
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", nil)
	request := capacityGateRequest(http.MethodPatch, path, capacityGatePatch())
	request.Header.Set("Content-Type", "application/json-patch+json")
	for _, details := range []string{"null", "missing"} {
		var status map[string]any
		_ = capacityDecode(capacityOfficialGeneric422(t), &status)
		if details == "null" {
			status["details"] = nil
		} else {
			delete(status, "details")
		}
		raw, _ := json.Marshal(status)
		// Existing nil/absent/null acceptance remains; no owned identity is
		// installed here, so it cannot come from the new owned-ledger branch.
		if !g.qualifyResponse(request, 422, raw) {
			t.Fatal("legacy nil-details error contract was silently tightened")
		}
	}
	conflict := apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, admissioncontract.ActiveLedgerName, errors.New("synthetic"))
	raw, err := runtime.Encode(clientgoscheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion), &conflict.ErrStatus)
	if err != nil || !g.qualifyResponse(request, 409, raw) {
		t.Fatal("original named-resource 409 contract changed")
	}
}

func TestCapacityGenericLedger422CannotBypassClosedPatchInput(t *testing.T) {
	for _, scenario := range []string{"foreign-uid", "null-uid", "empty-rv", "numeric-rv", "null-rv", "missing-old-data", "missing-proposed-data", "null-old-data", "null-proposed-data", "numeric-proposed-data", "malformed", "foreign-operation", "wrong-content-type", "foreign-path"} {
		t.Run(scenario, func(t *testing.T) {
			m := capacityGateManifest()
			path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				t.Fatal("unqualified input reached the generic-422 upstream")
				return nil, nil
			}))
			g.patches[path], g.owned[path] = "original", "original"
			body := capacityGatePatch()
			switch scenario {
			case "foreign-uid":
				body = bytes.Replace(body, []byte(`"value":"original"`), []byte(`"value":"foreign"`), 1)
			case "null-uid":
				body = bytes.Replace(body, []byte(`"value":"original"`), []byte(`"value":null`), 1)
			case "empty-rv":
				body = bytes.Replace(body, []byte(`"value":"1"`), []byte(`"value":""`), 1)
			case "numeric-rv":
				body = bytes.Replace(body, []byte(`"value":"1"`), []byte(`"value":1`), 1)
			case "null-rv":
				body = bytes.Replace(body, []byte(`"value":"1"`), []byte(`"value":null`), 1)
			case "missing-old-data":
				var operations []json.RawMessage
				_ = json.Unmarshal(body, &operations)
				body, _ = json.Marshal(append(operations[:2], operations[3:]...))
			case "missing-proposed-data":
				var operations []json.RawMessage
				_ = json.Unmarshal(body, &operations)
				body, _ = json.Marshal(operations[:3])
			case "null-old-data":
				body = bytes.Replace(body, []byte(`"value":"old"`), []byte(`"value":null`), 1)
			case "null-proposed-data":
				body = bytes.Replace(body, []byte(`"value":"new"`), []byte(`"value":null`), 1)
			case "numeric-proposed-data":
				body = bytes.Replace(body, []byte(`"value":"new"`), []byte(`"value":1`), 1)
			case "malformed":
				body = body[:len(body)-1]
			case "foreign-operation":
				body = bytes.Replace(body, []byte(`"op":"replace"`), []byte(`"op":"add"`), 1)
			case "foreign-path":
				path += "-foreign"
			}
			request := capacityGateRequest(http.MethodPatch, path, body)
			request.Header.Set("Content-Type", "application/json-patch+json")
			if scenario == "wrong-content-type" {
				request.Header.Set("Content-Type", "application/merge-patch+json")
			}
			if _, err := g.RoundTrip(request); err == nil || g.wireCalls != 0 || g.wireMutations != 0 {
				t.Fatal("new error compatibility bypassed existing CAS/input guards")
			}
		})
	}
}

type capacity422ReadbackAPI struct {
	admissiongenesis.CoreAPI
	target, mode, changedData string
	patches                   int
	patchError                error
}

func (a *capacity422ReadbackAPI) PatchConfigMap(context.Context, string, string, []byte) (*corev1.ConfigMap, error) {
	a.patches++
	return nil, a.patchError
}

func (a *capacity422ReadbackAPI) GetConfigMap(ctx context.Context, ns, name string) (*corev1.ConfigMap, error) {
	cm, err := a.CoreAPI.GetConfigMap(ctx, ns, name)
	if err != nil || a.patches == 0 || name != a.target {
		return cm, err
	}
	switch a.mode {
	case "advanced-original":
		cm.ResourceVersion += "-advanced"
	case "advanced-valid-data":
		cm.ResourceVersion += "-advanced"
		cm.Data[admissioncontract.ActiveLedgerDataKey] = a.changedData
	case "replacement":
		cm.UID += "-replacement"
	case "missing":
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
	case "invalid-data":
		cm.Data[admissioncontract.ActiveLedgerDataKey] = "{"
	}
	return cm, nil
}

func TestCapacityGenericLedger422StillRequiresAdvancedValidOriginalReadback(t *testing.T) {
	for _, scenario := range []string{"advanced-original", "advanced-valid-data", "replacement", "missing", "invalid-data", "unchanged"} {
		t.Run(scenario, func(t *testing.T) {
			r, api, _ := genesisAdmissionFixtureWithConfig(t, config.Config{MaxActiveJobs: 2, MaxActiveJobsPerRequester: 2, WorkerSlots: 2, MaxQueuedJobs: 2, MaxQueuedJobsPerRequester: 2})
			original, state, err := r.readReservations(t.Context(), r.Cfg.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			state.Fence++
			next, err := encodeReservations(state)
			if err != nil {
				t.Fatal(err)
			}
			var status metav1.Status
			if !capacityDecode(capacityOfficialGeneric422(t), &status) {
				t.Fatal("invalid official fixture")
			}
			observed := &capacity422ReadbackAPI{CoreAPI: api, target: original.Name, mode: scenario, changedData: string(next), patchError: apierrors.FromObject(&status)}
			r.Genesis.Bootstrap.API = observed
			err = r.Genesis.PatchLedgerData(t.Context(), original, admissioncontract.Active, string(next))
			wantConflict := strings.HasPrefix(scenario, "advanced-")
			if err == nil || apierrors.IsConflict(err) != wantConflict || observed.patches != 1 {
				t.Fatal("generic 422 bypassed original UID/body/direct-read qualification or replayed")
			}
			if scenario == "unchanged" && !apierrors.IsInvalid(err) {
				t.Fatal("unchanged original rejection became a conflict or success")
			}
		})
	}
}

func TestCapacityTransportNotFoundMustBeExactNamedStatus(t *testing.T) {
	m := capacityGateManifest()
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
	status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Code: 404, Reason: metav1.StatusReasonNotFound,
		Details: &metav1.StatusDetails{Name: admissioncontract.ActiveLedgerName, Kind: "configmaps"}}
	for _, scenario := range []string{"exact", "empty", "foreign-name", "foreign-kind", "foreign-group", "foreign-reason", "missing-details", "duplicate", "collection"} {
		t.Run(scenario, func(t *testing.T) {
			got := status.DeepCopy()
			requestPath := path
			switch scenario {
			case "foreign-name":
				got.Details.Name = "foreign"
			case "foreign-kind":
				got.Details.Kind = "pods"
			case "foreign-group":
				got.Details.Group = "foreign.example"
			case "foreign-reason":
				got.Reason = metav1.StatusReasonForbidden
			case "missing-details":
				got.Details = nil
			case "collection":
				requestPath = strings.TrimSuffix(path, "/"+admissioncontract.ActiveLedgerName) + "?limit=1"
			}
			raw, _ := json.Marshal(got)
			if scenario == "empty" {
				raw = []byte(`{}`)
			}
			if scenario == "duplicate" {
				raw = append(raw[:len(raw)-1], []byte(`,"code":404}`)...)
			}
			calls := 0
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(raw)), Header: http.Header{"Content-Type": {"application/json"}}}, nil
			}))
			if scenario != "collection" {
				// Exercise the real client-go error decoder: unqualified 404
				// must never become absence authority to EnsureFresh.
				cs, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: g.origin}, &http.Client{Transport: g})
				if err != nil {
					t.Fatal(err)
				}
				_, err = cs.CoreV1().ConfigMaps(m.Runner.Name).Get(context.Background(), admissioncontract.ActiveLedgerName, metav1.GetOptions{})
				if apierrors.IsNotFound(err) != (scenario == "exact") {
					t.Fatalf("404 qualification=%v", err)
				}
			} else if _, err := g.RoundTrip(capacityGateRequest("GET", requestPath, nil)); err == nil {
				t.Fatal("collection 404 treated as empty")
			}
			if g.unknown != (scenario != "exact") {
				t.Fatal("unqualified absence did not poison campaign")
			}
			if g.unknown {
				_, _ = g.RoundTrip(capacityGateRequest("GET", path, nil))
				if calls != 1 {
					t.Fatal("later wire followed unknown response")
				}
			}
		})
	}
}

func TestCapacityTransportMutationAckMatchesProposalAndOriginalUID(t *testing.T) {
	m := capacityGateManifest()
	for _, method := range []string{"PATCH", "PUT"} {
		for _, scenario := range []string{"exact", "empty", "foreign-uid", "changed-body", "changed-metadata", "duplicate", "wrong-code"} {
			t.Run(method+"/"+scenario, func(t *testing.T) {
				g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", nil)
				path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
				old := capacityGateLedger(m)
				ack := old.DeepCopy()
				ack.ResourceVersion = "2"
				ack.Data[admissioncontract.ActiveLedgerDataKey] = "new"
				rawOld, _ := json.Marshal(old)
				g.patches[path] = "original"
				g.observed[path], _ = capacityObject(rawOld, path, "original")
				input := capacityGatePatch()
				var output map[string]any
				raw, _ := json.Marshal(ack)
				_ = capacityDecode(raw, &output)
				if method == "PUT" {
					path = g.buildPath + "/status"
					build := &kovav1.KovaBuild{TypeMeta: metav1.TypeMeta{APIVersion: kovav1.SchemeGroupVersion.String(), Kind: "KovaBuild"}, ObjectMeta: metav1.ObjectMeta{Name: m.BuildName, Namespace: m.Runner.Name, UID: "original", ResourceVersion: "1"}}
					build.Status.Phase = kovav1.PhaseCancelled
					input, _ = json.Marshal(build)
					g.owned[g.buildPath] = "original"
					build.ResourceVersion = "2"
					raw, _ = json.Marshal(build)
					output = nil
					_ = capacityDecode(raw, &output)
				}
				switch scenario {
				case "foreign-uid":
					output["metadata"].(map[string]any)["uid"] = "foreign"
				case "changed-body":
					if method == "PUT" {
						output["status"].(map[string]any)["phase"] = "Succeeded"
					} else {
						output["data"].(map[string]any)[admissioncontract.ActiveLedgerDataKey] = "foreign"
					}
				case "changed-metadata":
					output["metadata"].(map[string]any)["labels"] = map[string]any{"foreign": "true"}
				}
				raw, _ = json.Marshal(output)
				if scenario == "empty" {
					raw = []byte(`{}`)
				}
				if scenario == "duplicate" {
					raw = append(raw[:len(raw)-1], []byte(`,"kind":"`+output["kind"].(string)+`"}`)...)
				}
				calls := 0
				g.upstream = capacityRoundTripper(func(*http.Request) (*http.Response, error) {
					calls++
					code := 200
					if scenario == "wrong-code" {
						code = 201
					}
					return &http.Response{StatusCode: code, Body: io.NopCloser(bytes.NewReader(raw))}, nil
				})
				r := capacityGateRequest(method, path, input)
				if method == "PATCH" {
					r.Header.Set("Content-Type", "application/json-patch+json")
				}
				_, err := g.RoundTrip(r)
				if (err == nil) != (scenario == "exact") || g.unknown != (scenario != "exact") {
					t.Fatalf("mutation ACK %s qualification=%v", scenario, err)
				}
				if g.unknown {
					_, _ = g.RoundTrip(capacityGateRequest("GET", g.buildPath, nil))
					if calls != 1 {
						t.Fatal("unknown ACK followed by another wire")
					}
				}
			})
		}
	}
}

func TestCapacityTransportDuplicateCreateAndDeleteCannotQualify(t *testing.T) {
	m := capacityGateManifest()
	name := "kova-grant-intent-" + strings.Repeat("a", 32)
	collection := "/api/v1/namespaces/" + m.Receipts.Name + "/configmaps"
	cm := capacityGateLedger(m)
	cm.Name, cm.Namespace = name, m.Receipts.Name
	for _, method := range []string{"POST", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			raw, _ := json.Marshal(cm)
			raw = append(raw[:len(raw)-1], []byte(`,"kind":"ConfigMap"}`)...)
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				code := 200
				if method == "POST" {
					code = 201
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			path := collection
			proposal := cm.DeepCopy()
			proposal.UID, proposal.ResourceVersion = "", ""
			input, _ := json.Marshal(proposal)
			if method == "DELETE" {
				path += "/" + name
				g.owned[path], g.receiptObjects[path] = string(cm.UID), cm
				uid := cm.UID
				input, _ = json.Marshal(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			}
			if _, err := g.RoundTrip(capacityGateRequest(method, path, input)); err == nil || !g.unknown {
				t.Fatal("duplicate mutation ACK accepted")
			}
		})
	}
}

func TestCapacityTransportRejectsReplacementPinnedReadAndEmptySuccess(t *testing.T) {
	m := capacityGateManifest()
	for _, raw := range [][]byte{[]byte(`{}`), []byte(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"` + m.Runner.Name + `","uid":"replacement","resourceVersion":"1"}}`)} {
		g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw))}, nil
		}))
		if _, err := g.RoundTrip(capacityGateRequest("GET", "/api/v1/namespaces/"+m.Runner.Name, nil)); err == nil || !g.unknown {
			t.Fatal("unqualified pinned read accepted")
		}
	}
}

func TestCapacitySingleWireTransportHasNoHiddenRetryBranches(t *testing.T) {
	upstream, err := capacitySingleWireTransport(&rest.Config{TLSClientConfig: rest.TLSClientConfig{ServerName: "127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.CloseIdleConnections()
	if upstream.Proxy != nil || !upstream.DisableKeepAlives || upstream.ForceAttemptHTTP2 || upstream.TLSNextProto == nil || len(upstream.TLSNextProto) != 0 || upstream.TLSClientConfig.MinVersion != tls.VersionTLS12 || upstream.DialContext == nil {
		t.Fatal("capacity upstream permits proxy, reused-connection or HTTP2 retries")
	}
}

func TestCapacityTransportCreateStatusUsesOnlyEmptyCRDResetForms(t *testing.T) {
	m := capacityGateManifest()
	for _, scenario := range []string{"empty-omitted", "assigned-status", "changed-spec", "wrong-code"} {
		t.Run(scenario, func(t *testing.T) {
			build := &kovav1.KovaBuild{TypeMeta: metav1.TypeMeta{APIVersion: kovav1.SchemeGroupVersion.String(), Kind: "KovaBuild"}, ObjectMeta: metav1.ObjectMeta{Name: m.BuildName, Namespace: m.Runner.Name},
				Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "oci"}}}
			input, _ := json.Marshal(build)
			build.UID, build.ResourceVersion = "assigned-original", "1"
			raw, _ := json.Marshal(build)
			var output map[string]any
			_ = capacityDecode(raw, &output)
			// Actual CRD create preparation removes even typed status:{}.
			delete(output, "status")
			if scenario == "assigned-status" {
				output["status"] = map[string]any{"phase": "Cancelled"}
			}
			if scenario == "changed-spec" {
				output["spec"].(map[string]any)["build"] = map[string]any{"format": "nydus"}
			}
			raw, _ = json.Marshal(output)
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				code := 201
				if scenario == "wrong-code" {
					code = 200
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			path := strings.TrimSuffix(g.buildPath, "/"+m.BuildName)
			_, err := g.RoundTrip(capacityGateRequest("POST", path, input))
			if (err == nil) != (scenario == "empty-omitted") || g.unknown != (scenario != "empty-omitted") {
				t.Fatalf("CRD create ACK %s=%v", scenario, err)
			}
		})
	}
}

func TestCapacityTransportRejectsImpossibleSuccessfulCAS(t *testing.T) {
	m := capacityGateManifest()
	path := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + admissioncontract.ActiveLedgerName
	old := capacityGateLedger(m)
	ack := old.DeepCopy()
	ack.ResourceVersion, ack.Data[admissioncontract.ActiveLedgerDataKey] = "2", "new"
	raw, _ := json.Marshal(ack)
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}))
	g.patches[path] = "original"
	old.Data[admissioncontract.ActiveLedgerDataKey] = "different-original"
	rawOld, _ := json.Marshal(old)
	g.observed[path], _ = capacityObject(rawOld, path, "original")
	r := capacityGateRequest("PATCH", path, capacityGatePatch())
	r.Header.Set("Content-Type", "application/json-patch+json")
	if _, err := g.RoundTrip(r); err == nil || !g.unknown {
		t.Fatal("successful ACK accepted despite failed original data test")
	}
}

func TestCapacityTypedClientKeepsTenSecondTimeoutAndExactQuery(t *testing.T) {
	m := capacityGateManifest()
	ledger := capacityGateLedger(m)
	receipt := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "kova-grant-intent-" + strings.Repeat("a", 32), Namespace: m.Receipts.Name, UID: "receipt-original", ResourceVersion: "1"}, Data: map[string]string{"intent": "fixed"}}
	ledgerPath := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/" + ledger.Name
	receiptPath := "/api/v1/namespaces/" + m.Receipts.Name + "/configmaps/" + receipt.Name
	var requests []*http.Request
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Clone(context.Background()))
		if r.Header.Get("Accept") != "application/json" {
			t.Fatal("typed client changed JSON negotiation")
		}
		var object any
		code := http.StatusOK
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/"+m.Runner.Name:
			object = &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
				ObjectMeta: metav1.ObjectMeta{Name: m.Runner.Name, UID: types.UID(m.Runner.UID), ResourceVersion: "1", CreationTimestamp: metav1.Now()}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/pods"):
			object = &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.Pod{}}
		case r.Method == "POST" && r.URL.Path == strings.TrimSuffix(ledgerPath, "/"+ledger.Name):
			object, code = ledger, http.StatusCreated
		case r.Method == "GET" && r.URL.Path == ledgerPath:
			object = ledger
		case r.Method == "PATCH" && r.URL.Path == ledgerPath:
			ack := ledger.DeepCopy()
			ack.ResourceVersion, ack.Data[admissioncontract.ActiveLedgerDataKey] = "2", "new"
			object = ack
		case r.Method == "POST" && r.URL.Path == strings.TrimSuffix(receiptPath, "/"+receipt.Name):
			object, code = receipt, http.StatusCreated
		case r.Method == "DELETE" && r.URL.Path == receiptPath:
			object = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess,
				Details: &metav1.StatusDetails{Name: receipt.Name, Kind: "configmaps", UID: receipt.UID}}
		default:
			t.Fatal("undeclared operation reached mock upstream")
		}
		raw, _ := json.Marshal(object)
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}))
	httpClient := &http.Client{Transport: g, Timeout: 10 * time.Second}
	cs, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: g.origin, Timeout: 10 * time.Second,
		ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"}}, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ns, err := cs.CoreV1().Namespaces().Get(ctx, m.Runner.Name, metav1.GetOptions{})
	if err != nil || capacityNamespaceReason(ns, m.Runner, m.PreparedAfter, time.Now(), err) != "" {
		t.Fatal("real typed Namespace GET failed bounded identity qualification")
	}
	if _, err := cs.CoreV1().Pods(m.Runner.Name).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	proposal := ledger.DeepCopy()
	proposal.UID, proposal.ResourceVersion = "", ""
	if _, err := cs.CoreV1().ConfigMaps(m.Runner.Name).Create(ctx, proposal, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ConfigMaps(m.Runner.Name).Get(ctx, ledger.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ConfigMaps(m.Runner.Name).Patch(ctx, ledger.Name, types.JSONPatchType, capacityGatePatch(), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	proposal = receipt.DeepCopy()
	proposal.UID, proposal.ResourceVersion = "", ""
	if _, err := cs.CoreV1().ConfigMaps(m.Receipts.Name).Create(ctx, proposal, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	uid := receipt.UID
	if err := cs.CoreV1().ConfigMaps(m.Receipts.Name).Delete(ctx, receipt.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 7 || httpClient.Timeout != 10*time.Second || g.wirePodPosts != 0 {
		t.Fatal("typed client lost timeout or expanded write scope")
	}
	for i, r := range requests {
		want := "timeout=10s"
		if i == 1 {
			// Generated List explicitly calls Request.Timeout(0) when
			// TimeoutSeconds is nil; http.Client still enforces 10s.
			want = "limit=1"
		}
		if r.URL.RawQuery != want {
			t.Fatalf("typed request %d has noncanonical query", i)
		}
	}
	if _, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1}); err == nil || len(requests) != 7 {
		t.Fatal("cluster-wide Namespace list expanded declared fixture")
	}
}

func TestCapacityTransportQueryClosedSetRejectsAmbiguityBeforeWire(t *testing.T) {
	m := capacityGateManifest()
	named := "/api/v1/namespaces/" + m.Runner.Name
	collection := named + "/pods"
	for _, target := range []struct{ method, path string }{{"GET", named}, {"GET", collection}, {"POST", collection}, {"PATCH", named}, {"PUT", named}, {"DELETE", named}} {
		for _, query := range []string{"watch=true", "labelSelector=x", "limit=2", "timeout=11s", "timeout=10000ms", "timeout=10s&timeout=10s", "%74imeout=10s", "timeout=10%73", "timeout=10s&", "timeout=10s&limit=1", "limit=1&limit=1", "limit=1&timeout=10s&watch=false"} {
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) { t.Fatal("ambiguous query forwarded"); return nil, nil }))
			r := capacityGateRequest(target.method, target.path, []byte(`{}`))
			r.URL.RawQuery = query
			if _, err := g.RoundTrip(r); err == nil || g.wireCalls != 0 || g.mutations != 0 {
				t.Fatal("query expanded upstream authority")
			}
		}
	}
	for _, query := range []string{"limit=1", "limit=1&timeout=10s"} {
		if capacityQueryAllowed(capacityGateRequest("GET", named+"?"+query, nil)) {
			t.Fatal("named read accepted collection limit")
		}
	}
	if capacityQueryAllowed(capacityGateRequest("GET", collection, nil)) {
		t.Fatal("unbounded collection read accepted")
	}
	for _, query := range []string{"limit=1", "limit=1&timeout=10s"} {
		if !capacityQueryAllowed(capacityGateRequest("GET", collection+"?"+query, nil)) {
			t.Fatal("exact bounded collection query rejected")
		}
	}
}

func TestCapacityNamespaceFailureReasonsAreFixedAndKeepIdentityGates(t *testing.T) {
	m := capacityGateManifest()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{UID: types.UID(m.Runner.UID), CreationTimestamp: metav1.Now()}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	for _, scenario := range []string{"exact", "read", "nil", "uid", "deleting", "phase", "old", "future"} {
		copy := ns.DeepCopy()
		var readErr error
		want := ""
		switch scenario {
		case "read":
			readErr, want = errors.New("secret/raw API payload"), "NAMESPACE_READ_UNKNOWN"
		case "nil":
			copy, want = nil, "NAMESPACE_READ_UNKNOWN"
		case "uid":
			copy.UID, want = "replacement", "NAMESPACE_UID_CHANGED"
		case "deleting":
			now := metav1.Now()
			copy.DeletionTimestamp, want = &now, "NAMESPACE_DELETING"
		case "phase":
			copy.Status.Phase, want = corev1.NamespaceTerminating, "NAMESPACE_NOT_ACTIVE"
		case "old":
			copy.CreationTimestamp, want = metav1.NewTime(m.PreparedAfter.Add(-time.Second)), "NAMESPACE_FRESHNESS_CHANGED"
		case "future":
			copy.CreationTimestamp, want = metav1.NewTime(time.Now().Add(time.Minute)), "NAMESPACE_FRESHNESS_CHANGED"
		}
		if got := capacityNamespaceReason(copy, m.Runner, m.PreparedAfter, time.Now(), readErr); got != want {
			t.Fatalf("namespace failure reason for %s changed", scenario)
		}
	}
}

func capacityGateRootCAList(m capacityManifest) *corev1.ConfigMapList {
	return &corev1.ConfigMapList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMapList"},
		ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.ConfigMap{{
			ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: m.Receipts.Name,
				UID: "11111111-1111-4111-8111-111111111111", ResourceVersion: "1"},
			Data: map[string]string{"ca.crt": "synthetic-root-ca"}}}}
}

func TestCapacityGoEncodedConfigMapListOmitsItemTypeMeta(t *testing.T) {
	m := capacityGateManifest()
	list := capacityGateRootCAList(m)
	raw, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if !capacityDecode(raw, &body) {
		t.Fatal("Go list encoding cannot decode")
	}
	item := body["items"].([]any)[0].(map[string]any)
	if _, present := item["apiVersion"]; present {
		t.Fatal("fixture unexpectedly includes item apiVersion")
	}
	if _, present := item["kind"]; present {
		t.Fatal("fixture unexpectedly includes item kind")
	}
	g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", nil)
	request := capacityGateRequest("GET", "/api/v1/namespaces/"+m.Receipts.Name+"/configmaps?limit=1", nil)
	if !g.qualifyResponse(request, http.StatusOK, raw) {
		t.Fatal("typed List parent qualifies but its absent item TypeMeta does not")
	}
}

func TestCapacityListItemTypeMetaInheritanceStaysStrict(t *testing.T) {
	m := capacityGateManifest()
	base, _ := json.Marshal(capacityGateRootCAList(m))
	path := "/api/v1/namespaces/" + m.Receipts.Name + "/configmaps?limit=1"
	for _, scenario := range []struct {
		name string
		edit func(map[string]any, map[string]any, map[string]any)
		good bool
	}{
		{"absent", func(_, _, _ map[string]any) {}, true},
		{"both-explicit-exact", func(_, item, _ map[string]any) { item["apiVersion"], item["kind"] = "v1", "ConfigMap" }, true},
		{"version-only", func(_, item, _ map[string]any) { item["apiVersion"] = "v1" }, true},
		{"kind-only", func(_, item, _ map[string]any) { item["kind"] = "ConfigMap" }, true},
		{"null-version", func(_, item, _ map[string]any) { item["apiVersion"] = nil }, false},
		{"null-kind", func(_, item, _ map[string]any) { item["kind"] = nil }, false},
		{"empty-version", func(_, item, _ map[string]any) { item["apiVersion"] = "" }, false},
		{"empty-kind", func(_, item, _ map[string]any) { item["kind"] = "" }, false},
		{"foreign-version", func(_, item, _ map[string]any) { item["apiVersion"] = "apps/v1" }, false},
		{"foreign-kind", func(_, item, _ map[string]any) { item["kind"] = "Secret" }, false},
		{"scalar-version", func(_, item, _ map[string]any) { item["apiVersion"] = 1 }, false},
		{"object-kind", func(_, item, _ map[string]any) { item["kind"] = map[string]any{} }, false},
		{"parent-missing-kind", func(parent, _, _ map[string]any) { delete(parent, "kind") }, false},
		{"parent-null-version", func(parent, _, _ map[string]any) { parent["apiVersion"] = nil }, false},
		{"parent-foreign-kind", func(parent, _, _ map[string]any) { parent["kind"] = "SecretList" }, false},
		{"foreign-namespace", func(_, _, meta map[string]any) { meta["namespace"] = "default" }, false},
		{"missing-namespace", func(_, _, meta map[string]any) { delete(meta, "namespace") }, false},
		{"empty-name", func(_, _, meta map[string]any) { meta["name"] = "" }, false},
		{"foreign-path-name", func(_, _, meta map[string]any) { meta["name"] = "foreign/path" }, false},
		{"missing-uid", func(_, _, meta map[string]any) { delete(meta, "uid") }, false},
		{"null-uid", func(_, _, meta map[string]any) { meta["uid"] = nil }, false},
		{"missing-rv", func(_, _, meta map[string]any) { delete(meta, "resourceVersion") }, false},
		{"empty-rv", func(_, _, meta map[string]any) { meta["resourceVersion"] = "" }, false},
		{"continue", func(parent, _, _ map[string]any) { parent["metadata"].(map[string]any)["continue"] = "next" }, false},
		{"remaining", func(parent, _, _ map[string]any) { parent["metadata"].(map[string]any)["remainingItemCount"] = 1 }, false},
		{"two-items", func(parent, item, _ map[string]any) { parent["items"] = []any{item, item} }, false},
		{"nonobject-item", func(parent, _, _ map[string]any) { parent["items"] = []any{nil} }, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var parent map[string]any
			_ = json.Unmarshal(base, &parent)
			item := parent["items"].([]any)[0].(map[string]any)
			scenario.edit(parent, item, item["metadata"].(map[string]any))
			raw, _ := json.Marshal(parent)
			calls := 0
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			response, err := g.RoundTrip(capacityGateRequest("GET", path, nil))
			if (err == nil) != scenario.good || calls != 1 || g.unknown == scenario.good || g.wireMutations != 0 {
				t.Fatal("List inheritance changed strict qualification or write scope")
			}
			if scenario.good {
				observed, _ := io.ReadAll(response.Body)
				if !bytes.Equal(observed, raw) {
					t.Fatal("inherited item GVK leaked into raw API response")
				}
			} else if _, err := g.RoundTrip(capacityGateRequest("GET", path, nil)); err == nil || calls != 1 {
				t.Fatal("unqualified List did not stop further wire")
			}
		})
	}
	for _, raw := range [][]byte{
		[]byte(strings.Replace(string(base), `"kind":"ConfigMapList"`, `"kind":"ConfigMapList","kind":"ConfigMapList"`, 1)),
		[]byte(strings.Replace(string(base), `"name":"kube-root-ca.crt"`, `"name":"kube-root-ca.crt","name":"kube-root-ca.crt"`, 1)),
	} {
		g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", nil)
		if g.qualifyResponse(capacityGateRequest("GET", path, nil), http.StatusOK, raw) {
			t.Fatal("duplicate List/item identity qualified")
		}
	}
}

func TestCapacityTypedListStillExposesOldReceiptsAndWork(t *testing.T) {
	m := capacityGateManifest()
	for _, objectName := range []string{"kube-root-ca.crt", "kova-grant-intent-" + strings.Repeat("a", 32)} {
		list := capacityGateRootCAList(m)
		list.Items[0].Name = objectName
		raw, _ := json.Marshal(list)
		g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
		}))
		cs, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: g.origin, ContentConfig: rest.ContentConfig{ContentType: "application/json"}}, &http.Client{Transport: g, Timeout: 10 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cms, err := cs.CoreV1().ConfigMaps(m.Receipts.Name).List(context.Background(), metav1.ListOptions{Limit: 1})
		if err != nil || len(cms.Items) != 1 || cms.Items[0].UID != list.Items[0].UID || cms.Items[0].ResourceVersion != list.Items[0].ResourceVersion || cms.Items[0].Data["ca.crt"] != "synthetic-root-ca" {
			t.Fatal("real typed List did not preserve item identity/data")
		}
		// Keep the actual receipt preflight rule: no old effect receipt is
		// hidden by a successful parent/item wire qualification.
		veto := err != nil || len(cms.Items) > 1 || cms.Continue != "" || (len(cms.Items) == 1 && cms.Items[0].Name != "kube-root-ca.crt")
		if veto != (objectName != "kube-root-ca.crt") || g.wireMutations != 0 {
			t.Fatal("old receipt bypassed preflight or permitted a write")
		}
	}
	meta := metav1.ObjectMeta{Name: "old-work", Namespace: m.Runner.Name, UID: "22222222-2222-4222-8222-222222222222", ResourceVersion: "1"}
	for _, workload := range []struct {
		path   string
		object any
	}{
		{"/api/v1/namespaces/" + m.Runner.Name + "/pods", &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.Pod{{ObjectMeta: meta}}}},
		{"/api/v1/namespaces/" + m.Runner.Name + "/services", &corev1.ServiceList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.Service{{ObjectMeta: meta}}}},
		{"/apis/apps/v1/namespaces/" + m.Runner.Name + "/deployments", &appsv1.DeploymentList{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DeploymentList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []appsv1.Deployment{{ObjectMeta: meta}}}},
		{"/apis/" + kovav1.SchemeGroupVersion.String() + "/namespaces/" + m.Runner.Name + "/kovabuilds", &kovav1.KovaBuildList{TypeMeta: metav1.TypeMeta{APIVersion: kovav1.SchemeGroupVersion.String(), Kind: "KovaBuildList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []kovav1.KovaBuild{{ObjectMeta: meta}}}},
	} {
		t.Run(workload.path, func(t *testing.T) {
			raw, _ := json.Marshal(workload.object)
			g := newCapacityTransport(m, admissioncontract.Receipt{}, "https://127.0.0.1:6443", capacityRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}))
			response, err := g.RoundTrip(capacityGateRequest("GET", workload.path+"?limit=1", nil))
			if err != nil {
				t.Fatal("valid old-work List lost wire qualification")
			}
			observed, _ := io.ReadAll(response.Body)
			var list struct {
				Metadata metav1.ListMeta `json:"metadata"`
				Items    []struct {
					Metadata metav1.ObjectMeta `json:"metadata"`
				} `json:"items"`
			}
			if !bytes.Equal(raw, observed) || json.Unmarshal(observed, &list) != nil || len(list.Items) != 1 || list.Items[0].Metadata.UID != meta.UID {
				t.Fatal("old workload was dropped or its identity changed")
			}
			// Each original workload preflight vetoes any nonempty List;
			// inheriting missing item tags must not synthesize an empty List.
			veto := err != nil || len(list.Items) != 0 || list.Metadata.Continue != ""
			if !veto || g.wireMutations != 0 || g.wirePodPosts != 0 {
				t.Fatal("nonempty old workload bypassed preflight or permitted a write")
			}
		})
	}
}
