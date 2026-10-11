package buildcontroller

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type capacityRoundTripper func(*http.Request) (*http.Response, error)

func (f capacityRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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
	for _, ack := range []string{"status", "object", "empty", "foreign"} {
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
						raw = []byte(`{"apiVersion":"v1","kind":"Status","status":"Success"}`)
					case "empty":
						raw = nil
					case "foreign":
						copy := original.DeepCopy()
						copy.UID = "replacement"
						raw, _ = json.Marshal(copy)
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
	g.calls = 1500
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
			object = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200}
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
