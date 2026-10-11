package buildcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

const capacityPrefix = "kova-v3-capacity-"

const (
	capacityFailureReserve = 8 * 1024
	capacityCallLimit      = 3500
	capacityClientQPS      = 20
	capacityClientBurst    = 20
)

type capacityRateLimiter struct {
	flowcontrol.RateLimiter
	mu         sync.Mutex
	waitFailed bool
}

func (l *capacityRateLimiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	closed := l.waitFailed
	l.mu.Unlock()
	if closed {
		return errors.New("capacity local rate wait already refused; stop and retain")
	}
	err := l.RateLimiter.Wait(ctx)
	if err != nil {
		l.mu.Lock()
		l.waitFailed = true
		l.mu.Unlock()
	}
	return err
}

func capacityClientConfig(original *rest.Config) *rest.Config {
	cfg := rest.CopyConfig(original)
	// One finite limiter is shared by every typed/controller REST client.
	// Per-resource default 5 QPS buckets cannot fit this exact direct-read
	// lifecycle within its unchanged three-minute context.
	cfg.QPS, cfg.Burst = capacityClientQPS, capacityClientBurst
	cfg.RateLimiter = &capacityRateLimiter{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(cfg.QPS, cfg.Burst)}
	return cfg
}

type capacityNamespace struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// The installer owns the single-node Kind, CRD, three fresh Namespaces,
// Initializing Genesis and immutable receipt Secret. This test never creates
// a Namespace, Service, worker, registry or Pod. In particular, the transport
// prohibits Pod POST; completing the locally never-sent issue is NOT runtime
// cancellation, Pod admission/defaulting, or unknown-Create recovery evidence.
type capacityManifest struct {
	SchemaVersion     int               `json:"schemaVersion"`
	SourceDirectory   string            `json:"sourceDirectory"`
	SourceCommit      string            `json:"sourceCommit"`
	TestBinarySHA256  string            `json:"testBinarySHA256"`
	Kubeconfig        string            `json:"kubeconfig"`
	KubeconfigSHA256  string            `json:"kubeconfigSHA256"`
	Context           string            `json:"context"`
	KubeSystemUID     string            `json:"kubeSystemUID"`
	ControlPlaneID    string            `json:"controlPlaneID"`
	PreparedAfter     time.Time         `json:"preparedAfter"`
	ExpiresAt         time.Time         `json:"expiresAt"`
	ReceiptPath       string            `json:"receiptPath"`
	Runner            capacityNamespace `json:"runner"`
	Control           capacityNamespace `json:"control"`
	Receipts          capacityNamespace `json:"receipts"`
	OriginalReceipt   string            `json:"originalReceipt"`
	OriginalSHA256    string            `json:"originalSHA256"`
	SecretName        string            `json:"secretName"`
	SecretUID         string            `json:"secretUID"`
	BuildName         string            `json:"buildName"`
	OldWritersStopped *bool             `json:"oldWritersStopped"`
	RetainOnSuccess   *bool             `json:"retainOnSuccess"`
}

func capacityManifestField(path []string, key string) bool {
	if len(path) == 0 {
		switch key {
		case "schemaVersion", "sourceDirectory", "sourceCommit", "testBinarySHA256", "kubeconfig", "kubeconfigSHA256", "context", "kubeSystemUID", "controlPlaneID", "preparedAfter", "expiresAt", "receiptPath", "runner", "control", "receipts", "originalReceipt", "originalSHA256", "secretName", "secretUID", "buildName", "oldWritersStopped", "retainOnSuccess":
			return true
		}
	}
	return len(path) == 1 && (path[0] == "runner" || path[0] == "control" || path[0] == "receipts") && (key == "name" || key == "uid")
}

func capacityName(s string) bool {
	return strings.HasPrefix(s, capacityPrefix) && len(s) > len(capacityPrefix) && len(validation.IsDNS1123Label(s)) == 0
}

func (m capacityManifest) sourcePins() podTemplateManifest {
	return podTemplateManifest{SourceDirectory: m.SourceDirectory, SourceCommit: m.SourceCommit, TestBinarySHA256: m.TestBinarySHA256,
		Context: m.Context, ControlPlaneID: m.ControlPlaneID, PreparedAfter: m.PreparedAfter}
}

func validateCapacityManifest(m capacityManifest, now time.Time) error {
	if m.SchemaVersion != 1 || !filepath.IsAbs(m.SourceDirectory) || !podTemplateHex(m.SourceCommit, 40) ||
		!podTemplateHex(m.TestBinarySHA256, 64) || !filepath.IsAbs(m.Kubeconfig) || !podTemplateHex(m.KubeconfigSHA256, 64) ||
		!capacityName(strings.TrimPrefix(m.Context, "kind-")) || !strings.HasPrefix(m.Context, "kind-") ||
		!admissioncontract.ValidUID(m.KubeSystemUID) || !podTemplateHex(m.ControlPlaneID, 64) ||
		!filepath.IsAbs(m.OriginalReceipt) || !podTemplateHex(m.OriginalSHA256, 64) || !filepath.IsAbs(m.ReceiptPath) ||
		m.ReceiptPath == m.Kubeconfig || m.ReceiptPath == m.OriginalReceipt ||
		!capacityName(m.SecretName) || !admissioncontract.ValidUID(m.SecretUID) || !capacityName(m.BuildName) ||
		m.OldWritersStopped == nil || !*m.OldWritersStopped || m.RetainOnSuccess == nil || !*m.RetainOnSuccess ||
		m.PreparedAfter.IsZero() || now.Before(m.PreparedAfter) || !now.Before(m.ExpiresAt) || m.ExpiresAt.Sub(m.PreparedAfter) > time.Hour {
		return errors.New("v3 capacity gate needs complete private source/target pins, retain assertions and a fresh one-hour window")
	}
	seenNames := map[string]bool{}
	seenUIDs := map[string]bool{m.KubeSystemUID: true, m.SecretUID: true}
	if m.KubeSystemUID == m.SecretUID {
		return errors.New("fixture UIDs must be distinct")
	}
	for _, n := range []capacityNamespace{m.Runner, m.Control, m.Receipts} {
		if !capacityName(n.Name) || seenNames[n.Name] || !admissioncontract.ValidUID(n.UID) || seenUIDs[n.UID] {
			return errors.New("v3 capacity gate needs three distinct dedicated Namespace identities")
		}
		seenNames[n.Name], seenUIDs[n.UID] = true, true
	}
	return nil
}

func capacityReceipt(m capacityManifest, raw []byte) (admissioncontract.Receipt, error) {
	r, err := admissioncontract.ParseReceipt(raw)
	if err != nil {
		return r, err
	}
	c := r.Contract
	if r.Namespace != m.Runner.Name || c.NamespaceUID != m.Runner.UID || c.ReceiptNamespace != m.Receipts.Name || c.ReceiptNamespaceUID != m.Receipts.UID ||
		c.Version != 3 || c.ActiveLedgerSchema != 2 || c.QueueLedgerSchema != 2 ||
		c.Limits != (admissioncontract.Limits{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 1000}) ||
		r.GenesisUID == m.KubeSystemUID || r.GenesisUID == m.SecretUID || r.GenesisUID == m.Runner.UID || r.GenesisUID == m.Control.UID || r.GenesisUID == m.Receipts.UID {
		return r, errors.New("original receipt differs from the v3/schema2 maximum-capacity fixture")
	}
	return r, nil
}

// Only fixed paths within this manifest can be used. Created UIDs are learned
// from qualified API responses, never invented or adopted by a subsequent GET.
// Each mutation body gets one wire attempt, spent BEFORE forwarding. Optional
// client-go retries therefore cannot repeat an unknown POST/PATCH/PUT/DELETE.
type capacityTransport struct {
	mu                                     sync.Mutex
	upstream                               http.RoundTripper
	origin                                 string
	reads                                  map[string]bool
	readUIDs                               map[string]string
	posts                                  map[string]int
	patches                                map[string]string
	buildPath                              string
	owned                                  map[string]string
	receiptObjects                         map[string]*corev1.ConfigMap
	receiptDigests                         map[string]string
	assigned                               func(path, uid, digest string) error
	spent                                  map[[32]byte]bool
	calls, mutations                       int
	wireCalls, wireMutations, wirePodPosts int
	unknown                                bool
	observed                               map[string]map[string]any // only the three ledgers and one CR
}

func capacityReceiptName(name string) bool {
	for _, prefix := range []string{"kova-admission-intent-", "kova-grant-intent-", "kova-pod-create-intent-"} {
		if strings.HasPrefix(name, prefix) && podTemplateHex(strings.TrimPrefix(name, prefix), 32) {
			return true
		}
	}
	return false
}

func newCapacityTransport(m capacityManifest, original admissioncontract.Receipt, origin string, upstream http.RoundTripper) *capacityTransport {
	g := &capacityTransport{upstream: upstream, origin: origin, reads: map[string]bool{}, readUIDs: map[string]string{}, posts: map[string]int{}, patches: map[string]string{},
		owned: map[string]string{}, receiptObjects: map[string]*corev1.ConfigMap{}, receiptDigests: map[string]string{}, spent: map[[32]byte]bool{}}
	g.observed = map[string]map[string]any{}
	for _, n := range []capacityNamespace{m.Runner, m.Control, m.Receipts, {Name: "kube-system", UID: m.KubeSystemUID}} {
		path := "/api/v1/namespaces/" + n.Name
		g.reads[path], g.readUIDs[path] = true, n.UID
	}
	for _, n := range []capacityNamespace{m.Runner, m.Control, m.Receipts} {
		prefix := "/api/v1/namespaces/" + n.Name + "/"
		for _, resource := range []string{"pods", "services", "configmaps"} {
			g.reads[prefix+resource] = true
		}
		g.reads["/apis/apps/v1/namespaces/"+n.Name+"/deployments"] = true
		g.reads["/apis/"+kovav1.SchemeGroupVersion.String()+"/namespaces/"+n.Name+"/kovabuilds"] = true
	}
	base := "/api/v1/namespaces/" + m.Runner.Name + "/configmaps/"
	for _, name := range []string{original.GenesisName, admissioncontract.ActiveLedgerName, admissioncontract.QueueLedgerName} {
		g.reads[base+name] = true
		g.patches[base+name] = ""
	}
	g.patches[base+original.GenesisName] = original.GenesisUID
	g.posts[strings.TrimSuffix(base, "/")] = 2
	g.posts["/api/v1/namespaces/"+m.Receipts.Name+"/configmaps"] = 3
	g.reads["/api/v1/namespaces/"+m.Control.Name+"/secrets/"+m.SecretName] = true
	g.readUIDs["/api/v1/namespaces/"+m.Control.Name+"/secrets/"+m.SecretName] = m.SecretUID
	g.buildPath = "/apis/" + kovav1.SchemeGroupVersion.String() + "/namespaces/" + m.Runner.Name + "/kovabuilds/" + m.BuildName
	g.reads[g.buildPath] = true
	// Product receipt cleanup checks the original derived runner Pod name.
	// This GET cannot settle the issue: settlement above is locally never-sent.
	g.reads["/api/v1/namespaces/"+m.Runner.Name+"/pods/"+buildPodName(m.BuildName)] = true
	g.posts[strings.TrimSuffix(g.buildPath, "/"+m.BuildName)] = 1
	return g
}

func (g *capacityTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unknown || r.URL.Scheme+"://"+r.URL.Host != g.origin || r.URL.User != nil || r.URL.RawPath != "" || r.URL.Fragment != "" ||
		!capacityQueryAllowed(r) || g.calls >= capacityCallLimit {
		return nil, errors.New("capacity API request escaped origin/path/read budget")
	}
	g.calls++
	path := r.URL.Path
	if r.Method == http.MethodGet {
		if !g.reads[path] && g.owned[path] == "" {
			return nil, errors.New("capacity read is not predeclared or originally owned")
		}
		return g.forward(r)
	}
	if r.Body == nil || g.mutations >= 64 {
		return nil, errors.New("capacity mutation body/budget is invalid")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 6*1024*1024+1))
	_ = r.Body.Close()
	if err != nil || len(raw) > 6*1024*1024 {
		return nil, errors.New("capacity mutation body exceeds bound")
	}
	var decoded map[string]any
	if r.Method != http.MethodPatch && !capacityDecode(raw, &decoded) || r.Method == http.MethodPatch && !capacityDecode(raw, new([]any)) {
		return nil, errors.New("capacity mutation has duplicate or malformed JSON")
	}
	var createdPath string
	switch r.Method {
	case http.MethodPost:
		var object struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		if !capacityDecode(raw, &object) || object.Metadata.UID != "" || object.Metadata.GenerateName != "" || g.posts[path] < 1 {
			return nil, errors.New("capacity POST is not a fresh declared object")
		}
		createdPath = path + "/" + object.Metadata.Name
		parts := strings.Split(createdPath, "/")
		if object.Metadata.Namespace != parts[len(parts)-3] {
			return nil, errors.New("POST Namespace differs from declared path")
		}
		if strings.HasSuffix(path, "/kovabuilds") {
			if createdPath != g.buildPath {
				return nil, errors.New("foreign CR POST")
			}
		} else {
			_, ledger := g.patches[createdPath]
			if !ledger && !capacityReceiptName(object.Metadata.Name) {
				return nil, errors.New("foreign ConfigMap POST")
			}
			// The only ledger collection is the one with predeclared patch
			// paths. A receipt name must never expand its allowed object set.
			if !ledger {
				for patchPath := range g.patches {
					if strings.HasPrefix(patchPath, path+"/") {
						return nil, errors.New("receipt POST in runner Namespace")
					}
				}
			}
		}
		if g.owned[createdPath] != "" || (g.patches[createdPath] != "" && strings.HasSuffix(path, "/configmaps")) {
			return nil, errors.New("capacity POST would adopt an existing identity")
		}
		g.posts[path]--
	case http.MethodPatch:
		uid, ok := g.patches[path]
		var ops []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		var got string
		if !ok || uid == "" || r.Header.Get("Content-Type") != string("application/json-patch+json") || !capacityDecode(raw, &ops) || len(ops) < 3 ||
			ops[0].Op != "test" || ops[0].Path != "/metadata/uid" || json.Unmarshal(ops[0].Value, &got) != nil || got != uid ||
			ops[1].Op != "test" || ops[1].Path != "/metadata/resourceVersion" {
			return nil, errors.New("capacity PATCH lacks original UID and resourceVersion CAS")
		}
		var rv string
		if json.Unmarshal(ops[1].Value, &rv) != nil || rv == "" || len(rv) > 256 {
			return nil, errors.New("invalid opaque CAS RV")
		}
		key, maxBytes := admissioncontract.ActiveLedgerDataKey, admissioncontract.MaxLedgerDataBytes
		if strings.HasSuffix(path, "/"+admissioncontract.QueueLedgerName) {
			key = admissioncontract.QueueLedgerDataKey
		}
		genesis := strings.HasSuffix(path, "/"+admissioncontract.GenesisName)
		if genesis {
			key, maxBytes = admissioncontract.GenesisDataKey, admissioncontract.MaxContractJSONBytes
		}
		var old string
		if ops[2].Op != "test" || ops[2].Path != "/data/"+key || json.Unmarshal(ops[2].Value, &old) != nil || len(old) > maxBytes {
			return nil, errors.New("PATCH lacks exact old data CAS")
		}
		next := 3
		if genesis && next < len(ops) && ops[next].Op == "test" && ops[next].Path == "/immutable" && string(ops[next].Value) == "false" {
			next++
		}
		var proposed string
		if next >= len(ops) || ops[next].Op != "replace" || ops[next].Path != "/data/"+key || json.Unmarshal(ops[next].Value, &proposed) != nil || len(proposed) > maxBytes {
			return nil, errors.New("PATCH must replace only fixed bounded data")
		}
		next++
		if genesis && next < len(ops) && ops[next].Op == "add" && ops[next].Path == "/immutable" && string(ops[next].Value) == "true" {
			next++
		}
		if next != len(ops) {
			return nil, errors.New("PATCH contains foreign metadata/data operations")
		}
	case http.MethodPut:
		var object struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		if (path != g.buildPath && path != g.buildPath+"/status") || g.owned[g.buildPath] == "" || !capacityDecode(raw, &object) ||
			string(object.Metadata.UID) != g.owned[g.buildPath] || object.Metadata.ResourceVersion == "" {
			return nil, errors.New("capacity PUT lacks original CR identity")
		}
	case http.MethodDelete:
		var options metav1.DeleteOptions
		if g.receiptObjects[path] == nil || !capacityDecode(raw, &options) || options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != g.owned[path] {
			return nil, errors.New("only this run's exact-UID receipts may be deleted")
		}
	default:
		return nil, errors.New("capacity operation is forbidden (including every Pod POST)")
	}
	key := sha256.Sum256(append([]byte(r.Method+" "+path+"\n"), raw...))
	if g.spent[key] {
		return nil, errors.New("capacity mutation already attempted; unknown is retained")
	}
	g.spent[key] = true
	g.mutations++
	r.Body, r.GetBody = io.NopCloser(bytes.NewReader(raw)), nil
	r.Header.Del("Idempotency-Key")
	r.Header.Del("X-Idempotency-Key")
	response, err := g.forward(r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, nil
	}
	observed, _ := io.ReadAll(response.Body)
	response.Body = io.NopCloser(bytes.NewReader(observed))
	if !g.qualifyMutation(r, raw, observed) {
		g.unknown = true
		return nil, errors.New("mutation acknowledgement differs from original identity/proposal")
	}
	if createdPath != "" {
		var object struct {
			Kind     string            `json:"kind"`
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		parts := strings.Split(createdPath, "/")
		if json.Unmarshal(observed, &object) != nil || !admissioncontract.ValidUID(string(object.Metadata.UID)) || object.Metadata.ResourceVersion == "" ||
			object.Metadata.Name != parts[len(parts)-1] || object.Metadata.Namespace != parts[len(parts)-3] ||
			(strings.HasSuffix(path, "/kovabuilds") && object.Kind != "KovaBuild") || (strings.HasSuffix(path, "/configmaps") && object.Kind != "ConfigMap") {
			g.unknown = true
			return nil, errors.New("capacity Create response cannot qualify original API-assigned identity")
		}
		g.owned[createdPath] = string(object.Metadata.UID)
		if _, ok := g.patches[createdPath]; ok {
			g.patches[createdPath] = string(object.Metadata.UID)
		}
		if capacityReceiptName(object.Metadata.Name) {
			var cm corev1.ConfigMap
			if json.Unmarshal(observed, &cm) != nil {
				return nil, errors.New("receipt response cannot decode")
			}
			g.receiptObjects[createdPath] = &cm
			digest, err := recoveryreceipt.DigestData(cm.Data)
			if err != nil {
				g.unknown = true
				return nil, errors.New("receipt data cannot be bounded/digested")
			}
			g.receiptDigests[createdPath] = digest
		}
		if g.assigned != nil {
			if err := g.assigned(createdPath, string(object.Metadata.UID), g.receiptDigests[createdPath]); err != nil {
				g.unknown = true
				return nil, errors.New("assigned UID receipt unavailable; stop all wire")
			}
		}
	}
	if r.Method == http.MethodDelete {
		var status metav1.Status
		var cm corev1.ConfigMap
		expected := g.receiptObjects[path]
		strict, decodeErr := kjson.UnmarshalStrict(observed, &status, kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
		success := decodeErr == nil && len(strict) == 0 && status.APIVersion == "v1" && status.Kind == "Status" && status.Status == metav1.StatusSuccess &&
			(status.Code == 0 || status.Code == int32(response.StatusCode)) && status.Reason == "" && status.Details == nil
		object := json.Unmarshal(observed, &cm) == nil && cm.APIVersion == "v1" && cm.Kind == "ConfigMap" && cm.Name == expected.Name && cm.Namespace == expected.Namespace && cm.UID == expected.UID &&
			reflect.DeepEqual(cm.Data, expected.Data) && reflect.DeepEqual(cm.BinaryData, expected.BinaryData) && reflect.DeepEqual(cm.Immutable, expected.Immutable)
		if !success && !object {
			g.unknown = true
			return nil, errors.New("receipt DELETE acknowledgement is unknown; stop and retain")
		}
	}
	return response, nil
}

func capacityQueryAllowed(r *http.Request) bool {
	// client-go copies http.Client.Timeout into Request.timeout and sends the
	// canonical timeout=10s query. Keep the actual 10s client deadline intact.
	// Compare raw bytes, not decoded values: duplicates, escaping, selectors,
	// watches, alternative durations and noncanonical ordering stay forbidden.
	if r.Method == http.MethodGet {
		_, _, kind, _, name := capacityAddress(r.URL.Path)
		if name == "" {
			return kind != "" && (r.URL.RawQuery == "limit=1" || r.URL.RawQuery == "limit=1&timeout=10s")
		}
		return r.URL.RawQuery == "" || r.URL.RawQuery == "timeout=10s"
	}
	switch r.Method {
	case http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete:
		return r.URL.RawQuery == "" || r.URL.RawQuery == "timeout=10s"
	}
	return false
}

func capacityNamespaceReason(ns *corev1.Namespace, pin capacityNamespace, prepared, now time.Time, readErr error) string {
	switch {
	case readErr != nil || ns == nil:
		return "NAMESPACE_READ_UNKNOWN"
	case string(ns.UID) != pin.UID:
		return "NAMESPACE_UID_CHANGED"
	case ns.DeletionTimestamp != nil:
		return "NAMESPACE_DELETING"
	case ns.Status.Phase != corev1.NamespaceActive:
		return "NAMESPACE_NOT_ACTIVE"
	case pin.Name != "kube-system" && (ns.CreationTimestamp.Time.Before(prepared) || ns.CreationTimestamp.Time.After(now)):
		return "NAMESPACE_FRESHNESS_CHANGED"
	}
	return ""
}

func capacityFailureRow(ctx context.Context, reason string, g *capacityTransport, limiter flowcontrol.RateLimiter) map[string]any {
	switch reason {
	case "NAMESPACE_READ_UNKNOWN", "NAMESPACE_UID_CHANGED", "NAMESPACE_DELETING", "NAMESPACE_NOT_ACTIVE", "NAMESPACE_FRESHNESS_CHANGED", "RECEIPT_LIST_PREFLIGHT_UNKNOWN":
	default:
		reason = "UNCLASSIFIED"
	}
	if limited, ok := limiter.(*capacityRateLimiter); ok {
		limited.mu.Lock()
		if limited.waitFailed {
			reason = "LOCAL_RATE_LIMIT_WAIT_REFUSED"
		}
		limited.mu.Unlock()
	}
	switch ctx.Err() {
	case context.DeadlineExceeded:
		reason = "LOCAL_CONTEXT_DEADLINE"
	case context.Canceled:
		reason = "LOCAL_CONTEXT_CANCELLED"
	}
	row := map[string]any{"stage": "unknown-retained", "reason": reason, "podPostsPermitted": false, "runtimeCancellationProved": false}
	if g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
		owned, digests := map[string]string{}, map[string]string{}
		for path, uid := range g.owned {
			owned[path] = uid
		}
		for path, digest := range g.receiptDigests {
			digests[path] = digest
		}
		row["apiTransportCalls"], row["apiTransportMutationAttempts"] = g.calls, g.mutations
		row["apiUpstreamCalls"], row["apiUpstreamMutations"], row["apiWireAttemptsUpperBound"] = g.wireCalls, g.wireMutations, g.wireCalls
		row["podPosts"], row["ownedUIDs"], row["receiptDataDigests"] = g.wirePodPosts, owned, digests
	}
	return row
}

func (g *capacityTransport) counters() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return map[string]int{"apiTransportCalls": g.calls, "apiTransportMutationAttempts": g.mutations,
		"apiUpstreamCalls": g.wireCalls, "apiUpstreamMutations": g.wireMutations, "apiWireAttemptsUpperBound": g.wireCalls, "podPosts": g.wirePodPosts}
}

func (g *capacityTransport) forward(r *http.Request) (*http.Response, error) {
	g.wireCalls++
	if r.Method != http.MethodGet {
		g.wireMutations++
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/pods") {
		g.wirePodPosts++
	}
	response, err := g.upstream.RoundTrip(r)
	if err != nil {
		g.unknown = true
		return nil, errors.New("capacity API wire outcome unknown; stop and retain")
	}
	if response == nil || response.Body == nil {
		g.unknown = true
		return nil, errors.New("capacity API response missing")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024+1))
	_ = response.Body.Close()
	if err != nil || len(raw) > 2*1024*1024 || !json.Valid(raw) || response.StatusCode >= 300 && response.StatusCode < 400 || response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
		g.unknown = true
		return nil, errors.New("capacity API response oversized, unreadable or redirected")
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	if !g.qualifyResponse(r, response.StatusCode, raw) {
		g.unknown = true
		return nil, errors.New("API response is duplicate, foreign or unqualified")
	}
	return response, nil
}

func capacityDecode(raw []byte, out any) bool {
	strict, err := kjson.UnmarshalStrict(raw, out, kjson.DisallowDuplicateFields)
	return err == nil && len(strict) == 0
}

func capacityAddress(path string) (group, version, kind, ns, name string) {
	parts := strings.Split(strings.TrimSuffix(path, "/status"), "/")
	if len(parts) == 5 && parts[1] == "api" && parts[3] == "namespaces" {
		return "", "v1", "Namespace", "", parts[4]
	}
	resources := map[string]string{"configmaps": "ConfigMap", "secrets": "Secret", "pods": "Pod", "services": "Service", "deployments": "Deployment", "kovabuilds": "KovaBuild"}
	if (len(parts) == 6 || len(parts) == 7) && parts[1] == "api" && parts[3] == "namespaces" {
		if len(parts) == 7 {
			name = parts[6]
		}
		return "", parts[2], resources[parts[5]], parts[4], name
	}
	if (len(parts) == 7 || len(parts) == 8) && parts[1] == "apis" && parts[4] == "namespaces" {
		if len(parts) == 8 {
			name = parts[7]
		}
		return parts[2], parts[3], resources[parts[6]], parts[5], name
	}
	return
}

func capacityObject(raw []byte, path, uid string) (map[string]any, bool) {
	group, version, kind, ns, name := capacityAddress(path)
	var object map[string]any
	if kind == "" || name == "" || !capacityDecode(raw, &object) {
		return nil, false
	}
	meta, ok := object["metadata"].(map[string]any)
	apiVersion := version
	if group != "" {
		apiVersion = group + "/" + version
	}
	gotUID, _ := meta["uid"].(string)
	rv, _ := meta["resourceVersion"].(string)
	return object, ok && object["apiVersion"] == apiVersion && object["kind"] == kind && meta["name"] == name &&
		(ns == "" && (meta["namespace"] == nil || meta["namespace"] == "") || ns != "" && meta["namespace"] == ns) &&
		admissioncontract.ValidUID(gotUID) && (uid == "" || gotUID == uid) && rv != "" && len(rv) <= 256
}

func (g *capacityTransport) qualifyResponse(r *http.Request, code int, raw []byte) bool {
	var object map[string]any
	if !capacityDecode(raw, &object) || object == nil {
		return false
	}
	if code >= 400 {
		var status metav1.Status
		strict, err := kjson.UnmarshalStrict(raw, &status, kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
		if err != nil || len(strict) != 0 || status.Kind != "Status" || status.APIVersion != "v1" || status.Status != metav1.StatusFailure || status.Code != int32(code) {
			return false
		}
		group, _, kind, _, name := capacityAddress(r.URL.Path)
		resource := map[string]string{"Namespace": "namespaces", "ConfigMap": "configmaps", "Secret": "secrets", "Pod": "pods", "Service": "services", "Deployment": "deployments", "KovaBuild": "kovabuilds"}[kind]
		if code == 404 {
			return r.Method == http.MethodGet && name != "" && status.Reason == metav1.StatusReasonNotFound && status.Details != nil && status.Details.Name == name && status.Details.Group == group && status.Details.Kind == resource
		}
		// A stale JSON Patch `test` can return a generic 422 Invalid Status
		// without details. Product code proves changed original data/RV by
		// direct readback before classifying it as CAS contention.
		return r.Method == http.MethodPatch && name != "" && (code == 422 && status.Reason == metav1.StatusReasonInvalid || code == 409 && status.Reason == metav1.StatusReasonConflict) &&
			(status.Details == nil || status.Details.Name == name && status.Details.Group == group && status.Details.Kind == resource)
	}
	if r.Method != http.MethodGet {
		return r.Method == http.MethodPost && code == http.StatusCreated ||
			(r.Method == http.MethodPatch || r.Method == http.MethodPut) && code == http.StatusOK ||
			r.Method == http.MethodDelete && (code == http.StatusOK || code == http.StatusAccepted)
	} // Mutations are qualified against their input below.
	if code != http.StatusOK {
		return false
	}
	group, version, kind, ns, name := capacityAddress(r.URL.Path)
	if name == "" {
		apiVersion := version
		if group != "" {
			apiVersion = group + "/" + version
		}
		meta, ok := object["metadata"].(map[string]any)
		items, itemsOK := object["items"].([]any)
		rv, _ := meta["resourceVersion"].(string)
		if !ok || !itemsOK || len(items) > 1 || object["kind"] != kind+"List" || object["apiVersion"] != apiVersion || rv == "" ||
			meta["continue"] != nil && meta["continue"] != "" || meta["remainingItemCount"] != nil && meta["remainingItemCount"] != int64(0) {
			return false
		}
		for _, item := range items {
			itemMap, ok := item.(map[string]any)
			if !ok {
				return false
			}
			// Typed Kubernetes Lists carry the GVK on the qualified parent;
			// their items may omit TypeMeta. Inherit only absent fields in this
			// local qualification projection, never explicit null/empty/conflict
			// or the raw response returned to the client.
			for field, expected := range map[string]string{"apiVersion": apiVersion, "kind": kind} {
				if value, present := itemMap[field]; present {
					if value != expected {
						return false
					}
				} else {
					itemMap[field] = expected
				}
			}
			im, ok := itemMap["metadata"].(map[string]any)
			if !ok {
				return false
			}
			itemName, _ := im["name"].(string)
			itemRaw, _ := json.Marshal(itemMap)
			if _, ok := capacityObject(itemRaw, r.URL.Path+"/"+itemName, ""); !ok || im["namespace"] != ns {
				return false
			}
		}
		return true
	}
	uid := g.owned[r.URL.Path]
	if uid == "" {
		uid = g.patches[r.URL.Path]
	}
	if uid == "" {
		uid = g.readUIDs[r.URL.Path]
	}
	got, ok := capacityObject(raw, r.URL.Path, uid)
	if ok {
		if _, ledger := g.patches[r.URL.Path]; ledger || r.URL.Path == g.buildPath {
			g.observed[r.URL.Path] = got
		}
	}
	return ok
}

func capacityProjection(object map[string]any) map[string]any {
	raw, _ := json.Marshal(object)
	var copy map[string]any
	_ = capacityDecode(raw, &copy)
	meta, _ := copy["metadata"].(map[string]any)
	delete(meta, "resourceVersion")
	delete(meta, "managedFields")
	// CRD /status creation strategy removes status; the typed zero-value
	// client instead serializes {}. Only these two empty forms are equal.
	// Every nonempty status (including the Cancelled fixture) stays exact.
	if copy["kind"] == "KovaBuild" && capacityEmptyStatus(copy["status"]) {
		delete(copy, "status")
	}
	return copy
}

func capacityEmptyStatus(value any) bool {
	object, ok := value.(map[string]any)
	return value == nil || ok && len(object) == 0
}

func (g *capacityTransport) qualifyMutation(r *http.Request, input, output []byte) bool {
	if r.Method == http.MethodDelete {
		return true
	} // Duplicate-free exact ACK checked by caller.
	var proposed map[string]any
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		if !capacityDecode(input, &proposed) {
			return false
		}
	}
	path := r.URL.Path
	if r.Method == http.MethodPost {
		meta, ok := proposed["metadata"].(map[string]any)
		if !ok {
			return false
		}
		name, _ := meta["name"].(string)
		path += "/" + name
	}
	uid := g.owned[strings.TrimSuffix(path, "/status")]
	if uid == "" {
		uid = g.patches[path]
	}
	got, ok := capacityObject(output, path, uid)
	if !ok {
		return false
	}
	if r.Method == http.MethodPost {
		// Explicit format avoids CRD spec defaulting; metadata defaults alone
		// cannot substitute proposal data/spec, labels or effect identities.
		for _, field := range []string{"data", "binaryData", "immutable", "spec"} {
			if !reflect.DeepEqual(proposed[field], got[field]) {
				return false
			}
		}
		if got["kind"] == "KovaBuild" {
			if !capacityEmptyStatus(proposed["status"]) || !capacityEmptyStatus(got["status"]) {
				return false
			}
		} else if !reflect.DeepEqual(proposed["status"], got["status"]) {
			return false
		}
		pm, _ := proposed["metadata"].(map[string]any)
		gm, _ := got["metadata"].(map[string]any)
		if gm["deletionTimestamp"] != nil || gm["deletionGracePeriodSeconds"] != nil {
			return false
		}
		for _, field := range []string{"annotations", "labels", "finalizers", "ownerReferences"} {
			if !reflect.DeepEqual(pm[field], gm[field]) {
				return false
			}
		}
	} else if r.Method == http.MethodPatch {
		old := g.observed[path]
		if old == nil {
			return false
		}
		raw, _ := json.Marshal(old)
		_ = capacityDecode(raw, &proposed)
		var ops []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		if !capacityDecode(input, &ops) {
			return false
		}
		meta, _ := proposed["metadata"].(map[string]any)
		var rv string
		_ = json.Unmarshal(ops[1].Value, &rv)
		if meta["resourceVersion"] != rv {
			return false
		}
		for _, op := range ops {
			if op.Op == "test" {
				var actual any
				switch {
				case strings.HasPrefix(op.Path, "/metadata/"):
					actual = meta[strings.TrimPrefix(op.Path, "/metadata/")]
				case strings.HasPrefix(op.Path, "/data/"):
					data, _ := proposed["data"].(map[string]any)
					actual = data[strings.TrimPrefix(op.Path, "/data/")]
				case op.Path == "/immutable":
					actual = proposed["immutable"]
				default:
					return false
				}
				var expected any
				if !capacityDecode(op.Value, &expected) || !reflect.DeepEqual(actual, expected) {
					return false
				}
			}
			if op.Op != "replace" && op.Op != "add" {
				continue
			}
			var v any
			_ = capacityDecode(op.Value, &v)
			if strings.HasPrefix(op.Path, "/data/") {
				data, _ := proposed["data"].(map[string]any)
				if data == nil {
					return false
				}
				data[strings.TrimPrefix(op.Path, "/data/")] = v
			} else if op.Path == "/immutable" {
				proposed["immutable"] = v
			} else {
				return false
			}
		}
		if !reflect.DeepEqual(capacityProjection(proposed), capacityProjection(got)) {
			return false
		}
	} else if !reflect.DeepEqual(capacityProjection(proposed), capacityProjection(got)) {
		return false
	}
	if _, ledger := g.patches[path]; ledger || strings.TrimSuffix(path, "/status") == g.buildPath {
		g.observed[strings.TrimSuffix(path, "/status")] = got
	}
	return true
}

func capacitySingleWireTransport(cfg *rest.Config) (*http.Transport, error) {
	tlsConfig, err := rest.TLSConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}
	tlsConfig.MinVersion = tls.VersionTLS12
	// A fresh HTTP/1 connection cannot hit Transport's reused-connection or
	// HTTP2 retry branches. One counted upstream call has at most one wire
	// attempt (a dial/TLS failure can result in no HTTP request being sent).
	return &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, DisableKeepAlives: true, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 64 * 1024}, nil
}

// Opt-in only: both private manifest variables must be explicitly supplied.
// Every object is retained on failure; even success deletes only ordinary
// queue/grant/Pod-intent receipts through the product's original-UID protocol.
func TestRealAPIGenesisV3FullTableLifecycle(t *testing.T) {
	path, hash := os.Getenv("KOVA_V3_CAPACITY_REAL_API_MANIFEST"), os.Getenv("KOVA_V3_CAPACITY_REAL_API_MANIFEST_SHA256")
	if path == "" && hash == "" {
		t.Skip("requires separately approved, pinned v3 capacity manifest")
	}
	raw, err := podTemplatePrivateFile(path, hash)
	if err != nil {
		t.Fatal(err)
	}
	var m capacityManifest
	if admissionjson.Decode(raw, &m, capacityManifestField) != nil || validateCapacityManifest(m, time.Now()) != nil {
		t.Fatal("invalid v3 capacity manifest")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if err := checkPodTemplateSource(ctx, m.sourcePins()); err != nil {
		t.Fatal(err)
	}
	kubeRaw, err := podTemplatePrivateFile(m.Kubeconfig, m.KubeconfigSHA256)
	if err != nil {
		t.Fatal(err)
	}
	apiConfig, port, err := podTemplateRESTConfig(kubeRaw, m.Context)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPodTemplateKind(ctx, m.sourcePins(), port); err != nil {
		t.Fatal(err)
	}
	originalRaw, err := podTemplatePrivateFile(m.OriginalReceipt, m.OriginalSHA256)
	if err != nil {
		t.Fatal(err)
	}
	original, err := capacityReceipt(m, originalRaw)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(m.ReceiptPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("fresh private output receipt cannot be created")
	}
	complete, unknownReason := false, "UNCLASSIFIED"
	var transport *capacityTransport
	defer func() {
		if !complete {
			if raw, err := json.Marshal(capacityFailureRow(ctx, unknownReason, transport, apiConfig.RateLimiter)); err == nil && len(raw)+1 <= capacityFailureReserve {
				_, _ = file.Write(append(raw, '\n'))
				_ = file.Sync()
			}
		}
		_ = file.Close()
	}()
	stageCount, receiptBytes := 0, 0
	journal := func(row map[string]any) error {
		stageCount++
		raw, err := json.Marshal(row)
		receiptBytes += len(raw) + 1
		if err != nil || stageCount > 20 || len(raw) > 16*1024 || receiptBytes > 64*1024-capacityFailureReserve {
			return errors.New("capacity journal size/stage budget exceeded")
		}
		if n, err := file.Write(append(raw, '\n')); err != nil || n != len(raw)+1 {
			if err == nil {
				err = io.ErrShortWrite
			}
			return err
		}
		return file.Sync()
	}
	write := func(stage string) {
		row := map[string]any{"stage": stage, "sourceCommit": m.SourceCommit, "testBinarySHA256": m.TestBinarySHA256, "manifestSHA256": hash,
			"genesisUID": original.GenesisUID, "podPostsPermitted": false, "runtimeCancellationProved": false}
		if transport != nil {
			row["ownedUIDs"], row["receiptDataDigests"] = transport.owned, transport.receiptDigests
			for field, count := range transport.counters() {
				row[field] = count
			}
		}
		if journal(row) != nil {
			t.Fatal("capacity output receipt unavailable; retain API objects")
		}
	}
	write("begin-retain-all")
	apiConfig = capacityClientConfig(apiConfig)
	apiConfig.ContentType, apiConfig.AcceptContentTypes = "application/json", "application/json"
	upstream, err := capacitySingleWireTransport(apiConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.CloseIdleConnections()
	transport = newCapacityTransport(m, original, apiConfig.Host, upstream)
	transport.assigned = func(path, uid, digest string) error {
		return journal(map[string]any{"stage": "assigned-original-api-uid", "path": path, "uid": uid, "receiptDataDigest": digest, "manifestSHA256": hash})
	}
	httpClient := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect forbidden") }}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, kovav1.SchemeGroupVersion, {Group: "apps", Version: "v1"}})
	for _, kind := range []string{"ConfigMap", "Secret", "Pod", "Service"} {
		mapper.Add(corev1.SchemeGroupVersion.WithKind(kind), meta.RESTScopeNamespace)
	}
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	mapper.Add(kovav1.SchemeGroupVersion.WithKind("KovaBuild"), meta.RESTScopeNamespace)
	scheme := testScheme(t)
	c, err := client.New(apiConfig, client.Options{Scheme: scheme, Mapper: mapper, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfigAndClient(apiConfig, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	check := func(ctx context.Context) error {
		if !time.Now().Before(m.ExpiresAt) {
			return errors.New("fixture authorization window expired")
		}
		if _, err := podTemplatePrivateFile(path, hash); err != nil {
			return err
		}
		if _, err := podTemplatePrivateFile(m.Kubeconfig, m.KubeconfigSHA256); err != nil {
			return err
		}
		if _, err := podTemplatePrivateFile(m.OriginalReceipt, m.OriginalSHA256); err != nil {
			return err
		}
		for _, n := range []capacityNamespace{m.Runner, m.Control, m.Receipts, {Name: "kube-system", UID: m.KubeSystemUID}} {
			ns, err := clientset.CoreV1().Namespaces().Get(ctx, n.Name, metav1.GetOptions{})
			if reason := capacityNamespaceReason(ns, n, m.PreparedAfter, time.Now(), err); reason != "" {
				unknownReason = reason
				return errors.New("original fresh Namespace preflight: " + reason)
			}
		}
		secret, err := clientset.CoreV1().Secrets(m.Control.Name).Get(ctx, m.SecretName, metav1.GetOptions{})
		if err != nil {
			return errors.New("original receipt Secret unavailable")
		}
		return admissioncontract.ValidateReceiptSecret(secret, m.Control.Name, m.SecretName, m.SecretUID, originalRaw)
	}
	if err := check(ctx); err != nil {
		t.Fatal(err)
	}
	preflight := func(ctx context.Context) error {
		if err := check(ctx); err != nil {
			return err
		}
		for _, n := range []capacityNamespace{m.Runner, m.Control, m.Receipts} {
			pods, err := clientset.CoreV1().Pods(n.Name).List(ctx, metav1.ListOptions{Limit: 1})
			if err != nil || len(pods.Items) != 0 || pods.Continue != "" {
				return errors.New("old Pod veto")
			}
			services, err := clientset.CoreV1().Services(n.Name).List(ctx, metav1.ListOptions{Limit: 1})
			if err != nil || len(services.Items) != 0 || services.Continue != "" {
				return errors.New("old Service veto")
			}
			deployments, err := clientset.AppsV1().Deployments(n.Name).List(ctx, metav1.ListOptions{Limit: 1})
			if err != nil || len(deployments.Items) != 0 || deployments.Continue != "" {
				return errors.New("old controller veto")
			}
			var builds kovav1.KovaBuildList
			if err := c.List(ctx, &builds, client.InNamespace(n.Name), client.Limit(1)); err != nil || len(builds.Items) != 0 || builds.Continue != "" {
				return errors.New("old CR veto")
			}
		}
		cms, err := clientset.CoreV1().ConfigMaps(m.Receipts.Name).List(ctx, metav1.ListOptions{Limit: 1})
		if err != nil || len(cms.Items) > 1 || cms.Continue != "" || (len(cms.Items) == 1 && cms.Items[0].Name != "kube-root-ca.crt") {
			unknownReason = "RECEIPT_LIST_PREFLIGHT_UNKNOWN"
			return errors.New("receipt Namespace must contain no old receipts")
		}
		return nil
	}
	for _, name := range []string{admissioncontract.ActiveLedgerName, admissioncontract.QueueLedgerName} {
		if _, err := clientset.CoreV1().ConfigMaps(m.Runner.Name).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatal("refusing existing or unknown ledger identity")
		}
	}
	genesis, err := clientset.CoreV1().ConfigMaps(m.Runner.Name).Get(ctx, original.GenesisName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := original.QualifyGenesis(genesis)
	if err != nil || state.Phase != admissioncontract.PhaseInitializing || state.ActiveLedgerUID != "" || state.QueueLedgerUID != "" {
		t.Fatal("requires original unstarted Initializing Genesis")
	}
	cfg := config.Config{Namespace: m.Runner.Name, WorkerPoolID: original.Contract.WorkerPoolID, RunnerImage: original.Contract.RunnerImage,
		MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 1000}
	cfg.RunnerImageDigest = cfg.RunnerImage[strings.LastIndex(cfg.RunnerImage, "@")+1:]
	activeData, err := GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueData, err := queueadmission.GenesisEmptyQueueData(1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := admissiongenesis.Bootstrapper{API: admissiongenesis.DirectClient{Client: clientset}, Receipt: original, Preflight: preflight, BeforeEffect: check,
		Active: admissiongenesis.LedgerTemplate{Role: admissioncontract.Active, DataKey: admissioncontract.ActiveLedgerDataKey, EmptyData: activeData,
			Validate: func(cm *corev1.ConfigMap) error { return ValidateAdmissionLedgerForGenesis(cm, cfg) }},
		Queue: admissiongenesis.LedgerTemplate{Role: admissioncontract.Queue, DataKey: admissioncontract.QueueLedgerDataKey, EmptyData: queueData,
			Validate: func(cm *corev1.ConfigMap) error { return queueadmission.ValidateQueueLedgerForGenesis(cm, 1000, 1000) }}}
	binding, err := bootstrap.EnsureFresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := admissiongenesis.NewGuard(ctx, bootstrap, binding)
	if err != nil {
		t.Fatal(err)
	}
	guard.ReceiptCheck = check
	r := &KovaBuildReconciler{Client: c, APIReader: c, Scheme: scheme, Cfg: cfg, Genesis: guard,
		RecoveryReceipts: clientset.CoreV1().ConfigMaps(m.Receipts.Name)}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: m.Runner.Name, Name: m.BuildName}, Spec: kovav1.KovaBuildSpec{
		Requester: kovav1.KovaBuildRequester{Username: strings.Repeat("<", 253)}, Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1},
		Source:  kovav1.KovaBuildSourceSpec{URI: "https://example.invalid/never-fetched.zip", Digest: "sha256:" + strings.Repeat("a", 64)},
		Targets: []kovav1.KovaBuildTargetSpec{{Target: "example.invalid/kova:never-built", Platform: "linux/amd64"}}}}
	checkpoint := func(stage string) {
		if err := check(ctx); err != nil {
			t.Fatal(err)
		}
		if err := checkPodTemplateSource(ctx, m.sourcePins()); err != nil {
			t.Fatal(err)
		}
		if err := checkPodTemplateKind(ctx, m.sourcePins(), port); err != nil {
			t.Fatal(err)
		}
		write(stage)
	}
	result := exerciseV3CapacityLifecycle(t, ctx, r, build, true, checkpoint)
	if err := journal(map[string]any{"stage": "measured-capacity", "bytes": result, "buildUID": string(build.UID), "activeLedgerUID": binding.ActiveLedgerUID, "queueLedgerUID": binding.QueueLedgerUID,
		"apiUpstreamCalls": transport.wireCalls, "apiUpstreamMutations": transport.wireMutations, "apiWireAttemptsUpperBound": transport.wireCalls,
		"podPosts": transport.wirePodPosts, "runtimeCancellationProved": false}); err != nil {
		t.Fatal("final measured receipt unavailable; retain")
	}
	write("pass-capacity-only-retained")
	complete = true
	t.Logf("original Genesis=%s active=%s queue=%s CR=%s; %d bounded single-attempt upstream calls/%d mutations/%d Pod POSTs; all retained except exact-UID product receipts", original.GenesisUID, binding.ActiveLedgerUID, binding.QueueLedgerUID, build.UID, transport.wireCalls, transport.wireMutations, transport.wirePodPosts)
}
