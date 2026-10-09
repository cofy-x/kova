package recoveryoperator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

const maxResponseBytes = 16 * 1024 * 1024

var errAPI = errors.New("exact recovery API request outcome is unknown")

// ConnectionRecord authenticates only a concrete API connection. It is not a
// retirement, archive or cutover proof. The authority hashes its canonical JSON
// into the existing ExecutionClusterIdentity.APIIdentityDigest.
type ConnectionRecord struct {
	Version            string `json:"version"`
	ServerURL          string `json:"serverUrl"`
	TLSServerName      string `json:"tlsServerName"`
	CASHA256           string `json:"caSha256"`
	SystemNamespaceUID string `json:"systemNamespaceUid"`
}

// Credentials is a private local input, never part of Pins or ordinary output.
// Exactly one explicit static authentication method is accepted. No kubeconfig
// discovery, exec credential plugin, token refresh, environment or proxy applies.
type Credentials struct {
	BearerToken          string `json:"bearerToken"`
	ClientCertificatePEM string `json:"clientCertificatePem"`
	ClientKeyPEM         string `json:"clientKeyPem"`
}

type exactAPI struct {
	http          *http.Client
	connection    ConnectionRecord
	token         string
	resources     map[string]recoverydisposal.ExecutionTarget
	namespacePins map[string]string
	responseLimit int64
}

// Execute connects only after full local preparation. The caller must request
// execution explicitly; this function has no default or automatic invocation.
// The existing executor rechecks grant time, every original namespace and every
// target. Unknown responses return immediately, never rerun or poll here.
func Execute(ctx context.Context, prepared Prepared, caPath, credentialsPath string) (recoverydisposal.ExecutionReport, error) {
	if ctx == nil || ctx.Err() != nil || !prepared.plan.Valid() || !prepared.grant.Valid() {
		return recoverydisposal.ExecutionReport{}, ErrInput
	}
	api, err := newExactAPI(prepared, caPath, credentialsPath)
	if err != nil {
		return recoverydisposal.ExecutionReport{}, err
	}
	defer api.http.CloseIdleConnections()
	return recoverydisposal.Execute(ctx, api, prepared.plan, prepared.grant, prepared.archives)
}

func validConnection(c ConnectionRecord) bool {
	u, err := url.Parse(c.ServerURL)
	if err != nil || c.Version != "1" || u.Scheme != "https" || u.Host == "" || u.User != nil ||
		u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" ||
		!safeToken(c.SystemNamespaceUID) || !validDigest(c.CASHA256) ||
		(net.ParseIP(c.TLSServerName) == nil && len(validation.IsDNS1123Subdomain(c.TLSServerName)) != 0) {
		return false
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return false
		}
	}
	return u.Hostname() != "" && !strings.ContainsAny(c.ServerURL, "\r\n\t ")
}

func newExactAPI(p Prepared, caPath, credentialsPath string) (*exactAPI, error) {
	if !p.plan.Valid() || !p.grant.Valid() || !validConnection(p.connection) ||
		digestCanonical(p.connection) != p.plan.Payload().Cluster.APIIdentityDigest {
		return nil, ErrInput
	}
	ca, err := readRegularFile(caPath, maxCredentialBytes, false)
	if err != nil || digest(ca) != p.connection.CASHA256 {
		return nil, ErrInput
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, ErrInput
	}
	raw, err := readRegularFile(credentialsPath, maxCredentialBytes, true)
	if err != nil {
		return nil, ErrInput
	}
	var credentials Credentials
	if decodeCanonical(raw, &credentials) != nil {
		return nil, ErrInput
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: p.connection.TLSServerName}
	if credentials.BearerToken != "" {
		if len(credentials.BearerToken) > 65536 || strings.ContainsAny(credentials.BearerToken, " \r\n\t") ||
			credentials.ClientCertificatePEM != "" || credentials.ClientKeyPEM != "" {
			return nil, ErrInput
		}
	} else {
		pair, err := tls.X509KeyPair([]byte(credentials.ClientCertificatePEM), []byte(credentials.ClientKeyPEM))
		if err != nil {
			return nil, ErrInput
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	// Fresh HTTP/1 connections avoid net/http's retry on a previously used
	// connection. No HTTP/2 automatic retry, implicit proxy or redirect exists.
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, DisableKeepAlives: true,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 64 * 1024}
	a := &exactAPI{http: &http.Client{Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		connection: p.connection, token: credentials.BearerToken, resources: map[string]recoverydisposal.ExecutionTarget{},
		namespacePins: map[string]string{"kube-system": p.connection.SystemNamespaceUID,
			p.plan.Payload().Source.Namespace:        p.plan.Payload().Source.NamespaceUID,
			p.plan.Payload().Source.ReceiptNamespace: p.plan.Payload().Source.ReceiptNamespaceUID},
		responseLimit: min(int64(maxResponseBytes), p.plan.Payload().Limits.MaxObjectArchiveBytes)}
	for _, t := range p.plan.Payload().Targets {
		a.resources[targetKey(t.Resource, t.Namespace, t.Name)] = t
	}
	return a, nil
}

func targetKey(r recoverydisposal.ExecutionResource, ns, name string) string {
	return strings.Join([]string{r.Group, r.Version, r.Resource, ns, name}, "\x00")
}

func (a *exactAPI) path(obj client.Object, write bool) (string, error) {
	g := obj.GetObjectKind().GroupVersionKind()
	if g == (schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}) && !write && obj.GetNamespace() == "" {
		// Executor alone selects the three pinned guard names; no namespace
		// write or subresource is ever exposed by this adapter.
		if _, pinned := a.namespacePins[obj.GetName()]; !pinned {
			return "", ErrInput
		}
		return "/api/v1/namespaces/" + obj.GetName(), nil
	}
	r := recoverydisposal.ExecutionResource{Group: g.Group, Version: g.Version}
	switch g {
	case schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}:
		r.Resource = "configmaps"
	case schema.GroupVersionKind{Version: "v1", Kind: "Pod"}:
		r.Resource = "pods"
	case schema.GroupVersionKind{Group: "kova.cofy.dev", Version: "v1alpha1", Kind: "KovaBuild"}:
		r.Resource = "kovabuilds"
	default:
		return "", ErrInput
	}
	t, ok := a.resources[targetKey(r, obj.GetNamespace(), obj.GetName())]
	if !ok || write && (string(obj.GetUID()) != t.UID || obj.GetResourceVersion() == "") {
		return "", ErrInput
	}
	prefix := "/api/" + r.Version
	if r.Group != "" {
		prefix = "/apis/" + r.Group + "/" + r.Version
	}
	return prefix + "/namespaces/" + t.Namespace + "/" + r.Resource + "/" + t.Name, nil
}

func (a *exactAPI) request(ctx context.Context, method, path, contentType string, body []byte, out client.Object) error {
	if ctx == nil || ctx.Err() != nil {
		return errAPI
	}
	req, err := http.NewRequestWithContext(ctx, method, a.connection.ServerURL+path, bytes.NewReader(body))
	if err != nil {
		return errAPI
	}
	req.GetBody = nil // never replay a mutation body
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	res, err := a.http.Do(req)
	if err != nil {
		return errAPI
	} // no diagnostics containing endpoint/token/body
	defer res.Body.Close()
	if res.ContentLength > a.responseLimit {
		return errAPI
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, a.responseLimit+1))
	if err != nil || int64(len(raw)) > a.responseLimit || ctx.Err() != nil {
		return errAPI
	}
	// An HTTP status alone is not an exact absence observation. Proxies can
	// return empty/HTML/foreign 404s; only the directly qualified Kubernetes
	// Status for this exact requested group/resource/name means NotFound.
	if res.StatusCode == http.StatusNotFound {
		if method != http.MethodGet || out == nil {
			return errAPI
		}
		var status metav1.Status
		strict, decodeErr := kjson.UnmarshalStrict(raw, &status, kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
		r := resourceFor(out)
		if out.GetObjectKind().GroupVersionKind() == (schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}) {
			r = recoverydisposal.ExecutionResource{Version: "v1", Resource: "namespaces"}
		}
		if decodeErr != nil || len(strict) != 0 || status.APIVersion != "v1" || status.Kind != "Status" ||
			status.Status != metav1.StatusFailure || status.Reason != metav1.StatusReasonNotFound || status.Code != 404 ||
			status.Details == nil || status.Details.Name != out.GetName() || status.Details.Group != r.Group || status.Details.Kind != r.Resource {
			return errAPI
		}
		return apierrors.NewNotFound(schema.GroupResource{Group: r.Group, Resource: r.Resource}, out.GetName())
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errAPI
	}
	if (method == http.MethodGet || method == http.MethodPatch) && res.StatusCode != http.StatusOK ||
		method == http.MethodDelete && res.StatusCode != http.StatusOK && res.StatusCode != http.StatusAccepted {
		return errAPI
	}
	if method == http.MethodDelete {
		if out == nil {
			return errAPI
		}
		var object map[string]any
		strict, decodeErr := kjson.UnmarshalStrict(raw, &object, kjson.DisallowDuplicateFields)
		if decodeErr != nil || len(strict) != 0 || object == nil {
			return errAPI
		}
		if object["kind"] == "Status" {
			var status metav1.Status
			strict, decodeErr = kjson.UnmarshalStrict(raw, &status, kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
			r := resourceFor(out)
			if decodeErr != nil || len(strict) != 0 || status.APIVersion != "v1" || status.Kind != "Status" ||
				status.Status != metav1.StatusSuccess || status.Reason != "" || (status.Code != 0 && status.Code != int32(res.StatusCode)) ||
				status.Details == nil || status.Details.Name != out.GetName() || status.Details.Group != r.Group ||
				status.Details.Kind != r.Resource || string(status.Details.UID) != string(out.GetUID()) {
				return errAPI
			}
		} else {
			observed := &unstructured.Unstructured{Object: object}
			if observed.GroupVersionKind() != out.GetObjectKind().GroupVersionKind() || observed.GetName() != out.GetName() ||
				observed.GetNamespace() != out.GetNamespace() || observed.GetUID() != out.GetUID() || observed.GetResourceVersion() == "" {
				return errAPI
			}
			original, ok := out.(*unstructured.Unstructured)
			if !ok {
				return errAPI
			}
			left, right := original.DeepCopy(), observed.DeepCopy()
			leftDeleting, leftDeletionErr := deletionState(left)
			rightDeleting, rightDeletionErr := deletionState(right)
			if leftDeletionErr != nil || rightDeletionErr != nil || leftDeleting && !rightDeleting {
				return errAPI
			}
			leftGeneration, leftPresent, leftErr := unstructured.NestedInt64(left.Object, "metadata", "generation")
			rightGeneration, rightPresent, rightErr := unstructured.NestedInt64(right.Object, "metadata", "generation")
			if leftErr != nil || rightErr != nil || leftGeneration < 0 || rightGeneration < 0 {
				return errAPI
			}
			if leftPresent != rightPresent || leftGeneration != rightGeneration {
				// The API's first deletion transition may increment generation
				// once; do not ignore arbitrary generation or body drift.
				if leftDeleting || !rightDeleting || !leftPresent || !rightPresent ||
					leftGeneration <= 0 || leftGeneration == 9223372036854775807 || rightGeneration != leftGeneration+1 {
					return errAPI
				}
				right.SetGeneration(leftGeneration)
			}
			for _, field := range []string{"resourceVersion", "managedFields", "deletionTimestamp", "deletionGracePeriodSeconds"} {
				unstructured.RemoveNestedField(left.Object, "metadata", field)
				unstructured.RemoveNestedField(right.Object, "metadata", field)
			}
			before, _ := json.Marshal(left.Object)
			after, _ := json.Marshal(right.Object)
			if !bytes.Equal(before, after) {
				return errAPI
			}
		}
		return nil // only acknowledgment; Execute still requires guarded readback
	}
	if out == nil {
		return errAPI
	}
	u, ok := out.(*unstructured.Unstructured)
	if !ok {
		return ErrInput
	}
	var decoded map[string]any
	strict, err := kjson.UnmarshalStrict(raw, &decoded, kjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 || decoded == nil {
		return errAPI
	}
	u.Object = decoded
	return nil
}

func (a *exactAPI) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	if len(opts) != 0 {
		return ErrInput
	}
	out.SetNamespace(key.Namespace)
	out.SetName(key.Name)
	path, err := a.path(out, false)
	if err != nil {
		return err
	}
	return a.request(ctx, http.MethodGet, path, "", nil, out)
}
func (a *exactAPI) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	path, err := a.path(obj, true)
	if err != nil {
		return err
	}
	o := &client.DeleteOptions{}
	for _, option := range opts {
		option.ApplyToDelete(o)
	}
	if o.Preconditions == nil || o.Preconditions.UID == nil || string(*o.Preconditions.UID) != string(obj.GetUID()) ||
		o.Preconditions.ResourceVersion == nil || *o.Preconditions.ResourceVersion != obj.GetResourceVersion() ||
		o.GracePeriodSeconds != nil || o.PropagationPolicy == nil || *o.PropagationPolicy != metav1.DeletePropagationBackground ||
		len(o.DryRun) != 0 || o.Raw != nil {
		return ErrInput
	}
	raw, err := json.Marshal(metav1.DeleteOptions{Preconditions: o.Preconditions, PropagationPolicy: o.PropagationPolicy})
	if err != nil {
		return ErrInput
	}
	return a.request(ctx, http.MethodDelete, path, "application/json", raw, obj)
}
func (a *exactAPI) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	path, err := a.path(obj, true)
	if err != nil {
		return err
	}
	if len(opts) != 0 || patch.Type() != types.JSONPatchType {
		return ErrInput
	}
	raw, err := patch.Data(obj)
	if err != nil || len(raw) > 2*maxResponseBytes+4096 {
		return ErrInput
	}
	// Only Execute constructs these exact UID/RV/finalizer tests and one
	// finalizer replacement. No general-purpose JSON Patch capability escapes.
	var operations []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(raw, &operations) != nil || len(operations) != 4 ||
		operations[0].Op != "test" || operations[0].Path != "/metadata/uid" ||
		operations[1].Op != "test" || operations[1].Path != "/metadata/resourceVersion" ||
		operations[2].Op != "test" || operations[2].Path != "/metadata/finalizers" ||
		operations[3].Op != "replace" || operations[3].Path != "/metadata/finalizers" {
		return ErrInput
	}
	var uid, rv string
	var before, after []string
	if json.Unmarshal(operations[0].Value, &uid) != nil || json.Unmarshal(operations[1].Value, &rv) != nil ||
		json.Unmarshal(operations[2].Value, &before) != nil || json.Unmarshal(operations[3].Value, &after) != nil ||
		uid != string(obj.GetUID()) || rv != obj.GetResourceVersion() {
		return ErrInput
	}
	t := a.resources[targetKey(resourceFor(obj), obj.GetNamespace(), obj.GetName())]
	wanted := []string{}
	for _, f := range before {
		if !contains(t.AllowedKovaFinalizers, f) {
			wanted = append(wanted, f)
		}
	}
	wantRaw, _ := json.Marshal(wanted)
	afterRaw, _ := json.Marshal(after)
	if !bytes.Equal(wantRaw, afterRaw) {
		return ErrInput
	}
	return a.request(ctx, http.MethodPatch, path, "application/json-patch+json", raw, obj)
}
func resourceFor(obj client.Object) recoverydisposal.ExecutionResource {
	g := obj.GetObjectKind().GroupVersionKind()
	r := recoverydisposal.ExecutionResource{Group: g.Group, Version: g.Version}
	switch g.Kind {
	case "ConfigMap":
		r.Resource = "configmaps"
	case "Pod":
		r.Resource = "pods"
	case "KovaBuild":
		r.Resource = "kovabuilds"
	}
	return r
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func deletionState(obj *unstructured.Unstructured) (bool, error) {
	value, exists, err := unstructured.NestedString(obj.Object, "metadata", "deletionTimestamp")
	if err != nil {
		return false, errAPI
	}
	if !exists {
		return false, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() {
		return false, errAPI
	}
	return true, nil
}

var _ recoverydisposal.ExecutionAPI = (*exactAPI)(nil)
