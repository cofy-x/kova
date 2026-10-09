package buildcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	"github.com/cofy-x/kova/internal/version"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// Source-only unless BOTH environment variables below are set. An external,
// separately approved installer supplies a fresh, dedicated single-node Kind
// cluster, two Namespaces, their default ServiceAccounts, and immutable dummy
// dockerconfig Secrets whose only data is {"auths":{}}. No CRD is required.
// Build this test binary from the manifest's clean sourceCommit with that SHA
// linked into internal/version.Commit, then pin its SHA256 in the manifest.
// The manifest and kubeconfig must be private files, and receiptPath must not
// exist. Every fixture identity and all three Pod names are declared up front.
//
// The ONLY API writes are one POST for each declared Pod. All Pods permanently
// retain the scheduling gate and imagePullPolicy Never. No controller, runner,
// exec, image pull, registry request, ledger, status write, or cleanup is used.
// A Namespace owner binds a real, stable UID without creating a KovaBuild or
// leaving a dangling owner for garbage collection. Retain objects on ALL exits;
// a failed/ambiguous POST is never retried, including by the HTTP transport.
// This proves canonical Spec API defaulting, not production KovaBuild-owner,
// status persistence, runner execution, or end-to-end recovery acceptance.
const (
	podTemplateManifestEnv = "KOVA_POD_TEMPLATE_REAL_API_MANIFEST"
	podTemplateHashEnv     = "KOVA_POD_TEMPLATE_REAL_API_MANIFEST_SHA256"
	podTemplateGate        = "kova.cofy.dev/retain-unscheduled-digest-fixture"
	podTemplateNeverNode   = "kova.cofy.dev/pod-template-never-run"
	podTemplatePrefix      = "kova-pod-template-"
)

type podTemplateNamespace struct {
	Name              string `json:"name"`
	UID               string `json:"uid"`
	ServiceAccountUID string `json:"serviceAccountUID"`
	ServiceAccountRV  string `json:"serviceAccountRV"`
	SecretName        string `json:"secretName"`
	SecretUID         string `json:"secretUID"`
	SecretRV          string `json:"secretRV"`
}

type podTemplateManifest struct {
	SourceDirectory  string               `json:"sourceDirectory"`
	SourceCommit     string               `json:"sourceCommit"`
	TestBinarySHA256 string               `json:"testBinarySHA256"`
	Kubeconfig       string               `json:"kubeconfig"`
	KubeconfigSHA256 string               `json:"kubeconfigSHA256"`
	Context          string               `json:"context"`
	KubeSystemUID    string               `json:"kubeSystemUID"`
	ControlPlaneID   string               `json:"controlPlaneID"`
	PreparedAfter    time.Time            `json:"preparedAfter"`
	ExpiresAt        time.Time            `json:"expiresAt"`
	ReceiptPath      string               `json:"receiptPath"`
	Positive         podTemplateNamespace `json:"positive"`
	Negative         podTemplateNamespace `json:"negative"`
	BaselinePod      string               `json:"baselinePod"`
	LimitsPod        string               `json:"limitsPod"`
	NegativePod      string               `json:"negativePod"`
}

func podTemplateManifestField(path []string, key string) bool {
	if len(path) == 0 {
		switch key {
		case "sourceDirectory", "sourceCommit", "testBinarySHA256", "kubeconfig", "kubeconfigSHA256", "context", "kubeSystemUID", "controlPlaneID", "preparedAfter", "expiresAt", "receiptPath", "positive", "negative", "baselinePod", "limitsPod", "negativePod":
			return true
		}
	}
	if len(path) == 1 && (path[0] == "positive" || path[0] == "negative") {
		switch key {
		case "name", "uid", "serviceAccountUID", "serviceAccountRV", "secretName", "secretUID", "secretRV":
			return true
		}
	}
	return false
}

func podTemplateHex(s string, length int) bool {
	_, err := hex.DecodeString(s)
	return len(s) == length && err == nil && strings.ToLower(s) == s
}

func podTemplateName(s string) bool {
	return strings.HasPrefix(s, podTemplatePrefix) && len(s) > len(podTemplatePrefix) && len(validation.IsDNS1123Label(s)) == 0
}

func validatePodTemplateManifest(m podTemplateManifest, now time.Time) error {
	if !filepath.IsAbs(m.SourceDirectory) || !podTemplateHex(m.SourceCommit, 40) ||
		!podTemplateHex(m.TestBinarySHA256, 64) || !filepath.IsAbs(m.Kubeconfig) || !podTemplateHex(m.KubeconfigSHA256, 64) ||
		!strings.HasPrefix(m.Context, "kind-"+podTemplatePrefix) || !podTemplateName(strings.TrimPrefix(m.Context, "kind-")) ||
		!admissioncontract.ValidUID(m.KubeSystemUID) || !podTemplateHex(m.ControlPlaneID, 64) ||
		!filepath.IsAbs(m.ReceiptPath) || m.ReceiptPath == m.Kubeconfig ||
		m.PreparedAfter.IsZero() || now.Before(m.PreparedAfter) || !now.Before(m.ExpiresAt) ||
		m.ExpiresAt.Sub(m.PreparedAfter) > time.Hour {
		return errors.New("Pod-template gate requires a complete source/binary/target pin and a fresh window of at most one hour")
	}
	seenNames := map[string]bool{}
	seenUIDs := map[string]bool{m.KubeSystemUID: true}
	for _, n := range []podTemplateNamespace{m.Positive, m.Negative} {
		if !podTemplateName(n.Name) || seenNames[n.Name] || !podTemplateName(n.SecretName) || n.ServiceAccountRV == "" || n.SecretRV == "" {
			return errors.New("Pod-template gate requires two distinct dedicated Namespaces and exact SA/Secret fixtures")
		}
		seenNames[n.Name] = true
		for _, uid := range []string{n.UID, n.ServiceAccountUID, n.SecretUID} {
			if !admissioncontract.ValidUID(uid) || seenUIDs[uid] {
				return errors.New("Pod-template fixture UID is missing or reused")
			}
			seenUIDs[uid] = true
		}
	}
	for _, name := range []string{m.BaselinePod, m.LimitsPod, m.NegativePod} {
		if !podTemplateName(name) || seenNames[name] {
			return errors.New("Pod-template gate requires three distinct predeclared fresh Pod names")
		}
		seenNames[name] = true
	}
	return nil
}

func podTemplatePrivateFile(path, hash string) ([]byte, error) {
	info, err := os.Lstat(path)
	if !filepath.IsAbs(path) || !podTemplateHex(hash, 64) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() < 1 || info.Size() > 64*1024 {
		return nil, errors.New("pinned private file is absent, symlinked, too large, or group/world accessible")
	}
	raw, err := os.ReadFile(path)
	sum := sha256.Sum256(raw)
	if err != nil || int64(len(raw)) != info.Size() || hex.EncodeToString(sum[:]) != hash {
		return nil, errors.New("pinned private file content changed")
	}
	return raw, nil
}

func loadPodTemplateManifest(path, hash string) (podTemplateManifest, error) {
	raw, err := podTemplatePrivateFile(path, hash)
	var m podTemplateManifest
	if err != nil {
		return m, err
	}
	if err := admissionjson.Decode(raw, &m, podTemplateManifestField); err != nil {
		return m, errors.New("Pod-template manifest has invalid, unknown, or duplicate fields")
	}
	return m, validatePodTemplateManifest(m, time.Now())
}

func podTemplateCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = time.Second
	raw, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read-only %s identity check failed", name)
	}
	return raw, nil
}

func checkPodTemplateSource(ctx context.Context, m podTemplateManifest) error {
	if version.Commit != m.SourceCommit {
		return errors.New("test binary version.Commit differs from sourceCommit")
	}
	head, err := podTemplateCommand(ctx, "git", "-C", m.SourceDirectory, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != m.SourceCommit {
		return errors.New("source checkout HEAD differs from sourceCommit")
	}
	status, err := podTemplateCommand(ctx, "git", "-C", m.SourceDirectory, "status", "--porcelain", "--untracked-files=all")
	if err != nil || len(status) != 0 {
		return errors.New("source checkout is not clean")
	}
	executable, err := os.Executable()
	if err != nil {
		return errors.New("test executable path unavailable")
	}
	const maximumBinarySize = 512 * 1024 * 1024
	info, err := os.Lstat(executable)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumBinarySize {
		return errors.New("test executable must be a bounded regular non-symlink file")
	}
	f, err := os.Open(executable)
	if err != nil {
		return errors.New("test executable unreadable")
	}
	defer f.Close()
	hash := sha256.New()
	if count, err := io.Copy(hash, io.LimitReader(f, maximumBinarySize+1)); err != nil || count != info.Size() || hex.EncodeToString(hash.Sum(nil)) != m.TestBinarySHA256 {
		return errors.New("test executable SHA256 differs from manifest")
	}
	return nil
}

func podTemplateRESTConfig(raw []byte, expectedContext string) (*rest.Config, string, error) {
	loaded, err := clientcmd.Load(raw)
	if err != nil || loaded.CurrentContext != expectedContext || len(loaded.Contexts) != 1 || len(loaded.Clusters) != 1 || len(loaded.AuthInfos) != 1 {
		return nil, "", errors.New("kubeconfig must contain only the exact dedicated current context")
	}
	selected := loaded.Contexts[expectedContext]
	if selected == nil || loaded.Clusters[selected.Cluster] == nil || loaded.AuthInfos[selected.AuthInfo] == nil {
		return nil, "", errors.New("kubeconfig exact context is incomplete")
	}
	cluster, auth := loaded.Clusters[selected.Cluster], loaded.AuthInfos[selected.AuthInfo]
	u, err := url.Parse(cluster.Server)
	if err != nil || u.Scheme != "https" || u.Hostname() != "127.0.0.1" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
		cluster.ProxyURL != "" || cluster.InsecureSkipTLSVerify || cluster.TLSServerName != "" || cluster.CertificateAuthority != "" ||
		len(cluster.CertificateAuthorityData) == 0 || len(auth.ClientCertificateData) == 0 || len(auth.ClientKeyData) == 0 ||
		auth.ClientCertificate != "" || auth.ClientKey != "" || auth.Exec != nil || auth.AuthProvider != nil ||
		auth.Token != "" || auth.TokenFile != "" || auth.Username != "" || auth.Password != "" || auth.Impersonate != "" ||
		len(auth.ImpersonateGroups) != 0 || len(auth.ImpersonateUserExtra) != 0 || auth.ImpersonateUID != "" {
		return nil, "", errors.New("kubeconfig must use embedded client certificates and direct verified loopback HTTPS without proxies or impersonation")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return nil, "", errors.New("kubeconfig requires an explicit API port")
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(*loaded, expectedContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil || cfg.Host != cluster.Server {
		return nil, "", errors.New("cannot construct exact direct API client")
	}
	cfg.Timeout = 10 * time.Second
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	return cfg, strconv.Itoa(port), nil
}

func checkPodTemplateKind(ctx context.Context, m podTemplateManifest, port string) error {
	cluster := strings.TrimPrefix(m.Context, "kind-")
	nodeName := cluster + "-control-plane"
	raw, err := podTemplateCommand(ctx, "kind", "get", "nodes", "--name", cluster)
	if err != nil || strings.TrimSpace(string(raw)) != nodeName {
		return errors.New("gate requires the exact dedicated single-node Kind cluster")
	}
	const format = `{"id":{{json .Id}},"running":{{json .State.Running}},"created":{{json .Created}},"cluster":{{json (index .Config.Labels "io.x-k8s.kind.cluster")}},"role":{{json (index .Config.Labels "io.x-k8s.kind.role")}},"ports":{{json (index .NetworkSettings.Ports "6443/tcp")}}}`
	raw, err = podTemplateCommand(ctx, "docker", "inspect", "--format", format, nodeName)
	if err != nil {
		return err
	}
	var node struct {
		ID, Cluster, Role string
		Running           bool
		Created           time.Time
		Ports             []struct{ HostIP, HostPort string }
	}
	if json.Unmarshal(raw, &node) != nil || node.ID != m.ControlPlaneID || !node.Running || node.Cluster != cluster || node.Role != "control-plane" ||
		node.Created.Before(m.PreparedAfter) || node.Created.After(time.Now()) || len(node.Ports) != 1 || node.Ports[0].HostIP != "127.0.0.1" || node.Ports[0].HostPort != port {
		return errors.New("fresh Kind Docker identity, labels, or loopback port binding differs")
	}
	return nil
}

// A transport-level allowlist seals the mutation budget before networking.
// Mark POST spent BEFORE forwarding; connection errors and lost replies do not
// permit a second attempt. Raw http.Client avoids client-go's retry machinery.
type podTemplateTransport struct {
	mu       sync.Mutex
	upstream http.RoundTripper
	origin   string
	reads    map[string]bool
	pods     map[string]*corev1.Pod
	spent    map[string]bool
}

func (g *podTemplateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme+"://"+r.URL.Host != g.origin || r.URL.RawQuery != "" || r.URL.Fragment != "" || r.URL.RawPath != "" || r.URL.User != nil {
		return nil, errors.New("API request escaped its exact origin/path")
	}
	if r.Method == http.MethodGet && g.reads[r.URL.Path] {
		return g.upstream.RoundTrip(r)
	}
	if r.Method != http.MethodPost || r.Body == nil {
		return nil, errors.New("API operation is outside the Pod-template gate")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 128*1024+1))
	_ = r.Body.Close()
	var got corev1.Pod
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err != nil || len(raw) > 128*1024 || decoder.Decode(&got) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("invalid Pod POST")
	}
	key := got.Namespace + "/" + got.Name
	expected := g.pods[key]
	if expected == nil || r.URL.Path != "/api/v1/namespaces/"+got.Namespace+"/pods" || !reflect.DeepEqual(expected, &got) || podTemplateUnscheduled(&got) != nil {
		return nil, errors.New("Pod POST differs from the exact predeclared gated template")
	}
	g.mu.Lock()
	if g.spent[key] {
		g.mu.Unlock()
		return nil, errors.New("Pod POST already attempted; unknown results cannot be reissued")
	}
	g.spent[key] = true
	g.mu.Unlock()
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.GetBody = nil
	r.Header.Del("Idempotency-Key")
	r.Header.Del("X-Idempotency-Key")
	return g.upstream.RoundTrip(r)
}

type podTemplateAPI struct {
	client *http.Client
	origin string
}

var errPodTemplateNotFound = errors.New("exact Pod is absent")

func (a *podTemplateAPI) request(ctx context.Context, method, path string, input, output any) (int, error) {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return 0, err
		}
		// An opaque Reader prevents net/http from generating GetBody for POST.
		body = struct{ io.Reader }{bytes.NewReader(raw)}
	}
	r, err := http.NewRequestWithContext(ctx, method, a.origin+path, body)
	if err != nil {
		return 0, errors.New("cannot prepare exact API request")
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	response, err := a.client.Do(r)
	if err != nil {
		return 0, errors.New("direct API request failed; retain objects and do not retry unknown writes")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return response.StatusCode, errors.New("API response unreadable or outside bounded size; retain objects")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var status metav1.Status
		parts := strings.Split(path, "/")
		if response.StatusCode == http.StatusNotFound && method == http.MethodGet && len(parts) == 7 && parts[1] == "api" && parts[2] == "v1" &&
			parts[3] == "namespaces" && parts[5] == "pods" && json.Unmarshal(raw, &status) == nil && status.APIVersion == "v1" && status.Kind == "Status" &&
			status.Status == metav1.StatusFailure && status.Code == http.StatusNotFound && status.Reason == metav1.StatusReasonNotFound &&
			status.Details != nil && status.Details.Name == parts[6] && status.Details.Kind == "pods" && status.Details.Group == "" {
			return response.StatusCode, errPodTemplateNotFound
		}
		return response.StatusCode, fmt.Errorf("direct API returned HTTP %d; response body withheld", response.StatusCode)
	}
	if output != nil && json.Unmarshal(raw, output) != nil {
		return response.StatusCode, errors.New("API response could not be decoded")
	}
	return response.StatusCode, nil
}

func podTemplateFixtures(m podTemplateManifest) []*corev1.Pod {
	var pods []*corev1.Pod
	for i, name := range []string{m.BaselinePod, m.LimitsPod, m.NegativePod} {
		ns := m.Positive
		if i == 2 {
			ns = m.Negative
		}
		pod := runner.PreparePod(runner.ManifestOptions{PodName: name, Namespace: ns.Name,
			Image: "registry.invalid/kova/never-run@sha256:" + strings.Repeat("a", 64), ImagePullPolicy: "Never",
			NodeSelector:    map[string]string{podTemplateNeverNode: "true"},
			ImagePullSecret: ns.SecretName, SourceURI: "oci://registry.invalid/kova/never-fetch@sha256:" + strings.Repeat("b", 64),
			SourceDigest: "sha256:" + strings.Repeat("c", 64)})
		yes, no := true, false
		pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: ns.Name, UID: types.UID(ns.UID), Controller: &yes, BlockOwnerDeletion: &no}}
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: podTemplateGate}}
		// No Node can both have this selector label and satisfy DoesNotExist.
		pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: podTemplateNeverNode, Operator: corev1.NodeSelectorOpDoesNotExist}}}},
		}}}
		if i == 1 {
			// PreparePod merges defaults, so deliberately remove the request map
			// after preparing the real runner and before Genesis defaulting.
			for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
				for j := range containers {
					containers[j].Resources = corev1.ResourceRequirements{Limits: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("750m"), corev1.ResourceMemory: resource.MustParse("96Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("192Mi"),
					}}
				}
			}
		}
		if i == 2 {
			// Preserve the source-fetch and Secret volume, but leave pull secrets
			// empty so admission copies the pinned default SA's pull secret.
			pod.Spec.ImagePullSecrets = nil
		}
		prepareGenesisPodDefaults(&pod)
		pods = append(pods, &pod)
	}
	return pods
}

func podTemplateUnscheduled(p *corev1.Pod) error {
	if p.Spec.NodeSelector[podTemplateNeverNode] != "true" || p.Spec.Affinity == nil || p.Spec.Affinity.NodeAffinity == nil ||
		!reflect.DeepEqual(p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: podTemplateNeverNode, Operator: corev1.NodeSelectorOpDoesNotExist}}}},
		}) {
		return errors.New("impossible node selector/affinity defense changed")
	}
	if p.Spec.NodeName != "" || len(p.Spec.SchedulingGates) != 1 || p.Spec.SchedulingGates[0].Name != podTemplateGate ||
		p.Spec.AutomountServiceAccountToken == nil || *p.Spec.AutomountServiceAccountToken || p.DeletionTimestamp != nil ||
		len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 1 || len(p.Spec.EphemeralContainers) != 0 ||
		len(p.Status.ContainerStatuses) != 0 || len(p.Status.InitContainerStatuses) != 0 || len(p.Status.EphemeralContainerStatuses) != 0 ||
		p.Status.StartTime != nil || p.Status.HostIP != "" || p.Status.PodIP != "" || (p.Status.Phase != "" && p.Status.Phase != corev1.PodPending) {
		return errors.New("Pod scheduling gate or never-executed invariant changed")
	}
	for _, c := range append(append([]corev1.Container{}, p.Spec.Containers...), p.Spec.InitContainers...) {
		if c.ImagePullPolicy != corev1.PullNever {
			return errors.New("Pod no longer has imagePullPolicy Never")
		}
	}
	return nil
}

func (a *podTemplateAPI) checkFixtures(ctx context.Context, m podTemplateManifest) error {
	for _, target := range []struct{ name, uid string }{{"kube-system", m.KubeSystemUID}, {m.Positive.Name, m.Positive.UID}, {m.Negative.Name, m.Negative.UID}} {
		var ns corev1.Namespace
		if _, err := a.request(ctx, "GET", "/api/v1/namespaces/"+target.name, nil, &ns); err != nil || string(ns.UID) != target.uid ||
			ns.Name != target.name || ns.ResourceVersion == "" || ns.Status.Phase != corev1.NamespaceActive || ns.DeletionTimestamp != nil || ns.CreationTimestamp.Time.Before(m.PreparedAfter) || ns.CreationTimestamp.Time.After(time.Now()) {
			return errors.New("fresh Namespace identity, phase, or creation time differs")
		}
	}
	for i, ns := range []podTemplateNamespace{m.Positive, m.Negative} {
		base := "/api/v1/namespaces/" + ns.Name
		var sa corev1.ServiceAccount
		if _, err := a.request(ctx, "GET", base+"/serviceaccounts/default", nil, &sa); err != nil || sa.Name != "default" || sa.Namespace != ns.Name || string(sa.UID) != ns.ServiceAccountUID || sa.ResourceVersion != ns.ServiceAccountRV || sa.DeletionTimestamp != nil {
			return errors.New("default ServiceAccount identity or resourceVersion differs")
		}
		if (i == 0 && len(sa.ImagePullSecrets) != 0) || (i == 1 && !reflect.DeepEqual(sa.ImagePullSecrets, []corev1.LocalObjectReference{{Name: ns.SecretName}})) {
			return errors.New("default ServiceAccount pull-secret injection differs from declared positive/negative fixture")
		}
		var secret corev1.Secret
		if _, err := a.request(ctx, "GET", base+"/secrets/"+ns.SecretName, nil, &secret); err != nil || secret.Name != ns.SecretName || secret.Namespace != ns.Name || string(secret.UID) != ns.SecretUID || secret.ResourceVersion != ns.SecretRV ||
			secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeDockerConfigJson ||
			len(secret.Data) != 1 || string(secret.Data[corev1.DockerConfigJsonKey]) != `{"auths":{}}` {
			return errors.New("pinned immutable credential-free dockerconfig Secret differs")
		}
	}
	return nil
}

func (a *podTemplateAPI) preflight(ctx context.Context, m podTemplateManifest, pods []*corev1.Pod) error {
	if err := a.checkFixtures(ctx, m); err != nil {
		return err
	}
	// Inspect both complete fixture namespaces and all three exact names before
	// the first write. No old Pod or admission policy may be adopted.
	for _, ns := range []podTemplateNamespace{m.Positive, m.Negative} {
		for resourceName, kind := range map[string]string{"pods": "PodList", "limitranges": "LimitRangeList", "resourcequotas": "ResourceQuotaList"} {
			var list struct {
				metav1.TypeMeta `json:",inline"`
				metav1.ListMeta `json:"metadata"`
				Items           []json.RawMessage `json:"items"`
			}
			if _, err := a.request(ctx, "GET", "/api/v1/namespaces/"+ns.Name+"/"+resourceName, nil, &list); err != nil || list.Items == nil || len(list.Items) != 0 ||
				list.Kind != kind || list.APIVersion != "v1" || list.ResourceVersion == "" || list.Continue != "" || (list.RemainingItemCount != nil && *list.RemainingItemCount != 0) {
				return errors.New("dedicated Namespace list is incomplete or contains existing Pods/admission resource policies")
			}
		}
	}
	for _, pod := range pods {
		status, err := a.request(ctx, "GET", "/api/v1/namespaces/"+pod.Namespace+"/pods/"+pod.Name, nil, nil)
		if status != http.StatusNotFound || !errors.Is(err, errPodTemplateNotFound) {
			return errors.New("every exact Pod name must be absent before the first write")
		}
	}
	return nil
}

type podTemplateCaseReceipt struct {
	Case          string      `json:"case"`
	Before        *corev1.Pod `json:"before"`
	Create        *corev1.Pod `json:"create,omitempty"`
	After         *corev1.Pod `json:"after,omitempty"`
	BeforeDigest  string      `json:"beforeDigest"`
	AfterDigest   string      `json:"afterDigest,omitempty"`
	CreateStarted bool        `json:"createStarted"`
	Outcome       string      `json:"outcome"`
}

type podTemplateReceipt struct {
	ManifestSHA256 string                   `json:"manifestSHA256"`
	SourceCommit   string                   `json:"sourceCommit"`
	BinarySHA256   string                   `json:"testBinarySHA256"`
	Context        string                   `json:"context"`
	KubeSystemUID  string                   `json:"kubeSystemUID"`
	ControlPlaneID string                   `json:"controlPlaneID"`
	Positive       podTemplateNamespace     `json:"positive"`
	Negative       podTemplateNamespace     `json:"negative"`
	Cases          []podTemplateCaseReceipt `json:"cases"`
	Outcome        string                   `json:"outcome"`
	RecordedAt     time.Time                `json:"recordedAt"`
}

func TestRealAPIPodTemplateDigestRoundTrip(t *testing.T) {
	path, hash := os.Getenv(podTemplateManifestEnv), os.Getenv(podTemplateHashEnv)
	if path == "" && hash == "" {
		t.Skip("opt-in Pod-template gate requires an exact private manifest and its SHA256")
	}
	m, err := loadPodTemplateManifest(path, hash)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if err := checkPodTemplateSource(ctx, m); err != nil {
		t.Fatal(err)
	}
	raw, err := podTemplatePrivateFile(m.Kubeconfig, m.KubeconfigSHA256)
	if err != nil {
		t.Fatal(err)
	}
	cfg, port, err := podTemplateRESTConfig(raw, m.Context)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPodTemplateKind(ctx, m, port); err != nil {
		t.Fatal(err)
	}
	// O_EXCL makes the receipt an invocation lock: reruns require a fresh,
	// explicitly reviewed manifest, never overwrite an unknown prior receipt.
	f, err := os.OpenFile(m.ReceiptPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal("receipt path must be a new writable private file")
	}
	defer f.Close()
	receipt := podTemplateReceipt{ManifestSHA256: hash, SourceCommit: m.SourceCommit, BinarySHA256: m.TestBinarySHA256,
		Context: m.Context, KubeSystemUID: m.KubeSystemUID, ControlPlaneID: m.ControlPlaneID, Positive: m.Positive, Negative: m.Negative, Outcome: "INCOMPLETE"}
	checkpoint := func() {
		receipt.RecordedAt = time.Now().UTC()
		encoded, encodeErr := json.MarshalIndent(receipt, "", "  ")
		if encodeErr != nil {
			t.Fatal("receipt encoding failed; retain all API objects")
		}
		// Append-only JSONL snapshots preserve the previous intent/readback even
		// if the process exits or the final receipt write fails.
		compact := &bytes.Buffer{}
		if json.Compact(compact, encoded) != nil {
			t.Fatal("receipt compaction failed")
		}
		if _, err := f.Write(append(compact.Bytes(), '\n')); err != nil || f.Sync() != nil {
			t.Fatal("receipt checkpoint failed; retain all API objects")
		}
	}
	checkpoint()
	pods := podTemplateFixtures(m)
	guard := &podTemplateTransport{origin: cfg.Host, reads: map[string]bool{"/api/v1/namespaces/kube-system": true}, pods: map[string]*corev1.Pod{}, spent: map[string]bool{}}
	for _, ns := range []podTemplateNamespace{m.Positive, m.Negative} {
		base := "/api/v1/namespaces/" + ns.Name
		for _, suffix := range []string{"", "/serviceaccounts/default", "/secrets/" + ns.SecretName, "/pods", "/limitranges", "/resourcequotas"} {
			guard.reads[base+suffix] = true
		}
	}
	for i, pod := range pods {
		digest, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
		if err != nil {
			t.Fatal(err)
		}
		pod.Annotations = map[string]string{recoveryreceipt.PodTemplateDigestAnnotation: digest}
		// Normalize Go-only nil/empty distinctions to the exact JSON request.
		encoded, _ := json.Marshal(pod)
		var wire corev1.Pod
		if json.Unmarshal(encoded, &wire) != nil {
			t.Fatal("cannot prepare wire Pod")
		}
		pods[i] = &wire
		guard.pods[pod.Namespace+"/"+pod.Name] = &wire
		guard.reads["/api/v1/namespaces/"+pod.Namespace+"/pods/"+pod.Name] = true
		receipt.Cases = append(receipt.Cases, podTemplateCaseReceipt{Case: []string{"baseline", "limits-only", "admission-negative"}[i], Before: wire.DeepCopy(), BeforeDigest: digest, Outcome: "NOT_ATTEMPTED"})
	}
	checkpoint()
	upstream, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatal("could not construct direct certificate transport")
	}
	guard.upstream = upstream
	a := &podTemplateAPI{origin: cfg.Host, client: &http.Client{Transport: guard, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("API redirects forbidden") }}}
	check := func() {
		if _, err := loadPodTemplateManifest(path, hash); err != nil {
			t.Fatal(err)
		}
		if _, err := podTemplatePrivateFile(m.Kubeconfig, m.KubeconfigSHA256); err != nil {
			t.Fatal(err)
		}
		if err := checkPodTemplateSource(ctx, m); err != nil {
			t.Fatal(err)
		}
		if err := checkPodTemplateKind(ctx, m, port); err != nil {
			t.Fatal(err)
		}
		if err := a.checkFixtures(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	check()
	if err := a.preflight(ctx, m, pods); err != nil {
		t.Fatal(err)
	}
	for i, pod := range pods {
		check()
		c := &receipt.Cases[i]
		c.CreateStarted, c.Outcome = true, "CREATE_UNKNOWN"
		checkpoint() // fsync the exact intent before its only POST.
		var created corev1.Pod
		status, err := a.request(ctx, "POST", "/api/v1/namespaces/"+pod.Namespace+"/pods", pod, &created)
		if err != nil || status != http.StatusCreated {
			t.Fatal("Pod Create result unknown or rejected; do not retry; all objects retained")
		}
		c.Create = created.DeepCopy()
		c.Outcome = "CREATED_NOT_VERIFIED"
		checkpoint()
		if created.UID == "" || created.ResourceVersion == "" || created.Name != pod.Name || created.Namespace != pod.Namespace || podTemplateUnscheduled(&created) != nil {
			t.Fatal("created Pod identity or scheduling invariant differs; retained")
		}
		var observed corev1.Pod
		if _, err := a.request(ctx, "GET", "/api/v1/namespaces/"+pod.Namespace+"/pods/"+pod.Name, nil, &observed); err != nil {
			t.Fatal(err)
		}
		c.After = observed.DeepCopy()
		c.AfterDigest, err = recoveryreceipt.CanonicalPodTemplateDigest(&observed)
		checkpoint()
		if err != nil || observed.UID != created.UID || observed.ResourceVersion == "" || podTemplateUnscheduled(&observed) != nil ||
			observed.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] != c.BeforeDigest {
			t.Fatal("direct Pod GET lacks original identity, scheduling gate, or stamped witness")
		}
		if i < 2 && c.BeforeDigest != c.AfterDigest {
			t.Fatal("positive Pod template changed during admission; inspect retained JSON receipt")
		}
		if i == 1 && !podTemplateLimitsDefaulted(&observed) {
			t.Fatal("regular/init limits-only requests were not preserved for CPU, memory, and ephemeral storage")
		}
		if i == 2 {
			if c.BeforeDigest == c.AfterDigest || !reflect.DeepEqual(observed.Spec.ImagePullSecrets, []corev1.LocalObjectReference{{Name: m.Negative.SecretName}}) {
				t.Fatal("negative default-SA injection did not change the recomputed full canonical digest")
			}
			withoutInjection := observed.DeepCopy()
			withoutInjection.Spec.ImagePullSecrets = nil
			digest, err := recoveryreceipt.CanonicalPodTemplateDigest(withoutInjection)
			if err != nil || digest != c.BeforeDigest {
				t.Fatal("negative admission changed more than the declared pull-secret injection")
			}
		}
		c.Outcome = "PASS"
		checkpoint()
	}
	check()
	for i, pod := range pods {
		var final corev1.Pod
		if _, err := a.request(ctx, "GET", "/api/v1/namespaces/"+pod.Namespace+"/pods/"+pod.Name, nil, &final); err != nil || final.UID != receipt.Cases[i].After.UID || podTemplateUnscheduled(&final) != nil {
			t.Fatal("final retained Pod identity or never-executed invariant differs")
		}
		digest, err := recoveryreceipt.CanonicalPodTemplateDigest(&final)
		if err != nil || digest != receipt.Cases[i].AfterDigest {
			t.Fatal("retained Pod template changed after readback")
		}
		receipt.Cases[i].After = final.DeepCopy()
	}
	receipt.Outcome = "PASS"
	checkpoint()
	t.Logf("PASS: three permanently scheduling-gated Pods retained; append-only private JSONL receipt=%s", m.ReceiptPath)
}

func podTemplateLimitsDefaulted(pod *corev1.Pod) bool {
	for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		if len(containers) != 1 {
			return false
		}
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
			limit, ok := containers[0].Resources.Limits[name]
			request, present := containers[0].Resources.Requests[name]
			if !ok || !present || limit.Cmp(request) != 0 {
				return false
			}
		}
	}
	return true
}

func podTemplateUnitManifest() podTemplateManifest {
	now := time.Now().UTC()
	return podTemplateManifest{SourceDirectory: "/private/clean-kova", SourceCommit: strings.Repeat("a", 40), TestBinarySHA256: strings.Repeat("b", 64),
		Kubeconfig: "/private/gate/kubeconfig", KubeconfigSHA256: strings.Repeat("c", 64), Context: "kind-kova-pod-template-unit",
		KubeSystemUID: "system-uid", ControlPlaneID: strings.Repeat("d", 64), PreparedAfter: now.Add(-time.Minute), ExpiresAt: now.Add(10 * time.Minute), ReceiptPath: "/private/gate/receipt.jsonl",
		Positive:    podTemplateNamespace{Name: "kova-pod-template-positive", UID: "positive-uid", ServiceAccountUID: "positive-sa-uid", ServiceAccountRV: "12", SecretName: "kova-pod-template-dummy", SecretUID: "positive-secret-uid", SecretRV: "13"},
		Negative:    podTemplateNamespace{Name: "kova-pod-template-negative", UID: "negative-uid", ServiceAccountUID: "negative-sa-uid", ServiceAccountRV: "22", SecretName: "kova-pod-template-dummy", SecretUID: "negative-secret-uid", SecretRV: "23"},
		BaselinePod: "kova-pod-template-baseline", LimitsPod: "kova-pod-template-limits", NegativePod: "kova-pod-template-injected"}
}

func TestPodTemplateGateManifestRefusals(t *testing.T) {
	m := podTemplateUnitManifest()
	if err := validatePodTemplateManifest(m, time.Now()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*podTemplateManifest){
		"shared context":                func(m *podTemplateManifest) { m.Context = "kind-kova-rc-api-old" },
		"missing source":                func(m *podTemplateManifest) { m.SourceCommit = "" },
		"abbreviated source":            func(m *podTemplateManifest) { m.SourceCommit = "abc123" },
		"missing binary pin":            func(m *podTemplateManifest) { m.TestBinarySHA256 = "" },
		"missing config pin":            func(m *podTemplateManifest) { m.KubeconfigSHA256 = "" },
		"relative config":               func(m *podTemplateManifest) { m.Kubeconfig = "kubeconfig" },
		"missing control ID":            func(m *podTemplateManifest) { m.ControlPlaneID = "" },
		"namespace reused":              func(m *podTemplateManifest) { m.Negative.Name = m.Positive.Name },
		"namespace UID reused":          func(m *podTemplateManifest) { m.Negative.UID = m.Positive.UID },
		"system UID reused":             func(m *podTemplateManifest) { m.Positive.UID = m.KubeSystemUID },
		"missing SA UID":                func(m *podTemplateManifest) { m.Positive.ServiceAccountUID = "" },
		"missing SA RV":                 func(m *podTemplateManifest) { m.Positive.ServiceAccountRV = "" },
		"missing secret UID":            func(m *podTemplateManifest) { m.Negative.SecretUID = "" },
		"missing secret RV":             func(m *podTemplateManifest) { m.Negative.SecretRV = "" },
		"old namespace":                 func(m *podTemplateManifest) { m.Positive.Name = "default" },
		"reused Pod name":               func(m *podTemplateManifest) { m.NegativePod = m.BaselinePod },
		"expired window":                func(m *podTemplateManifest) { m.ExpiresAt = time.Now().Add(-time.Second) },
		"unbounded freshness":           func(m *podTemplateManifest) { m.ExpiresAt = m.PreparedAfter.Add(2 * time.Hour) },
		"future installation":           func(m *podTemplateManifest) { m.PreparedAfter = time.Now().Add(time.Minute) },
		"receipt overwrites credential": func(m *podTemplateManifest) { m.ReceiptPath = m.Kubeconfig },
	} {
		t.Run(name, func(t *testing.T) {
			changed := m
			mutate(&changed)
			if validatePodTemplateManifest(changed, time.Now()) == nil {
				t.Fatal("unsafe/incomplete manifest accepted")
			}
		})
	}
	for _, raw := range []string{`{"context":"one","context":"two"}`, `{"cleanupOnSuccess":true}`, `{"positive":{"uid":"one","uid":"two"}}`, `{"positive":{"other":"field"}}`} {
		var decoded podTemplateManifest
		if admissionjson.Decode([]byte(raw), &decoded, podTemplateManifestField) == nil {
			t.Fatal("duplicate or unknown manifest field accepted")
		}
	}
}

func TestPodTemplateGatePrivateFileRefusals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	raw := []byte(`{"private":"fixture"}`)
	hash := sha256.Sum256(raw)
	pin := hex.EncodeToString(hash[:])
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := podTemplatePrivateFile(path, pin); err != nil {
		t.Fatal(err)
	}
	if _, err := podTemplatePrivateFile(path, strings.Repeat("a", 64)); err == nil {
		t.Fatal("wrong hash accepted")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := podTemplatePrivateFile(path, pin); err == nil {
		t.Fatal("nonprivate file accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(path), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := podTemplatePrivateFile(link, pin); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestPodTemplateGateKubeconfigRefusesProxyAndAlternateAuthority(t *testing.T) {
	m := podTemplateUnitManifest()
	base := clientcmdapi.Config{CurrentContext: m.Context,
		Contexts:  map[string]*clientcmdapi.Context{m.Context: {Cluster: "dedicated", AuthInfo: "dedicated"}},
		Clusters:  map[string]*clientcmdapi.Cluster{"dedicated": {Server: "https://127.0.0.1:6443", CertificateAuthorityData: []byte("unit-ca")}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"dedicated": {ClientCertificateData: []byte("unit-cert"), ClientKeyData: []byte("unit-key")}}}
	// These environment proxies must not affect the explicitly direct client.
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	raw, err := clientcmd.Write(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg, port, err := podTemplateRESTConfig(raw, m.Context)
	if err != nil || port != "6443" {
		t.Fatalf("direct config rejected: %v", err)
	}
	if proxy, err := cfg.Proxy(&http.Request{}); err != nil || proxy != nil {
		t.Fatal("ambient proxy not disabled")
	}
	for name, mutate := range map[string]func(*clientcmdapi.Config){
		"proxy":         func(c *clientcmdapi.Config) { c.Clusters["dedicated"].ProxyURL = "http://127.0.0.1:1" },
		"remote":        func(c *clientcmdapi.Config) { c.Clusters["dedicated"].Server = "https://example.invalid:6443" },
		"http":          func(c *clientcmdapi.Config) { c.Clusters["dedicated"].Server = "http://127.0.0.1:6443" },
		"path":          func(c *clientcmdapi.Config) { c.Clusters["dedicated"].Server += "/proxy" },
		"no port":       func(c *clientcmdapi.Config) { c.Clusters["dedicated"].Server = "https://127.0.0.1" },
		"TLS insecure":  func(c *clientcmdapi.Config) { c.Clusters["dedicated"].InsecureSkipTLSVerify = true },
		"external CA":   func(c *clientcmdapi.Config) { c.Clusters["dedicated"].CertificateAuthority = "/other/ca" },
		"external cert": func(c *clientcmdapi.Config) { c.AuthInfos["dedicated"].ClientCertificate = "/other/cert" },
		"external key":  func(c *clientcmdapi.Config) { c.AuthInfos["dedicated"].ClientKey = "/other/key" },
		"exec credential": func(c *clientcmdapi.Config) {
			c.AuthInfos["dedicated"].Exec = &clientcmdapi.ExecConfig{Command: "unapproved"}
		},
		"bearer token":  func(c *clientcmdapi.Config) { c.AuthInfos["dedicated"].Token = "unit-token" },
		"impersonation": func(c *clientcmdapi.Config) { c.AuthInfos["dedicated"].Impersonate = "other" },
		"extra context": func(c *clientcmdapi.Config) { c.Contexts["shared"] = &clientcmdapi.Context{} },
		"wrong current": func(c *clientcmdapi.Config) { c.CurrentContext = "shared" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base.DeepCopy()
			mutate(changed)
			raw, err := clientcmd.Write(*changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := podTemplateRESTConfig(raw, m.Context); err == nil {
				t.Fatal("unsafe kubeconfig accepted")
			}
		})
	}
}

type podTemplateRoundTripFunc func(*http.Request) (*http.Response, error)

func (f podTemplateRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func podTemplateResponse(status int, value any) *http.Response {
	raw, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}
}

func podTemplateNotFound(name string) *metav1.Status {
	return &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure,
		Code: http.StatusNotFound, Reason: metav1.StatusReasonNotFound, Details: &metav1.StatusDetails{Name: name, Kind: "pods"}}
}

func podTemplateEmptyList(kind string) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"resourceVersion": "100"}, "items": []any{}}
}

func TestPodTemplateGateTransportAtMostOneCreate(t *testing.T) {
	m := podTemplateUnitManifest()
	pod := podTemplateFixtures(m)[0]
	raw, _ := json.Marshal(pod)
	var wire corev1.Pod
	if json.Unmarshal(raw, &wire) != nil {
		t.Fatal("bad fixture")
	}
	for _, outcome := range []string{"lost-reply", "429", "500", "307", "201"} {
		t.Run(outcome, func(t *testing.T) {
			calls := 0
			guard := &podTemplateTransport{origin: "https://127.0.0.1:6443", reads: map[string]bool{}, pods: map[string]*corev1.Pod{pod.Namespace + "/" + pod.Name: &wire}, spent: map[string]bool{},
				upstream: podTemplateRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.GetBody != nil || r.Header.Get("Idempotency-Key") != "" {
						t.Fatal("POST became replayable")
					}
					if outcome == "lost-reply" {
						return nil, errors.New("lost reply after persistence")
					}
					status, _ := strconv.Atoi(outcome)
					response := podTemplateResponse(status, pod)
					response.Header.Set("Retry-After", "0")
					response.Header.Set("Location", "https://127.0.0.1:6443/other")
					return response, nil
				})}
			a := &podTemplateAPI{origin: guard.origin, client: &http.Client{Transport: guard,
				CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect forbidden") }}}
			for attempt := 0; attempt < 2; attempt++ {
				_, err := a.request(t.Context(), "POST", "/api/v1/namespaces/"+pod.Namespace+"/pods", &wire, nil)
				if attempt == 1 && err == nil {
					t.Fatal("second Create attempt accepted")
				}
			}
			if calls != 1 {
				t.Fatalf("Create reached transport %d times", calls)
			}
		})
	}
}

func TestPodTemplateGateTransportRefusesExtraOperations(t *testing.T) {
	m := podTemplateUnitManifest()
	pod := podTemplateFixtures(m)[0]
	raw, _ := json.Marshal(pod)
	var wire corev1.Pod
	_ = json.Unmarshal(raw, &wire)
	base := "/api/v1/namespaces/" + pod.Namespace
	for _, tc := range []struct{ method, path string }{
		{"DELETE", base + "/pods/" + pod.Name}, {"PATCH", base + "/pods/" + pod.Name}, {"PUT", base + "/pods/" + pod.Name},
		{"POST", base + "/pods/" + pod.Name + "/exec"}, {"POST", base + "/secrets"}, {"POST", "/api/v1/namespaces/default/pods"},
		{"GET", "/api/v1/namespaces/default/pods"}, {"POST", base + "/pods?dryRun=All"},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			guard := &podTemplateTransport{origin: "https://127.0.0.1:6443", reads: map[string]bool{}, pods: map[string]*corev1.Pod{pod.Namespace + "/" + pod.Name: &wire}, spent: map[string]bool{},
				upstream: podTemplateRoundTripFunc(func(*http.Request) (*http.Response, error) {
					t.Fatal("forbidden operation reached network")
					return nil, nil
				})}
			r, _ := http.NewRequest(tc.method, guard.origin+tc.path, bytes.NewReader(raw))
			if _, err := guard.RoundTrip(r); err == nil {
				t.Fatal("forbidden operation accepted")
			}
		})
	}
	for _, mutate := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Spec.SchedulingGates = nil },
		func(p *corev1.Pod) { p.Spec.NodeName = "some-node" },
		func(p *corev1.Pod) { p.Spec.Affinity = nil },
		func(p *corev1.Pod) { p.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways },
		func(p *corev1.Pod) { p.Name = "undeclared-pod" },
	} {
		changed := wire.DeepCopy()
		mutate(changed)
		guard := &podTemplateTransport{origin: "https://127.0.0.1:6443", pods: map[string]*corev1.Pod{pod.Namespace + "/" + pod.Name: &wire}, spent: map[string]bool{},
			upstream: podTemplateRoundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unsafe Pod reached network"); return nil, nil })}
		raw, _ := json.Marshal(changed)
		r, _ := http.NewRequest("POST", guard.origin+base+"/pods", bytes.NewReader(raw))
		if _, err := guard.RoundTrip(r); err == nil {
			t.Fatal("unsafe or undeclared Pod accepted")
		}
	}
}

func TestPodTemplateGatePreflightRefusals(t *testing.T) {
	m := podTemplateUnitManifest()
	now := metav1.Now()
	objects := func() map[string]any {
		objects := map[string]any{"/api/v1/namespaces/kube-system": &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: types.UID(m.KubeSystemUID), ResourceVersion: "1", CreationTimestamp: now}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}}
		for i, ns := range []podTemplateNamespace{m.Positive, m.Negative} {
			base := "/api/v1/namespaces/" + ns.Name
			objects[base] = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns.Name, UID: types.UID(ns.UID), ResourceVersion: "2", CreationTimestamp: now}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
			sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns.Name, UID: types.UID(ns.ServiceAccountUID), ResourceVersion: ns.ServiceAccountRV}}
			if i == 1 {
				sa.ImagePullSecrets = []corev1.LocalObjectReference{{Name: ns.SecretName}}
			}
			objects[base+"/serviceaccounts/default"] = sa
			yes := true
			objects[base+"/secrets/"+ns.SecretName] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ns.SecretName, Namespace: ns.Name, UID: types.UID(ns.SecretUID), ResourceVersion: ns.SecretRV},
				Immutable: &yes, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)}}
			for list, kind := range map[string]string{"pods": "PodList", "limitranges": "LimitRangeList", "resourcequotas": "ResourceQuotaList"} {
				objects[base+"/"+list] = podTemplateEmptyList(kind)
			}
		}
		return objects
	}
	for name, mutate := range map[string]func(map[string]any){
		"valid": nil,
		"replaced namespace": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name].(*corev1.Namespace).UID = "replacement"
		},
		"old namespace": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name].(*corev1.Namespace).CreationTimestamp = metav1.NewTime(m.PreparedAfter.Add(-time.Second))
		},
		"terminating namespace": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Negative.Name].(*corev1.Namespace).DeletionTimestamp = &now
		},
		"changed SA": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Negative.Name+"/serviceaccounts/default"].(*corev1.ServiceAccount).ResourceVersion = "changed"
		},
		"unexpected injection": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/serviceaccounts/default"].(*corev1.ServiceAccount).ImagePullSecrets = []corev1.LocalObjectReference{{Name: "unexpected"}}
		},
		"missing injection": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Negative.Name+"/serviceaccounts/default"].(*corev1.ServiceAccount).ImagePullSecrets = nil
		},
		"replaced Secret": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/secrets/"+m.Positive.SecretName].(*corev1.Secret).UID = "replacement"
		},
		"non-dummy Secret": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/secrets/"+m.Positive.SecretName].(*corev1.Secret).Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{"unexpected":{}}}`)
		},
		"existing Pod in list": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Negative.Name+"/pods"] = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "old"}}}}
		},
		"existing exact Pod": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods/"+m.BaselinePod] = podTemplateFixtures(m)[0]
		},
		"limit policy": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/limitranges"] = map[string]any{"items": []any{map[string]any{}}}
		},
		"empty object is not list": func(o map[string]any) { o["/api/v1/namespaces/"+m.Positive.Name+"/pods"] = map[string]any{} },
		"null is not list":         func(o map[string]any) { o["/api/v1/namespaces/"+m.Positive.Name+"/pods"] = nil },
		"wrong list kind": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods"] = podTemplateEmptyList("Status")
		},
		"list missing RV": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods"].(map[string]any)["metadata"] = map[string]any{}
		},
		"list missing items": func(o map[string]any) {
			delete(o["/api/v1/namespaces/"+m.Positive.Name+"/pods"].(map[string]any), "items")
		},
		"list null items": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods"].(map[string]any)["items"] = nil
		},
		"partial empty page": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods"].(map[string]any)["metadata"] = map[string]any{"resourceVersion": "100", "continue": "more"}
		},
		"remaining objects": func(o map[string]any) {
			o["/api/v1/namespaces/"+m.Positive.Name+"/pods"].(map[string]any)["metadata"] = map[string]any{"resourceVersion": "100", "remainingItemCount": 1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := objects()
			if mutate != nil {
				mutate(o)
			}
			a := &podTemplateAPI{origin: "https://127.0.0.1:6443", client: &http.Client{Transport: podTemplateRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" {
					t.Fatal("preflight wrote an API object")
				}
				if object, ok := o[r.URL.Path]; ok {
					return podTemplateResponse(http.StatusOK, object), nil
				}
				return podTemplateResponse(http.StatusNotFound, podTemplateNotFound(filepath.Base(r.URL.Path))), nil
			})}}
			err := a.preflight(t.Context(), m, podTemplateFixtures(m))
			if (err != nil) != (mutate != nil) {
				t.Fatalf("preflight error = %v", err)
			}
		})
	}
}

type podTemplateFailedReader struct{}

func (podTemplateFailedReader) Read([]byte) (int, error) { return 0, errors.New("truncated API reply") }
func (podTemplateFailedReader) Close() error             { return nil }

func TestPodTemplateGateNotFoundRequiresCompleteTypedResponse(t *testing.T) {
	name := "kova-pod-template-baseline"
	path := "/api/v1/namespaces/kova-pod-template-positive/pods/" + name
	for label, response := range map[string]*http.Response{
		"valid":        podTemplateResponse(http.StatusNotFound, podTemplateNotFound(name)),
		"empty":        podTemplateResponse(http.StatusNotFound, map[string]any{}),
		"null":         podTemplateResponse(http.StatusNotFound, nil),
		"wrong object": podTemplateResponse(http.StatusNotFound, podTemplateNotFound("different-pod")),
		"wrong code":   podTemplateResponse(http.StatusForbidden, podTemplateNotFound(name)),
		"failed read":  {StatusCode: http.StatusNotFound, Header: make(http.Header), Body: podTemplateFailedReader{}},
		"oversized":    {StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(strings.Repeat(" ", 1024*1024+1)))},
		"truncated":    {StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"kind":"Status"`))},
	} {
		t.Run(label, func(t *testing.T) {
			a := &podTemplateAPI{origin: "https://127.0.0.1:6443", client: &http.Client{Transport: podTemplateRoundTripFunc(func(*http.Request) (*http.Response, error) { return response, nil })}}
			_, err := a.request(t.Context(), "GET", path, nil, nil)
			if errors.Is(err, errPodTemplateNotFound) != (label == "valid") {
				t.Fatalf("absence classification = %v", err)
			}
		})
	}
}

func TestPodTemplateGateFixtureCoversProductionDefaultsAndInjection(t *testing.T) {
	m := podTemplateUnitManifest()
	pods := podTemplateFixtures(m)
	for i, pod := range pods {
		if err := podTemplateUnscheduled(pod); err != nil {
			t.Fatal(err)
		}
		if len(pod.Spec.Volumes) != 2 || pod.Spec.Volumes[1].Secret == nil || *pod.Spec.Volumes[1].Secret.DefaultMode != 0644 ||
			pod.Spec.InitContainers[0].Name != "source-fetch" || len(pod.Spec.InitContainers[0].VolumeMounts) != 2 {
			t.Fatal("fixture lost real runner source-fetch/Secret-volume defaults")
		}
		before, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
		if err != nil {
			t.Fatal(err)
		}
		pod.Annotations = map[string]string{recoveryreceipt.PodTemplateDigestAnnotation: before}
		if i == 1 && !podTemplateLimitsDefaulted(pod) {
			t.Fatal("Genesis did not default regular and init requests from all three limits")
		}
		if i == 2 {
			pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: m.Negative.SecretName}}
			after, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
			if err != nil || before == after || pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] != before {
				t.Fatal("full digest did not reject injected imagePullSecrets despite the stamped annotation")
			}
		}
	}
}
