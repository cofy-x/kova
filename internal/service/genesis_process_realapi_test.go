package service

import (
	"bufio"
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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Opt in with KOVA_GENESIS_PROCESS_API_MANIFEST=/absolute/private/matrix.json.
// The external installer must first create ten distinct, fresh runner
// Namespaces, Initializing Genesises, and immutable receipt Secrets. This gate
// creates only the two runtime ledgers; it never starts a Service, Pod, runner,
// manager, listener, registry, or leader. No API object is removed on failure.
// A passing gate is process-loss evidence around prepareGenesisRuntime only.
const (
	processManifestEnv = "KOVA_GENESIS_PROCESS_API_MANIFEST"
	processChildEnv    = "KOVA_GENESIS_PROCESS_CHILD"
	processHashEnv     = "KOVA_GENESIS_PROCESS_MANIFEST_SHA256"
	processCaseEnv     = "KOVA_GENESIS_PROCESS_CASE_INDEX"
	processDeadlineEnv = "KOVA_GENESIS_PROCESS_CHILD_DEADLINE_UNIX_NANO"
	processTotalLimit  = 12 * time.Minute
	processChildLimit  = 45 * time.Second
)

var processStages = [...]string{"active-create", "active-pin", "queue-create", "queue-pin", "commit"}
var processPoints = [...]string{"before", "after"}

type processManifest struct {
	Kubeconfig       string        `json:"kubeconfig"`
	KubeconfigSHA256 string        `json:"kubeconfigSHA256"`
	Context          string        `json:"context"`
	KubeSystemUID    string        `json:"kubeSystemUID"`
	ControlPlaneID   string        `json:"controlPlaneID"`
	CleanupOnSuccess *bool         `json:"cleanupOnSuccess"`
	Cases            []processCase `json:"cases"`
}

type processCase struct {
	Stage              string `json:"stage"`
	Point              string `json:"point"`
	Namespace          string `json:"namespace"`
	NamespaceUID       string `json:"namespaceUID"`
	GenesisUID         string `json:"genesisUID"`
	ReceiptFile        string `json:"receiptFile"`
	ReceiptSHA256      string `json:"receiptSHA256"`
	SecretNamespace    string `json:"secretNamespace"`
	SecretNamespaceUID string `json:"secretNamespaceUID"`
	SecretName         string `json:"secretName"`
	SecretUID          string `json:"secretUID"`
}

func processManifestField(path []string, key string) bool {
	if len(path) == 0 {
		switch key {
		case "kubeconfig", "kubeconfigSHA256", "context", "kubeSystemUID", "controlPlaneID", "cleanupOnSuccess", "cases":
			return true
		}
	}
	if len(path) == 1 && path[0] == "cases" {
		switch key {
		case "stage", "point", "namespace", "namespaceUID", "genesisUID", "receiptFile", "receiptSHA256", "secretNamespace", "secretNamespaceUID", "secretName", "secretUID":
			return true
		}
	}
	return false
}

func validProcessHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func dedicatedProcessContext(value string) bool {
	return strings.HasPrefix(value, "kind-kova-genesis-api-") || strings.HasPrefix(value, "kind-kova-genesis-process-")
}

func dedicatedProcessNamespace(value string) bool {
	return strings.HasPrefix(value, "kova-genesis-api-proc-") || strings.HasPrefix(value, "kova-genesis-process-")
}

func validateProcessManifest(m processManifest) error {
	if !filepath.IsAbs(m.Kubeconfig) || !validProcessHash(m.KubeconfigSHA256) ||
		!dedicatedProcessContext(m.Context) ||
		!admissioncontract.ValidUID(m.KubeSystemUID) || !validProcessHash(m.ControlPlaneID) ||
		m.CleanupOnSuccess == nil || len(m.Cases) != len(processStages)*len(processPoints) {
		return errors.New("process gate requires a complete dedicated Kind target and ten explicit cases")
	}
	seenCase := map[string]bool{}
	seenNamespace := map[string]bool{}
	seenNamespaceUID := map[string]bool{}
	seenGenesisUID := map[string]bool{}
	seenSecret := map[string]bool{}
	seenSecretUID := map[string]bool{}
	for _, c := range m.Cases {
		validStage, validPoint := false, false
		for _, stage := range processStages {
			validStage = validStage || c.Stage == stage
		}
		for _, point := range processPoints {
			validPoint = validPoint || c.Point == point
		}
		key := c.Stage + "/" + c.Point
		secretKey := c.SecretNamespace + "/" + c.SecretName
		if !validStage || !validPoint || seenCase[key] ||
			!dedicatedProcessNamespace(c.Namespace) || len(validation.IsDNS1123Label(c.Namespace)) != 0 ||
			!admissioncontract.ValidUID(c.NamespaceUID) || c.NamespaceUID == m.KubeSystemUID ||
			!admissioncontract.ValidUID(c.GenesisUID) ||
			!filepath.IsAbs(c.ReceiptFile) || !validProcessHash(c.ReceiptSHA256) ||
			!dedicatedProcessNamespace(c.SecretNamespace) || len(validation.IsDNS1123Label(c.SecretNamespace)) != 0 || c.SecretNamespace == c.Namespace ||
			!admissioncontract.ValidUID(c.SecretNamespaceUID) || c.SecretNamespaceUID == c.NamespaceUID ||
			len(validation.IsDNS1123Label(c.SecretName)) != 0 || !admissioncontract.ValidUID(c.SecretUID) ||
			seenNamespace[c.Namespace] || seenNamespaceUID[c.NamespaceUID] || seenGenesisUID[c.GenesisUID] ||
			seenSecret[secretKey] || seenSecretUID[c.SecretUID] {
			return fmt.Errorf("process gate case %s has incomplete or reused original identities", key)
		}
		seenCase[key], seenNamespace[c.Namespace], seenNamespaceUID[c.NamespaceUID] = true, true, true
		seenGenesisUID[c.GenesisUID], seenSecret[secretKey], seenSecretUID[c.SecretUID] = true, true, true
	}
	for _, stage := range processStages {
		for _, point := range processPoints {
			if !seenCase[stage+"/"+point] {
				return errors.New("process gate omits a bootstrap write point")
			}
		}
	}
	return nil
}

func readPinnedProcessFile(path, expectedHash string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) || !validProcessHash(expectedHash) {
		return nil, errors.New("process gate file lacks an absolute path or pinned SHA256")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("process gate file is missing, symlinked, or outside its size bound")
	}
	raw, err := os.ReadFile(path)
	if err != nil || int64(len(raw)) != info.Size() {
		return nil, errors.New("process gate file changed or became unreadable")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != expectedHash {
		return nil, errors.New("process gate file SHA256 changed")
	}
	return raw, nil
}

func loadProcessManifest(path, pinnedHash string) (processManifest, string, error) {
	if !filepath.IsAbs(path) {
		return processManifest{}, "", errors.New("process gate manifest path must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64*1024 {
		return processManifest{}, "", errors.New("process gate manifest is missing, symlinked, or outside its size bound")
	}
	raw, err := os.ReadFile(path)
	if err != nil || int64(len(raw)) != info.Size() {
		return processManifest{}, "", errors.New("process gate manifest changed or became unreadable")
	}
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	if pinnedHash != "" && hash != pinnedHash {
		return processManifest{}, "", errors.New("process gate manifest changed between processes")
	}
	var m processManifest
	if err := admissionjson.Decode(raw, &m, processManifestField); err != nil {
		return processManifest{}, "", err
	}
	if err := validateProcessManifest(m); err != nil {
		return processManifest{}, "", err
	}
	return m, hash, nil
}

func processConfig(c processCase, receipt admissioncontract.Receipt) config.Config {
	limits := receipt.Contract.Limits
	return config.Config{Namespace: c.Namespace,
		MaxActiveJobs: limits.MaxActiveJobs, MaxActiveJobsPerRequester: limits.MaxActiveJobsPerRequester,
		WorkerSlots: limits.WorkerSlots, MaxQueuedJobs: limits.MaxQueuedJobs,
		MaxQueuedJobsPerRequester: limits.MaxQueuedJobsPerRequester}
}

func processReceipt(c processCase) ([]byte, admissioncontract.Receipt, error) {
	raw, err := readPinnedProcessFile(c.ReceiptFile, c.ReceiptSHA256, admissioncontract.MaxContractJSONBytes)
	if err != nil {
		return nil, admissioncontract.Receipt{}, err
	}
	receipt, err := admissioncontract.ParseReceipt(raw)
	if err != nil {
		return nil, admissioncontract.Receipt{}, err
	}
	if receipt.Namespace != c.Namespace || receipt.Contract.NamespaceUID != c.NamespaceUID ||
		receipt.GenesisUID != c.GenesisUID || receipt.GenesisName != admissioncontract.GenesisName {
		return nil, admissioncontract.Receipt{}, errors.New("process gate receipt differs from explicit original identities")
	}
	return raw, receipt, nil
}

type processAPI struct {
	direct    admissiongenesis.DirectClient
	reader    client.Client
	clientset kubernetes.Interface
}

func processCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	commandCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	out, err := exec.CommandContext(commandCtx, name, args...).Output()
	if err != nil || len(out) > 64*1024 {
		return nil, errors.New("dedicated Kind identity command failed or exceeded its bound")
	}
	return out, nil
}

func checkProcessKind(ctx context.Context, m processManifest, port string) error {
	cluster := strings.TrimPrefix(m.Context, "kind-")
	nodeName := cluster + "-control-plane"
	nodes, err := processCommand(ctx, "kind", "get", "nodes", "--name", cluster)
	if err != nil {
		return err
	}
	found := false
	for _, node := range strings.Fields(string(nodes)) {
		found = found || node == nodeName
	}
	if !found {
		return errors.New("dedicated Kind control-plane is absent")
	}
	const format = `{"id":{{json .Id}},"running":{{json .State.Running}},"cluster":{{json (index .Config.Labels "io.x-k8s.kind.cluster")}},"role":{{json (index .Config.Labels "io.x-k8s.kind.role")}},"ports":{{json (index .NetworkSettings.Ports "6443/tcp")}}}`
	raw, err := processCommand(ctx, "docker", "inspect", "--format", format, nodeName)
	if err != nil {
		return err
	}
	var node struct {
		ID      string `json:"id"`
		Running bool   `json:"running"`
		Cluster string `json:"cluster"`
		Role    string `json:"role"`
		Ports   []struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"ports"`
	}
	if json.Unmarshal(raw, &node) != nil || node.ID != m.ControlPlaneID || !node.Running ||
		node.Cluster != cluster || node.Role != "control-plane" || len(node.Ports) != 1 ||
		node.Ports[0].HostIP != "127.0.0.1" || node.Ports[0].HostPort != port {
		return errors.New("Kind control-plane ID, labels, or loopback API binding changed")
	}
	return nil
}

func openProcessAPI(ctx context.Context, m processManifest) (*processAPI, error) {
	kubeconfig, err := readPinnedProcessFile(m.Kubeconfig, m.KubeconfigSHA256, 64*1024)
	if err != nil {
		return nil, err
	}
	loaded, err := clientcmd.Load(kubeconfig)
	if err != nil || loaded.CurrentContext != m.Context {
		return nil, errors.New("Kind kubeconfig lacks the exact pinned current context")
	}
	selected := loaded.Contexts[m.Context]
	if selected == nil || loaded.Clusters[selected.Cluster] == nil || loaded.AuthInfos[selected.AuthInfo] == nil {
		return nil, errors.New("Kind kubeconfig lacks a complete exact context")
	}
	cluster := loaded.Clusters[selected.Cluster]
	auth := loaded.AuthInfos[selected.AuthInfo]
	endpoint, err := url.Parse(cluster.Server)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() != "127.0.0.1" ||
		endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		cluster.ProxyURL != "" || auth.Exec != nil || auth.AuthProvider != nil ||
		len(cluster.CertificateAuthorityData) == 0 || len(auth.ClientCertificateData) == 0 || len(auth.ClientKeyData) == 0 ||
		auth.Token != "" || auth.TokenFile != "" {
		return nil, errors.New("Kind kubeconfig is not a direct loopback HTTPS target")
	}
	port, err := strconv.Atoi(endpoint.Port())
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("Kind kubeconfig lacks a valid explicit API port")
	}
	if err := checkProcessKind(ctx, m, strconv.Itoa(port)); err != nil {
		return nil, err
	}
	restConfig, err := clientcmd.NewNonInteractiveClientConfig(*loaded, m.Context, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil || restConfig.Host != cluster.Server {
		return nil, errors.New("Kind kubeconfig could not construct a direct client")
	}
	restConfig.Timeout = 10 * time.Second
	restConfig.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	restConfig = singleAttemptWrites(restConfig)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	crReader, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, errors.New("Kind direct CR/Pod client construction failed")
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, errors.New("Kind direct core client construction failed")
	}
	api := &processAPI{direct: admissiongenesis.DirectClient{Client: clientset}, reader: crReader, clientset: clientset}
	if err := api.checkSystem(ctx, m); err != nil {
		return nil, err
	}
	return api, nil
}

func (a *processAPI) checkSystem(ctx context.Context, m processManifest) error {
	ns, err := a.direct.GetNamespace(ctx, "kube-system")
	if err != nil || string(ns.UID) != m.KubeSystemUID || ns.Status.Phase != corev1.NamespaceActive || ns.DeletionTimestamp != nil {
		return errors.New("dedicated Kind kube-system UID or phase changed")
	}
	return nil
}

type processSnapshot struct {
	State      admissioncontract.GenesisData
	Immutable  bool
	ActiveUID  string
	QueueUID   string
	GenesisUID string
	SecretUID  string
}

func (a *processAPI) snapshot(ctx context.Context, m processManifest, c processCase, receipt admissioncontract.Receipt, raw []byte) (processSnapshot, error) {
	if err := a.checkSystem(ctx, m); err != nil {
		return processSnapshot{}, err
	}
	ns, err := a.direct.GetNamespace(ctx, c.Namespace)
	if err != nil || receipt.QualifyNamespace(ns) != nil {
		return processSnapshot{}, errors.New("original process-test Namespace changed")
	}
	controlNS, err := a.direct.GetNamespace(ctx, c.SecretNamespace)
	if err != nil || controlNS == nil || string(controlNS.UID) != c.SecretNamespaceUID ||
		controlNS.Status.Phase != corev1.NamespaceActive || controlNS.DeletionTimestamp != nil {
		return processSnapshot{}, errors.New("original process-test control Namespace changed")
	}
	secretCheck := (genesisReceiptOptions{SecretNamespace: c.SecretNamespace, SecretName: c.SecretName, SecretUID: c.SecretUID}).checkRaw(raw, a.direct)
	if err := secretCheck(ctx); err != nil {
		return processSnapshot{}, errors.New("original immutable receipt Secret changed")
	}
	genesis, err := a.direct.GetConfigMap(ctx, c.Namespace, admissioncontract.GenesisName)
	if err != nil {
		return processSnapshot{}, errors.New("original process-test Genesis is unavailable")
	}
	state, err := receipt.QualifyGenesis(genesis)
	if err != nil {
		return processSnapshot{}, errors.New("original process-test Genesis changed")
	}
	s := processSnapshot{State: state, GenesisUID: string(genesis.UID), SecretUID: c.SecretUID,
		Immutable: genesis.Immutable != nil && *genesis.Immutable}
	cfg := processConfig(c, receipt)
	activeEmpty, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		return processSnapshot{}, err
	}
	queueEmpty, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		return processSnapshot{}, err
	}
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		name, key, expected := admissioncontract.ActiveLedgerName, admissioncontract.ActiveLedgerDataKey, activeEmpty
		if role == admissioncontract.Queue {
			name, key, expected = admissioncontract.QueueLedgerName, admissioncontract.QueueLedgerDataKey, queueEmpty
		}
		ledger, err := a.direct.GetConfigMap(ctx, c.Namespace, name)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil || receipt.QualifyLedgerIdentity(ledger, role) != nil ||
			len(ledger.Data) != 1 || len(ledger.BinaryData) != 0 || ledger.Data[key] != expected {
			return processSnapshot{}, errors.New("process-test ledger is missing canonical original empty data")
		}
		if role == admissioncontract.Active {
			if err := buildcontroller.ValidateAdmissionLedgerForGenesis(ledger, cfg); err != nil {
				return processSnapshot{}, err
			}
			s.ActiveUID = string(ledger.UID)
		} else {
			if err := queueadmission.ValidateQueueLedgerForGenesis(ledger, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester); err != nil {
				return processSnapshot{}, err
			}
			s.QueueUID = string(ledger.UID)
		}
	}
	return s, nil
}

func assertProcessSnapshot(s processSnapshot, c processCase, completed int) error {
	if s.GenesisUID != c.GenesisUID || s.SecretUID != c.SecretUID || completed < 0 || completed > len(processStages) {
		return errors.New("process-test original Genesis or Secret identity changed")
	}
	activePresent, queuePresent := s.ActiveUID != "", s.QueueUID != ""
	if activePresent != (completed >= 1) || queuePresent != (completed >= 3) {
		return errors.New("process-test ledger presence differs from the paused write point")
	}
	if (completed >= 2) != (s.State.ActiveLedgerUID != "") ||
		(completed >= 4) != (s.State.QueueLedgerUID != "") ||
		(completed >= 2 && s.State.ActiveLedgerUID != s.ActiveUID) ||
		(completed >= 4 && s.State.QueueLedgerUID != s.QueueUID) {
		return errors.New("process-test provisional ledger UID pins differ")
	}
	if completed == len(processStages) {
		if s.State.Phase != admissioncontract.PhaseCommitted || !s.Immutable {
			return errors.New("process-test Genesis did not commit immutably")
		}
	} else if s.State.Phase != admissioncontract.PhaseInitializing || s.Immutable {
		return errors.New("process-test Genesis advanced before its commit write")
	}
	return nil
}

func processPreflight(ctx context.Context, a *processAPI, m processManifest, c processCase) error {
	raw, receipt, err := processReceipt(c)
	if err != nil {
		return err
	}
	s, err := a.snapshot(ctx, m, c, receipt, raw)
	if err != nil {
		return err
	}
	if err := assertProcessSnapshot(s, c, 0); err != nil {
		return err
	}
	var builds kovav1.KovaBuildList
	var pods corev1.PodList
	if err := a.reader.List(ctx, &builds, client.InNamespace(c.Namespace)); err != nil {
		return errors.New("process-test KovaBuild veto List failed")
	}
	if err := a.reader.List(ctx, &pods, client.InNamespace(c.Namespace)); err != nil {
		return errors.New("process-test Pod veto List failed")
	}
	if len(builds.Items) != 0 || len(pods.Items) != 0 {
		return errors.New("process-test runner Namespace contains visible old work")
	}
	var controlPods corev1.PodList
	if err := a.reader.List(ctx, &controlPods, client.InNamespace(c.SecretNamespace)); err != nil || len(controlPods.Items) != 0 {
		return errors.New("process-test control Namespace contains Pods or cannot be listed")
	}
	return nil
}

// The child starts from one fresh Initializing fixture. The wrapper accepts
// precisely the expected write order, then blocks at one selected edge. The
// parent never releases the block: it kills and joins this exact child. A
// before marker means no request was sent; an after marker requires a complete
// successful API response with the original object identity and an RV.
type processPauseCore struct {
	admissiongenesis.CoreAPI
	receipt admissioncontract.Receipt
	stage   string
	point   string
	next    int
}

func (p *processPauseCore) begin(ctx context.Context, stage string) error {
	if p.next >= len(processStages) || stage != processStages[p.next] {
		return errors.New("runtime bootstrap write order changed")
	}
	if p.stage == stage && p.point == "before" {
		return p.pause(ctx)
	}
	return nil
}

func (p *processPauseCore) finish(ctx context.Context, stage string, object *corev1.ConfigMap) error {
	if object == nil || object.UID == "" || object.ResourceVersion == "" ||
		object.Namespace != p.receipt.Namespace {
		return errors.New("bootstrap write lacks a successful exact API object response")
	}
	if stage == "active-create" && object.Name != admissioncontract.ActiveLedgerName ||
		stage == "queue-create" && object.Name != admissioncontract.QueueLedgerName ||
		(stage == "active-pin" || stage == "queue-pin" || stage == "commit") &&
			(object.Name != admissioncontract.GenesisName || string(object.UID) != p.receipt.GenesisUID) {
		return errors.New("bootstrap write response has an unexpected object identity")
	}
	p.next++
	if p.stage == stage && p.point == "after" {
		return p.pause(ctx)
	}
	return nil
}

func (p *processPauseCore) pause(ctx context.Context) error {
	if _, err := fmt.Fprintf(os.Stdout, "PAUSED %s %s\n", p.stage, p.point); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (p *processPauseCore) CreateConfigMap(ctx context.Context, namespace string, request *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	stage := ""
	switch request.Name {
	case admissioncontract.ActiveLedgerName:
		stage = "active-create"
	case admissioncontract.QueueLedgerName:
		stage = "queue-create"
	default:
		return nil, errors.New("runtime attempted an unexpected ConfigMap Create")
	}
	if namespace != p.receipt.Namespace || request.Namespace != namespace {
		return nil, errors.New("runtime attempted a cross-namespace ledger Create")
	}
	if err := p.begin(ctx, stage); err != nil {
		return nil, err
	}
	object, err := p.CoreAPI.CreateConfigMap(ctx, namespace, request)
	if err != nil {
		return nil, err
	}
	if err := p.finish(ctx, stage, object); err != nil {
		return nil, err
	}
	return object, nil
}

func processPatchStage(body []byte) (string, error) {
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(body, &ops); err != nil {
		return "", errors.New("runtime Genesis Patch is not JSON")
	}
	for _, op := range ops {
		if op.Op != "replace" || op.Path != "/data/genesis.json" {
			continue
		}
		var encoded string
		var state admissioncontract.GenesisData
		if json.Unmarshal(op.Value, &encoded) != nil || json.Unmarshal([]byte(encoded), &state) != nil {
			return "", errors.New("runtime Genesis Patch has invalid state")
		}
		switch {
		case state.Phase == admissioncontract.PhaseCommitted && state.ActiveLedgerUID != "" && state.QueueLedgerUID != "":
			return "commit", nil
		case state.Phase == admissioncontract.PhaseInitializing && state.ActiveLedgerUID != "" && state.QueueLedgerUID != "":
			return "queue-pin", nil
		case state.Phase == admissioncontract.PhaseInitializing && state.ActiveLedgerUID != "" && state.QueueLedgerUID == "":
			return "active-pin", nil
		}
	}
	return "", errors.New("runtime Genesis Patch has an unexpected phase or pin")
}

func (p *processPauseCore) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if namespace != p.receipt.Namespace || name != admissioncontract.GenesisName {
		return nil, errors.New("runtime attempted an unexpected Genesis Patch")
	}
	stage, err := processPatchStage(body)
	if err != nil {
		return nil, err
	}
	if err := p.begin(ctx, stage); err != nil {
		return nil, err
	}
	object, err := p.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
	if err != nil {
		return nil, err
	}
	if _, err := p.receipt.QualifyGenesis(object); err != nil {
		return nil, errors.New("successful Genesis Patch response failed original qualification")
	}
	if err := p.finish(ctx, stage, object); err != nil {
		return nil, err
	}
	return object, nil
}

// Only the parent sets processChildEnv. The child cannot outlive its absolute
// deadline if the parent itself disappears while it is paused or in API I/O.
func TestGenesisProcessBootstrapChild(t *testing.T) {
	mode := os.Getenv(processChildEnv)
	if mode == "" {
		t.Skip("internal opt-in Genesis process-gate child")
	}
	if mode != "pause" && mode != "successor" {
		t.Fatal("invalid process-gate child mode")
	}
	if !validProcessHash(os.Getenv(processHashEnv)) {
		t.Fatal("process-gate child lacks a pinned manifest hash")
	}
	deadlineNanos, err := strconv.ParseInt(os.Getenv(processDeadlineEnv), 10, 64)
	if err != nil || deadlineNanos <= time.Now().UnixNano() || deadlineNanos > time.Now().Add(processChildLimit+time.Second).UnixNano() {
		t.Fatal("process-gate child has no bounded absolute deadline")
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, deadlineNanos))
	defer cancel()
	m, _, err := loadProcessManifest(os.Getenv(processManifestEnv), os.Getenv(processHashEnv))
	if err != nil {
		t.Fatal("process-gate child manifest changed or is invalid")
	}
	index, err := strconv.Atoi(os.Getenv(processCaseEnv))
	if err != nil || index < 0 || index >= len(m.Cases) {
		t.Fatal("process-gate child case index is invalid")
	}
	c := m.Cases[index]
	api, err := openProcessAPI(ctx, m)
	if err != nil {
		t.Fatal("process-gate child target identity failed")
	}
	raw, receipt, err := processReceipt(c)
	if err != nil {
		t.Fatal("process-gate child receipt changed")
	}
	options := genesisReceiptOptions{File: c.ReceiptFile, SecretNamespace: c.SecretNamespace,
		SecretName: c.SecretName, SecretUID: c.SecretUID}
	loaded, checkSecret, err := options.loadAndCheck(ctx, api.direct)
	if err != nil || !bytes.Equal(raw, loaded) {
		t.Fatal("process-gate child original immutable Secret changed")
	}
	var core admissiongenesis.CoreAPI = api.direct
	if mode == "pause" {
		if err := processPreflight(ctx, api, m, c); err != nil {
			t.Fatal("process-gate child fresh-install preflight failed")
		}
		core = &processPauseCore{CoreAPI: api.direct, receipt: receipt, stage: c.Stage, point: c.Point}
	}
	guard, err := prepareGenesisRuntime(ctx, processConfig(c, receipt), raw, core, api.reader, checkSecret)
	if err != nil || guard == nil || guard.Check(ctx) != nil {
		t.Fatal("process-gate child runtime bootstrap did not qualify")
	}
	if mode == "pause" {
		t.Fatal("process-gate child passed its required pause point")
	}
	_, _ = fmt.Fprintln(os.Stdout, "COMMITTED")
}

func processChildEnvironment(manifestPath, manifestHash string, index int, mode string, deadline time.Time) []string {
	var env []string
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "KOVA_GENESIS_PROCESS_") {
			env = append(env, value)
		}
	}
	return append(env,
		processManifestEnv+"="+manifestPath,
		processHashEnv+"="+manifestHash,
		processCaseEnv+"="+strconv.Itoa(index),
		processChildEnv+"="+mode,
		processDeadlineEnv+"="+strconv.FormatInt(deadline.UnixNano(), 10))
}

// The parent owns the only spawned PID. It always kills and Waits that exact
// process on a pause, timeout, malformed marker, or failure. It never tries
// to clean API state in these cases.
func runProcessChild(parent context.Context, manifestPath, manifestHash string, index int, c processCase, mode string) error {
	ctx, cancel := context.WithTimeout(parent, processChildLimit)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("process child has no absolute deadline")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestGenesisProcessBootstrapChild$")
	cmd.Env = processChildEnvironment(manifestPath, manifestHash, index, mode, deadline)
	cmd.Stderr = io.Discard // Never reflect Secret/API response content in parent logs.
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	marker := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(io.LimitReader(pipe, 256)).ReadString('\n')
		marker <- strings.TrimSpace(line)
	}()
	expected := "COMMITTED"
	if mode == "pause" {
		expected = "PAUSED " + c.Stage + " " + c.Point
	}
	var got string
	select {
	case got = <-marker:
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return errors.New("process child timed out before its exact marker; retained all API objects")
	}
	if got != expected {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return errors.New("process child exited or emitted an unexpected marker; retained all API objects")
	}
	if mode == "pause" {
		if err := cmd.Process.Kill(); err != nil {
			_ = cmd.Wait()
			return errors.New("paused process could not be killed before its deadline")
		}
		if err := cmd.Wait(); err == nil {
			return errors.New("paused process exited without the required kill")
		}
		return nil
	}
	if err := cmd.Wait(); err != nil || ctx.Err() != nil {
		return errors.New("successor process did not exit successfully before its deadline")
	}
	return nil
}

func processCompletedWrites(c processCase) int {
	for index, stage := range processStages {
		if c.Stage == stage {
			if c.Point == "after" {
				return index + 1
			}
			return index
		}
	}
	return -1
}

func processCurrentSnapshot(ctx context.Context, m processManifest, c processCase) (*processAPI, processSnapshot, error) {
	api, err := openProcessAPI(ctx, m)
	if err != nil {
		return nil, processSnapshot{}, err
	}
	raw, receipt, err := processReceipt(c)
	if err != nil {
		return nil, processSnapshot{}, err
	}
	s, err := api.snapshot(ctx, m, c, receipt, raw)
	return api, s, err
}

func processOptIn(path string, childFields ...string) (bool, error) {
	if path != "" {
		return true, nil
	}
	for _, field := range childFields {
		if field != "" {
			return false, errors.New("process gate is partially configured")
		}
	}
	return false, nil
}

func processDeleteExact(ctx context.Context, api *processAPI, namespace, kind, name, uidValue string) error {
	uid := types.UID(uidValue)
	if uid == "" || namespace == "" || name == "" {
		return errors.New("process cleanup lacks an exact object identity")
	}
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
	var err error
	switch kind {
	case "configmap":
		err = api.clientset.CoreV1().ConfigMaps(namespace).Delete(ctx, name, options)
	case "secret":
		err = api.clientset.CoreV1().Secrets(namespace).Delete(ctx, name, options)
	default:
		return errors.New("process cleanup has an unsupported object kind")
	}
	if err != nil {
		return errors.New("UID-preconditioned process fixture cleanup failed; inspect retained objects")
	}
	switch kind {
	case "configmap":
		_, err = api.direct.GetConfigMap(ctx, namespace, name)
	case "secret":
		_, err = api.direct.GetSecret(ctx, namespace, name)
	}
	if !apierrors.IsNotFound(err) {
		return errors.New("process fixture cleanup lacks a direct NotFound confirmation")
	}
	return nil
}

// This is deliberately a ten-fixture, opt-in gate: one new Namespace per
// uncertain process-loss boundary. It does not infer freshness from an empty
// List; the external create-only installer and stopped-writer attestation are
// preconditions. The List only vetoes visible old work.
func TestRealAPIGenesisProcessBootstrap(t *testing.T) {
	path := os.Getenv(processManifestEnv)
	enabled, err := processOptIn(path, os.Getenv(processChildEnv), os.Getenv(processHashEnv),
		os.Getenv(processCaseEnv), os.Getenv(processDeadlineEnv))
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Skip("set KOVA_GENESIS_PROCESS_API_MANIFEST to a private absolute ten-case Kind fixture manifest")
	}
	if testDeadline, ok := t.Deadline(); ok && time.Until(testDeadline) < processTotalLimit+time.Minute {
		t.Fatal("process gate requires go test -timeout=15m or longer so every spawned child can be joined")
	}
	m, hash, err := loadProcessManifest(path, "")
	if err != nil {
		t.Fatalf("process fixture manifest refused before effects: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), processTotalLimit)
	defer cancel()
	api, err := openProcessAPI(ctx, m)
	if err != nil {
		t.Fatalf("process fixture target refused before effects: %v", err)
	}
	// All ten fixtures are checked before the first write. A partial matrix,
	// duplicated UID, replaced receipt, visible CR/Pod, or pre-existing ledger
	// stops this gate without adopting any state.
	for _, c := range m.Cases {
		if err := processPreflight(ctx, api, m, c); err != nil {
			t.Fatalf("process fixture %s/%s refused before effects: %v", c.Stage, c.Point, err)
		}
	}
	finals := make([]processSnapshot, len(m.Cases))
	for index, c := range m.Cases {
		if _, _, err := loadProcessManifest(path, hash); err != nil {
			t.Fatalf("process manifest changed before %s/%s; retained all objects", c.Stage, c.Point)
		}
		t.Logf("starting runtime bootstrap process-loss case %s/%s", c.Stage, c.Point)
		if err := runProcessChild(ctx, path, hash, index, c, "pause"); err != nil {
			t.Fatalf("process pause %s/%s failed: %v", c.Stage, c.Point, err)
		}
		_, partial, err := processCurrentSnapshot(ctx, m, c)
		if err != nil {
			t.Fatalf("process pause %s/%s direct readback failed; retained all objects: %v", c.Stage, c.Point, err)
		}
		if err := assertProcessSnapshot(partial, c, processCompletedWrites(c)); err != nil {
			t.Fatalf("process pause %s/%s persisted unexpected state; retained all objects: %v", c.Stage, c.Point, err)
		}
		if err := runProcessChild(ctx, path, hash, index, c, "successor"); err != nil {
			t.Fatalf("process successor %s/%s failed; retained all objects: %v", c.Stage, c.Point, err)
		}
		_, final, err := processCurrentSnapshot(ctx, m, c)
		if err != nil {
			t.Fatalf("process successor %s/%s direct readback failed; retained all objects: %v", c.Stage, c.Point, err)
		}
		if err := assertProcessSnapshot(final, c, len(processStages)); err != nil ||
			(partial.ActiveUID != "" && final.ActiveUID != partial.ActiveUID) ||
			(partial.QueueUID != "" && final.QueueUID != partial.QueueUID) {
			t.Fatalf("process successor %s/%s changed original UIDs or failed commit; retained all objects", c.Stage, c.Point)
		}
		finals[index] = final
		t.Logf("PASS runtime bootstrap process loss at %s/%s, original Namespace UID=%s Genesis UID=%s active UID=%s queue UID=%s",
			c.Stage, c.Point, c.NamespaceUID, c.GenesisUID, final.ActiveUID, final.QueueUID)
	}
	if !*m.CleanupOnSuccess {
		t.Log("all ten committed installations retained for exact-UID operator inspection; no Service or runner was started")
		return
	}
	if _, _, err := loadProcessManifest(path, hash); err != nil {
		t.Fatal("process manifest changed before cleanup; retained all objects")
	}
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) < 2*time.Minute {
		t.Fatal("process cleanup lacks a two-minute bounded window; retained all objects")
	}
	// Cleanup is explicitly opted into, and is reached only after every child
	// has been joined and every effect and original UID has direct readback.
	// Recheck the complete matrix before the first UID-preconditioned DELETE.
	for index, c := range m.Cases {
		_, current, err := processCurrentSnapshot(ctx, m, c)
		if err != nil || assertProcessSnapshot(current, c, len(processStages)) != nil ||
			current.ActiveUID != finals[index].ActiveUID || current.QueueUID != finals[index].QueueUID {
			t.Fatalf("process cleanup refused changed fixture %s/%s; retained objects", c.Stage, c.Point)
		}
	}
	for index, c := range m.Cases {
		for _, object := range []struct{ kind, namespace, name, uid string }{
			{"configmap", c.Namespace, admissioncontract.ActiveLedgerName, finals[index].ActiveUID},
			{"configmap", c.Namespace, admissioncontract.QueueLedgerName, finals[index].QueueUID},
			{"configmap", c.Namespace, admissioncontract.GenesisName, c.GenesisUID},
			{"secret", c.SecretNamespace, c.SecretName, c.SecretUID},
		} {
			if err := processDeleteExact(ctx, api, object.namespace, object.kind, object.name, object.uid); err != nil {
				t.Fatalf("process cleanup stopped at %s/%s: %v", c.Stage, c.Point, err)
			}
		}
	}
	t.Log("UID-preconditioned cleanup confirmed; all ten runner Namespaces remain for inspection")
}

func validProcessFixtureForUnit() processManifest {
	cleanup := false
	m := processManifest{Kubeconfig: "/tmp/kova-process-test.kubeconfig", KubeconfigSHA256: strings.Repeat("a", 64),
		Context: "kind-kova-genesis-process-unit", KubeSystemUID: "system-unit", ControlPlaneID: strings.Repeat("b", 64),
		CleanupOnSuccess: &cleanup}
	for i, stage := range processStages {
		for j, point := range processPoints {
			id := strconv.Itoa(i*len(processPoints) + j)
			m.Cases = append(m.Cases, processCase{Stage: stage, Point: point,
				Namespace: "kova-genesis-process-" + id, NamespaceUID: "namespace-uid-" + id,
				GenesisUID: "genesis-uid-" + id, ReceiptFile: "/tmp/receipt-" + id + ".json",
				ReceiptSHA256: strings.Repeat("c", 64), SecretNamespace: "kova-genesis-process-control", SecretNamespaceUID: "control-uid",
				SecretName: "receipt-" + id, SecretUID: "secret-uid-" + id})
		}
	}
	return m
}

func TestGenesisProcessManifestRejectsPartialAndReusedFixtures(t *testing.T) {
	base := validProcessFixtureForUnit()
	if err := validateProcessManifest(base); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*processManifest){
		"partial matrix":          func(m *processManifest) { m.Cases = m.Cases[:9] },
		"missing cleanup choice":  func(m *processManifest) { m.CleanupOnSuccess = nil },
		"missing Secret UID":      func(m *processManifest) { m.Cases[2].SecretUID = "" },
		"missing control UID":     func(m *processManifest) { m.Cases[2].SecretNamespaceUID = "" },
		"duplicate Namespace":     func(m *processManifest) { m.Cases[2].Namespace = m.Cases[1].Namespace },
		"duplicate Namespace UID": func(m *processManifest) { m.Cases[2].NamespaceUID = m.Cases[1].NamespaceUID },
		"duplicate Genesis UID":   func(m *processManifest) { m.Cases[2].GenesisUID = m.Cases[1].GenesisUID },
		"duplicate Secret UID":    func(m *processManifest) { m.Cases[2].SecretUID = m.Cases[1].SecretUID },
		"non-dedicated context":   func(m *processManifest) { m.Context = "kind-default" },
		"relative receipt":        func(m *processManifest) { m.Cases[2].ReceiptFile = "receipt.json" },
		"shared control NS":       func(m *processManifest) { m.Cases[2].SecretNamespace = "default" },
		"duplicate stage": func(m *processManifest) {
			m.Cases[2].Stage, m.Cases[2].Point = m.Cases[1].Stage, m.Cases[1].Point
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := base
			m.Cases = append([]processCase(nil), base.Cases...)
			change(&m)
			if err := validateProcessManifest(m); err == nil {
				t.Fatal("unsafe process fixture passed validation")
			}
		})
	}
}

func TestGenesisProcessPartialOptInAndStrictJSON(t *testing.T) {
	if enabled, err := processOptIn("", ""); enabled || err != nil {
		t.Fatal("empty opt-in was not an inert skip")
	}
	if _, err := processOptIn("", "orphan-child-marker"); err == nil {
		t.Fatal("partial process-gate environment was accepted")
	}
	if enabled, err := processOptIn("/private/fixture.json"); !enabled || err != nil {
		t.Fatal("complete process-gate opt-in was rejected")
	}
	var parsed processManifest
	if err := admissionjson.Decode([]byte(`{"context":"one","context":"two"}`), &parsed, processManifestField); err == nil {
		t.Fatal("duplicate manifest key was accepted")
	}
}

func TestGenesisProcessPinnedReceiptRejectsChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("original"))
	hash := hex.EncodeToString(sum[:])
	if _, err := readPinnedProcessFile(path, hash, 1024); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed!"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPinnedProcessFile(path, hash, 1024); err == nil {
		t.Fatal("changed receipt passed its pinned hash")
	}
}
