package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

var errServiceResponseDropped = errors.New("test-owned API response deliberately dropped")

// The proxy lives in the parent, not the process that is killed. A held
// request is never forwarded on cancellation; only forwardHeld owns that
// one bounded attempt. All unapproved effects are denied and fail the gate.
type serviceFaultProxy struct {
	server        *httptest.Server
	transport     http.RoundTripper
	target        *url.URL
	token         string
	receipt       admissioncontract.Receipt
	control       string
	secret        string
	mode          string
	mu            sync.Mutex
	held          *http.Request
	heldBody      []byte
	heldHash      string
	heldForwarded bool
	faulted       bool
	violations    int
	attempts      map[string]int
	holders       map[string]bool
	fired         chan struct{}
}

func serviceProxyToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func newServiceFaultProxy(cfg *rest.Config, c serviceProcessCase, receipt admissioncontract.Receipt, mode string) (*serviceFaultProxy, error) {
	token, err := serviceProxyToken()
	if err != nil {
		return nil, err
	}
	upstream := rest.CopyConfig(cfg)
	upstream.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	transport, err := rest.TransportFor(upstream)
	if err != nil {
		return nil, err
	}
	target, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, err
	}
	p := &serviceFaultProxy{transport: transport, target: target, token: token, receipt: receipt, control: c.Fixture.SecretNamespace,
		secret: c.Fixture.SecretName, mode: mode, attempts: map[string]int{}, holders: map[string]bool{}, fired: make(chan struct{})}
	reverse := httputil.NewSingleHostReverseProxy(target)
	reverse.Transport = p
	reverse.FlushInterval = -1
	reverse.ErrorLog = log.New(io.Discard, "", 0)
	reverse.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, errServiceResponseDropped) {
			// A real connection close, not a synthetic 5xx, forces the Service
			// to classify the already-persisted mutation response as unknown.
			conn, _, hijackErr := http.NewResponseController(w).Hijack()
			if hijackErr == nil {
				_ = conn.Close()
				return
			}
		}
		http.Error(w, "bounded test proxy refused request", http.StatusBadGateway)
	}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+p.token || r.Header.Get("Upgrade") != "" {
			p.violation()
			http.Error(w, "test capability refused", http.StatusForbidden)
			return
		}
		r.Header.Del("Authorization")
		reverse.ServeHTTP(w, r)
	}))
	return p, nil
}

func (p *serviceFaultProxy) violation() { p.mu.Lock(); p.violations++; p.mu.Unlock() }

func (p *serviceFaultProxy) Close() { p.server.CloseClientConnections(); p.server.Close() }

func (p *serviceFaultProxy) requestStage(req *http.Request, body []byte) (string, bool) {
	path := req.URL.Path
	if req.URL.RawPath != "" || strings.Contains(path, "//") || strings.Contains(path, "..") || strings.Contains(path, "%") {
		return "", false
	}
	runner := "/api/v1/namespaces/" + p.receipt.Namespace
	control := "/api/v1/namespaces/" + p.control
	receipts := "/api/v1/namespaces/" + p.receipt.Contract.ReceiptNamespace
	crs := "/apis/kova.cofy.dev/v1alpha1/namespaces/" + p.receipt.Namespace + "/kovabuilds"
	lease := "/apis/coordination.k8s.io/v1/namespaces/" + p.control + "/leases"
	if req.Method == http.MethodGet {
		// Discovery is read-only. Namespaced reads stay in the explicitly
		// pinned fixture, including the manager's Pod List/Watch.
		if path == "/api" || path == "/api/v1" || path == "/apis" || path == "/apis/kova.cofy.dev" ||
			path == "/apis/kova.cofy.dev/v1alpha1" || path == "/apis/coordination.k8s.io" || path == "/apis/coordination.k8s.io/v1" ||
			path == "/version" || path == runner || path == control || path == receipts || path == control+"/secrets/"+p.secret ||
			(path == runner+"/configmaps" && req.URL.Query().Get("fieldSelector") == "metadata.name="+admissioncontract.ActiveLedgerName) || path == runner+"/configmaps/"+admissioncontract.GenesisName ||
			path == runner+"/configmaps/"+admissioncontract.ActiveLedgerName || path == runner+"/configmaps/"+admissioncontract.QueueLedgerName ||
			path == runner+"/pods" || path == crs || path == lease+"/kova-service.kova.cofy.dev" {
			return "read", true
		}
		return "", false
	}
	if (req.Method == http.MethodPost && path == lease) || (req.Method == http.MethodPut && path == lease+"/kova-service.kova.cofy.dev") {
		var object coordinationv1.Lease
		if json.Unmarshal(body, &object) != nil || object.Namespace != p.control || object.Name != "kova-service.kova.cofy.dev" || object.Spec.HolderIdentity == nil || *object.Spec.HolderIdentity == "" {
			return "", false
		}
		return "lease", true
	}
	if req.Method == http.MethodPost && path == runner+"/configmaps" {
		var object corev1.ConfigMap
		if json.Unmarshal(body, &object) != nil || object.Namespace != p.receipt.Namespace || object.UID != "" || object.ResourceVersion != "" || len(object.Data) != 1 || len(object.BinaryData) != 0 {
			return "", false
		}
		// Admission identity qualification expects a persisted UID/RV. Only
		// the validation copy gets placeholders; original wire bytes stay exact.
		object.UID, object.ResourceVersion = "test-create-validation", "test-create-validation"
		cfg := processConfig(processCase{Namespace: p.receipt.Namespace}, p.receipt)
		for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
			if p.receipt.QualifyLedgerIdentity(&object, role) == nil {
				if role == admissioncontract.Active {
					empty, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
					return "active-create", err == nil && object.Data[admissioncontract.ActiveLedgerDataKey] == empty
				}
				empty, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
				return "queue-create", err == nil && object.Data[admissioncontract.QueueLedgerDataKey] == empty
			}
		}
		return "", false
	}
	if req.Method == http.MethodPatch && path == runner+"/configmaps/"+admissioncontract.GenesisName && req.Header.Get("Content-Type") == "application/json-patch+json" {
		return p.genesisPatchStage(body)
	}
	// Leader election emits only small Events for this exact Lease. Events
	// are not execution authority, but unknown destinations remain forbidden.
	if req.Method == http.MethodPost && path == control+"/events" {
		var event corev1.Event
		if json.Unmarshal(body, &event) == nil && event.Namespace == p.control && event.InvolvedObject.Kind == "Lease" &&
			event.InvolvedObject.Namespace == p.control && event.InvolvedObject.Name == "kova-service.kova.cofy.dev" {
			return "event", true
		}
	}
	return "", false
}

func (p *serviceFaultProxy) genesisPatchStage(body []byte) (string, bool) {
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(body, &ops) != nil || len(ops) < 4 || len(ops) > 6 {
		return "", false
	}
	value := func(index int, op, path string) (string, bool) {
		var text string
		if index >= len(ops) || ops[index].Op != op || ops[index].Path != path || json.Unmarshal(ops[index].Value, &text) != nil {
			return "", false
		}
		return text, true
	}
	uid, ok := value(0, "test", "/metadata/uid")
	if !ok || uid != p.receipt.GenesisUID {
		return "", false
	}
	rv, ok := value(1, "test", "/metadata/resourceVersion")
	if !ok || rv == "" {
		return "", false
	}
	oldData, ok := value(2, "test", "/data/genesis.json")
	if !ok {
		return "", false
	}
	original := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: p.receipt.Namespace, Name: p.receipt.GenesisName, UID: types.UID(uid), ResourceVersion: rv}, Data: map[string]string{admissioncontract.GenesisDataKey: oldData}}
	old, err := p.receipt.QualifyGenesis(original)
	if err != nil || old.Phase != admissioncontract.PhaseInitializing {
		return "", false
	}
	nextIndex := 3
	if ops[nextIndex].Op == "test" && ops[nextIndex].Path == "/immutable" {
		if string(ops[nextIndex].Value) != "false" {
			return "", false
		}
		nextIndex++
	}
	nextData, ok := value(nextIndex, "replace", "/data/genesis.json")
	if !ok {
		return "", false
	}
	proposed := original.DeepCopy()
	proposed.Data[admissioncontract.GenesisDataKey] = nextData
	committing := len(ops) == nextIndex+2
	if committing {
		last := ops[nextIndex+1]
		if last.Op != "add" || last.Path != "/immutable" || string(last.Value) != "true" {
			return "", false
		}
		immutable := true
		proposed.Immutable = &immutable
	} else if len(ops) != nextIndex+1 {
		return "", false
	}
	next, err := p.receipt.QualifyGenesis(proposed)
	if err != nil {
		return "", false
	}
	if committing && next.Phase == admissioncontract.PhaseCommitted && next.ActiveLedgerUID == old.ActiveLedgerUID && next.QueueLedgerUID == old.QueueLedgerUID {
		return "commit", true
	}
	if !committing && next.Phase == admissioncontract.PhaseInitializing {
		if old.ActiveLedgerUID == "" && next.ActiveLedgerUID != "" && next.QueueLedgerUID == old.QueueLedgerUID {
			return "active-pin", true
		}
		if old.QueueLedgerUID == "" && next.QueueLedgerUID != "" && next.ActiveLedgerUID == old.ActiveLedgerUID {
			return "queue-pin", true
		}
	}
	return "", false
}

func (p *serviceFaultProxy) RoundTrip(req *http.Request) (*http.Response, error) {
	if p.target == nil || req.URL.Scheme != p.target.Scheme || req.URL.Host != p.target.Host {
		p.violation()
		return nil, errors.New("test proxy refused a changed upstream target")
	}
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(req.Body, 1024*1024+1))
		_ = req.Body.Close()
		if err != nil || len(body) > 1024*1024 {
			p.violation()
			return nil, errors.New("test proxy request exceeds bound")
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	stage, allowed := p.requestStage(req, body)
	if !allowed {
		p.violation()
		return nil, errors.New("test proxy denied unapproved API operation")
	}
	p.mu.Lock()
	selected := (strings.HasPrefix(p.mode, "lost-") && stage == strings.TrimPrefix(p.mode, "lost-")) || (strings.HasPrefix(p.mode, "late-") && stage == "active-create")
	if selected && p.faulted {
		p.violations++
		p.mu.Unlock()
		return nil, errors.New("fault mutation was unexpectedly retried")
	}
	if selected {
		p.faulted = true
	}
	if selected && strings.HasPrefix(p.mode, "late-") {
		p.held = req.Clone(context.Background())
		p.held.Body, p.held.GetBody = nil, nil
		p.heldBody = append([]byte(nil), body...)
		sum := sha256.Sum256(body)
		p.heldHash = hex.EncodeToString(sum[:])
		close(p.fired)
		p.mu.Unlock()
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	p.attempts[stage]++
	p.mu.Unlock()
	response, err := p.transport.RoundTrip(req)
	if err != nil {
		return response, err
	}
	if stage == "lease" && response.StatusCode >= 200 && response.StatusCode < 300 {
		var lease coordinationv1.Lease
		if json.Unmarshal(body, &lease) == nil && lease.Spec.HolderIdentity != nil {
			p.mu.Lock()
			p.holders[*lease.Spec.HolderIdentity] = true
			p.mu.Unlock()
		}
	}
	if !selected {
		return response, nil
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	_ = response.Body.Close()
	var object corev1.ConfigMap
	if readErr != nil || len(raw) > 1024*1024 || response.StatusCode < 200 || response.StatusCode >= 300 ||
		json.Unmarshal(raw, &object) != nil || object.Namespace != p.receipt.Namespace || object.UID == "" || object.ResourceVersion == "" {
		p.violation()
		return nil, errors.New("lost-response fault lacks confirmed persisted original API object")
	}
	if strings.HasSuffix(stage, "create") {
		role := admissioncontract.Active
		if stage == "queue-create" {
			role = admissioncontract.Queue
		}
		if p.receipt.QualifyLedgerIdentity(&object, role) != nil {
			p.violation()
			return nil, errors.New("lost Create response identity mismatch")
		}
	} else if _, err := p.receipt.QualifyGenesis(&object); err != nil {
		p.violation()
		return nil, errors.New("lost Genesis response identity mismatch")
	}
	close(p.fired)
	return nil, errServiceResponseDropped
}

func (p *serviceFaultProxy) forwardHeld(ctx context.Context) (int, string, error) {
	p.mu.Lock()
	if p.held == nil || p.heldForwarded || p.violations != 0 {
		p.mu.Unlock()
		return 0, "", errors.New("late Create has no unique unforwarded original request")
	}
	p.heldForwarded = true
	request := p.held.Clone(ctx)
	body := append([]byte(nil), p.heldBody...)
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != p.heldHash {
		p.mu.Unlock()
		return 0, "", errors.New("held Create bytes changed")
	}
	p.attempts["held-active-create"]++
	p.mu.Unlock()
	request.Body = io.NopCloser(bytes.NewReader(body))
	response, err := p.transport.RoundTrip(request)
	if err != nil {
		return 0, "", errors.New("late Create result unknown; retained all API evidence")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return 0, "", errors.New("late Create response unknown or oversized")
	}
	if response.StatusCode == http.StatusConflict {
		return response.StatusCode, "", nil
	}
	var object corev1.ConfigMap
	var requested corev1.ConfigMap
	if response.StatusCode != http.StatusCreated || json.Unmarshal(raw, &object) != nil || json.Unmarshal(body, &requested) != nil || object.UID == "" || p.receipt.QualifyLedgerIdentity(&object, admissioncontract.Active) != nil ||
		len(object.Data) != 1 || len(object.BinaryData) != 0 || object.Data[admissioncontract.ActiveLedgerDataKey] != requested.Data[admissioncontract.ActiveLedgerDataKey] {
		return 0, "", errors.New("late Create lacked an exact known response")
	}
	return response.StatusCode, string(object.UID), nil
}

func (p *serviceFaultProxy) ownsHolder(value string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.holders[value]
}

func (p *serviceFaultProxy) check() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.violations != 0 {
		return errors.New("Service attempted an unapproved effect or repeated fault write")
	}
	if strings.HasPrefix(p.mode, "lost-") && (!p.faulted || p.attempts[strings.TrimPrefix(p.mode, "lost-")] != 1) {
		return errors.New("lost response did not have exactly one upstream attempt")
	}
	if strings.HasPrefix(p.mode, "late-") && (!p.heldForwarded || p.attempts["active-create"] != 0 || p.attempts["held-active-create"] != 1) {
		return errors.New("late Create lacked exactly one parent-owned forwarding attempt")
	}
	return nil
}

func serviceLoopbackAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	return address, listener.Close()
}

func serviceHTTPStatus(ctx context.Context, address, path string) (int, error) {
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probe, http.MethodGet, "http://"+address+path, nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("Service redirected probe") }}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if n, err := io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024+1)); err != nil {
		return 0, err
	} else if n > 64*1024 {
		return 0, errors.New("Service HTTP probe body exceeds bound")
	}
	return response.StatusCode, nil
}
