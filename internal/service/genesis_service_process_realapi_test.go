package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/urfave/cli/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func serviceChildAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	number, parseErr := strconv.Atoi(port)
	return err == nil && parseErr == nil && host == "127.0.0.1" && number > 0 && number <= 65535
}

func serviceChildArgs(c serviceProcessCase, receipt admissioncontract.Receipt, address string) []string {
	limits := receipt.Contract.Limits
	return []string{"kova-controller", "service", "--listen=" + address, "--namespace=" + c.Fixture.Namespace,
		"--leader-election-namespace=" + c.Fixture.SecretNamespace, "--leader-elect=true", "--metrics-bind-address=0",
		"--auth-mode=unsafe-none", "--runner-image=" + receipt.Contract.RunnerImage, "--runner-image-pull-policy=Never",
		"--worker-pool-id=" + receipt.Contract.WorkerPoolID, "--recovery-receipt-namespace=" + receipt.Contract.ReceiptNamespace,
		"--runner-node-selector=kova-test-never=true", "--buildkit-platform-addr=linux/amd64=tcp://127.0.0.1:1",
		"--max-active-jobs=" + strconv.Itoa(limits.MaxActiveJobs), "--max-active-jobs-per-requester=" + strconv.Itoa(limits.MaxActiveJobsPerRequester),
		"--max-queued-jobs=" + strconv.Itoa(limits.MaxQueuedJobs), "--max-queued-jobs-per-requester=" + strconv.Itoa(limits.MaxQueuedJobsPerRequester),
		"--worker-slots=" + strconv.Itoa(limits.WorkerSlots), "--admission-genesis-receipt-file=" + c.Fixture.ReceiptFile,
		"--admission-genesis-receipt-secret-namespace=" + c.Fixture.SecretNamespace, "--admission-genesis-receipt-secret-name=" + c.Fixture.SecretName,
		"--admission-genesis-receipt-secret-uid=" + c.Fixture.SecretUID}
}

// This child runs the complete actual Service Action, not a mocked manager
// or a standalone Bootstrapper. It never receives a source/build submission.
func TestGenesisActualServiceChild(t *testing.T) {
	indexText := os.Getenv(serviceProcessChildEnv)
	if indexText == "" {
		t.Skip("internal explicitly source-bound Service gate child")
	}
	deadlineNanos, err := strconv.ParseInt(os.Getenv(serviceProcessDeadlineEnv), 10, 64)
	deadline := time.Unix(0, deadlineNanos)
	if err != nil || time.Until(deadline) <= 0 || time.Until(deadline) > serviceProcessChildLimit {
		t.Fatal("Service child lacks a bounded absolute deadline")
	}
	// The child cannot outlive its deadline even if a future component ignores
	// context cancellation, or its parent disappears while holding a request.
	hardStop := time.AfterFunc(time.Until(deadline), func() { os.Exit(124) })
	defer hardStop.Stop()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if !validProcessHash(os.Getenv(serviceProcessHashEnv)) {
		t.Fatal("Service child lacks pinned manifest hash")
	}
	m, _, err := loadServiceProcessManifest(os.Getenv(serviceProcessManifestEnv), os.Getenv(serviceProcessHashEnv))
	if err != nil || checkServiceProcessExecutable(m) != nil {
		t.Fatal("Service child source/binary/manifest pin failed")
	}
	index, err := strconv.Atoi(indexText)
	if err != nil || index < 0 || index >= len(m.Cases) {
		t.Fatal("Service child case index invalid")
	}
	c := m.Cases[index]
	_, _, err = openServiceProcessConfig(ctx, m)
	if err != nil {
		t.Fatal("Service child original Kind target changed")
	}
	_, receipt, err := processReceipt(c.Fixture)
	if err != nil {
		t.Fatal("Service child receipt changed")
	}
	proxy, err := url.Parse(os.Getenv(serviceProcessProxyEnv))
	address := os.Getenv(serviceProcessListenEnv)
	token := os.Getenv(serviceProcessTokenEnv)
	if err != nil || proxy.Scheme != "http" || !serviceChildAddress(proxy.Host) || proxy.Path != "" || proxy.RawQuery != "" || proxy.User != nil || proxy.Fragment != "" ||
		!serviceChildAddress(address) || proxy.Host == address || !validProcessHash(token) {
		t.Fatal("Service child loopback endpoints/capability invalid")
	}
	cfg := &rest.Config{Host: proxy.String(), BearerToken: token, ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"},
		Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }}
	app := &cli.App{Writer: io.Discard, ErrWriter: io.Discard, Commands: []*cli.Command{serviceCLICommand(func() (*rest.Config, error) { return rest.CopyConfig(cfg), nil })}}
	_, _ = fmt.Fprintf(os.Stdout, "SERVICE_VALIDATED %d %d\n", index, os.Getpid())
	if err := app.RunContext(ctx, serviceChildArgs(c, receipt, address)); err != nil {
		// In the shared Action, ErrChanged is returned only by Genesis checks
		// before startServiceComponents starts the manager/listener. Its final
		// guard also runs before either starts; asynchronous HTTP errors do not
		// escape Action as ErrChanged. Do not turn an arbitrary child exit into
		// evidence of an original-identity startup refusal.
		if errors.Is(err, admissiongenesis.ErrChanged) {
			_, _ = fmt.Fprintf(os.Stdout, "SERVICE_REFUSED_CHANGED %d %d\n", index, os.Getpid())
		}
		t.Fatal("actual Service Action refused startup or stopped")
	}
}

type ownedServiceChild struct {
	command *exec.Cmd
	done    chan error
	refusal chan string
	address string
	joined  bool
}

func startOwnedServiceChild(ctx context.Context, path, hash string, index int, proxy *serviceFaultProxy) (*ownedServiceChild, error) {
	address, err := serviceLoopbackAddress()
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(serviceProcessChildLimit - time.Second)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestGenesisActualServiceChild$", "-test.timeout=4m")
	cmd.WaitDelay = 5 * time.Second
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "KOVA_GENESIS_SERVICE_") && !strings.HasPrefix(value, "KOVA_SERVICE_") &&
			!strings.HasPrefix(value, "KOVA_RUNNER_") && !strings.HasPrefix(value, "KOVA_OTEL_") && !strings.HasPrefix(value, "OTEL_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, serviceProcessManifestEnv+"="+path, serviceProcessHashEnv+"="+hash,
		serviceProcessChildEnv+"="+strconv.Itoa(index), serviceProcessDeadlineEnv+"="+strconv.FormatInt(deadline.UnixNano(), 10),
		serviceProcessProxyEnv+"="+proxy.server.URL, serviceProcessTokenEnv+"="+proxy.token, serviceProcessListenEnv+"="+address)
	cmd.Stderr = io.Discard
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	child := &ownedServiceChild{command: cmd, done: make(chan error, 1), refusal: make(chan string, 1), address: address}
	marker := make(chan string, 1)
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		reader := bufio.NewReaderSize(pipe, 256)
		line, readErr := reader.ReadSlice('\n')
		if readErr == nil && len(line) <= 256 {
			marker <- strings.TrimSpace(string(line))
		} else {
			marker <- ""
		}
		for {
			line, readErr := reader.ReadSlice('\n')
			if readErr == nil && strings.HasPrefix(string(line), "SERVICE_REFUSED_CHANGED ") {
				select {
				case child.refusal <- strings.TrimSpace(string(line)):
				default:
				}
			}
			if readErr != nil && readErr != bufio.ErrBufferFull {
				return
			}
		}
	}()
	// Finish reading the bounded child protocol before Wait closes StdoutPipe.
	go func() { <-outputDone; child.done <- cmd.Wait() }()
	select {
	case line := <-marker:
		if line == fmt.Sprintf("SERVICE_VALIDATED %d %d", index, cmd.Process.Pid) {
			return child, nil
		}
	case <-ctx.Done():
	}
	if err := child.killAndJoin(); err != nil {
		return child, err
	}
	return child, errors.New("Service child failed exact source-validated PID marker; retained API state")
}

func (c *ownedServiceChild) killAndJoin() error {
	if c.joined {
		return nil
	}
	killErr := c.command.Process.Kill()
	select {
	case waitErr := <-c.done:
		c.joined = true
		if killErr != nil || waitErr == nil {
			return errors.New("Service child did not undergo the required owned PID kill")
		}
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("Service child was killed but could not be joined; retain all API evidence")
	}
}

func waitServiceCondition(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		done, err := check()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("bounded Service condition deadline exceeded")
		case <-ticker.C:
		}
	}
}

func waitServiceReady(ctx context.Context, child *ownedServiceChild, expected int) error {
	return waitServiceCondition(ctx, func() (bool, error) {
		select {
		case <-child.done:
			child.joined = true
			return false, errors.New("Service child exited before expected readiness")
		default:
		}
		code, err := serviceHTTPStatus(ctx, child.address, "/readyz")
		return err == nil && code == expected, nil
	})
}

func getServiceLease(ctx context.Context, api *processAPI, c serviceProcessCase) (*coordinationv1.Lease, error) {
	read, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return api.clientset.CoordinationV1().Leases(c.Fixture.SecretNamespace).Get(read, "kova-service.kova.cofy.dev", metav1.GetOptions{})
}

func waitServiceLeader(ctx context.Context, api *processAPI, c serviceProcessCase, proxies []*serviceFaultProxy, previousUID, previousHolder string) (*coordinationv1.Lease, int, error) {
	var result *coordinationv1.Lease
	owner := -1
	err := waitServiceCondition(ctx, func() (bool, error) {
		lease, err := getServiceLease(ctx, api, c)
		if apierrors.IsNotFound(err) && previousUID == "" {
			return false, nil
		}
		if err != nil {
			return false, errors.New("Service Lease direct read failed")
		}
		if lease.UID == "" || lease.DeletionTimestamp != nil || (previousUID != "" && string(lease.UID) != previousUID) {
			return false, errors.New("original Service Lease UID changed")
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" || *lease.Spec.HolderIdentity == previousHolder {
			return false, nil
		}
		for index, proxy := range proxies {
			if proxy.ownsHolder(*lease.Spec.HolderIdentity) {
				if owner != -1 && owner != index {
					return false, errors.New("Service holder aliases multiple owned processes")
				}
				owner = index
			}
		}
		if owner == -1 {
			return false, nil
		}
		result = lease
		return true, nil
	})
	return result, owner, err
}

func serviceEmptyWorkloads(ctx context.Context, api *processAPI, c serviceProcessCase) error {
	for _, namespace := range []string{c.Fixture.Namespace, c.Fixture.SecretNamespace} {
		var pods corev1.PodList
		var builds kovav1.KovaBuildList
		if err := api.reader.List(ctx, &pods, client.InNamespace(namespace), client.Limit(1)); err != nil || len(pods.Items) != 0 || pods.Continue != "" {
			return errors.New("Service gate saw a Pod or incomplete inventory")
		}
		if err := api.reader.List(ctx, &builds, client.InNamespace(namespace), client.Limit(1)); err != nil || len(builds.Items) != 0 || builds.Continue != "" {
			return errors.New("Service gate saw a KovaBuild or incomplete inventory")
		}
	}
	return nil
}

func serviceOriginalInstallation(ctx context.Context, api *processAPI, m serviceProcessManifest, c serviceProcessCase) error {
	if err := api.checkSystem(ctx, m.target()); err != nil {
		return err
	}
	raw, receipt, err := processReceipt(c.Fixture)
	if err != nil {
		return err
	}
	ns, err := api.direct.GetNamespace(ctx, c.Fixture.Namespace)
	if err != nil || receipt.QualifyNamespace(ns) != nil {
		return errors.New("original Service runner Namespace changed")
	}
	control, err := api.direct.GetNamespace(ctx, c.Fixture.SecretNamespace)
	if err != nil || string(control.UID) != c.Fixture.SecretNamespaceUID || control.Status.Phase != corev1.NamespaceActive || control.DeletionTimestamp != nil {
		return errors.New("original Service control Namespace changed")
	}
	options := genesisReceiptOptions{SecretNamespace: c.Fixture.SecretNamespace, SecretName: c.Fixture.SecretName, SecretUID: c.Fixture.SecretUID}
	return options.checkRaw(raw, api.direct)(ctx)
}

func serviceCommittedSnapshot(ctx context.Context, api *processAPI, m serviceProcessManifest, c serviceProcessCase) (processSnapshot, error) {
	raw, receipt, err := processReceipt(c.Fixture)
	if err != nil {
		return processSnapshot{}, err
	}
	snapshot, err := api.snapshot(ctx, m.target(), c.Fixture, receipt, raw)
	if err == nil {
		err = assertProcessSnapshot(snapshot, c.Fixture, len(processStages))
	}
	return snapshot, err
}

func runActualServiceCase(ctx context.Context, t *testing.T, path, hash string, m serviceProcessManifest, index int, cfg *rest.Config, api *processAPI) error {
	c := m.Cases[index]
	_, receipt, err := processReceipt(c.Fixture)
	if err != nil {
		return err
	}
	var proxies []*serviceFaultProxy
	var children []*ownedServiceChild
	var refusedProxy *serviceFaultProxy
	stopped := false
	stopOwned := func() error {
		if stopped {
			return nil
		}
		var firstError error
		for _, child := range children {
			if err := child.killAndJoin(); err != nil && firstError == nil {
				firstError = err
			}
		}
		for _, proxy := range proxies {
			proxy.Close()
		}
		stopped = true
		return firstError
	}
	// Children are always killed/joined before closing their proxies. Closing a
	// held proxy never releases its captured mutation or cleans any API state.
	defer func() {
		if err := stopOwned(); err != nil {
			t.Errorf("case %s: %v", c.Name, err)
		}
	}()
	spawn := func(mode string) (*ownedServiceChild, *serviceFaultProxy, error) {
		proxy, err := newServiceFaultProxy(cfg, c, receipt, mode)
		if err != nil {
			return nil, nil, err
		}
		proxies = append(proxies, proxy)
		child, err := startOwnedServiceChild(ctx, path, hash, index, proxy)
		if child != nil {
			children = append(children, child)
		}
		if err != nil {
			return nil, nil, err
		}
		return child, proxy, nil
	}
	first, fault, err := spawn(c.Name)
	if err != nil {
		return err
	}
	select {
	case <-fault.fired:
	case <-first.done:
		first.joined = true
		return errors.New("Service exited before the expected exact fault")
	case <-ctx.Done():
		return errors.New("Service did not reach its exact proxy fault")
	}
	late := strings.HasPrefix(c.Name, "late-")
	if late {
		if err := first.killAndJoin(); err != nil {
			return err
		}
		first, _, err = spawn("")
		if err != nil {
			return err
		}
	}
	if err := waitServiceReady(ctx, first, http.StatusOK); err != nil {
		return err
	}
	before, err := serviceCommittedSnapshot(ctx, api, m, c)
	if err != nil {
		return err
	}
	second, _, err := spawn("")
	if err != nil {
		return err
	}
	if err := waitServiceReady(ctx, second, http.StatusOK); err != nil {
		return err
	}
	// Only read-only HTTP routes are used; no fake authorization or submitted
	// source/build is hidden behind the Pod/Exec safety veto.
	for _, child := range []*ownedServiceChild{first, second} {
		if code, err := serviceHTTPStatus(ctx, child.address, "/v1/builds"); err != nil || code != http.StatusOK {
			return errors.New("actual Service empty HTTP read failed")
		}
	}
	lease, owner, err := waitServiceLeader(ctx, api, c, proxies, "", "")
	if err != nil {
		return err
	}
	if owner >= len(children) || children[owner].joined {
		return errors.New("Lease holder is not a live owned Service PID")
	}
	if err := children[owner].killAndJoin(); err != nil {
		return err
	}
	nextLease, nextOwner, err := waitServiceLeader(ctx, api, c, proxies, string(lease.UID), *lease.Spec.HolderIdentity)
	if err != nil || nextOwner == owner || nextOwner < 0 || children[nextOwner].joined {
		return errors.New("original Lease did not hand off to the surviving exact Service process")
	}
	survivor := children[nextOwner]
	if err := waitServiceReady(ctx, survivor, http.StatusOK); err != nil {
		return err
	}
	if late {
		if *c.AllowCommittedLedgerDeletion {
			// This is the one manifest-authorized destructive effect. It targets
			// only this case's newly observed committed UID, never an old fixture.
			current, err := serviceCommittedSnapshot(ctx, api, m, c)
			if err != nil || current.ActiveUID != before.ActiveUID || current.QueueUID != before.QueueUID {
				return errors.New("Service committed identities changed before exact fault deletion")
			}
			if err := processDeleteExact(ctx, api, c.Fixture.Namespace, "configmap", admissioncontract.ActiveLedgerName, before.ActiveUID); err != nil {
				return err
			}
		}
		forward, cancel := context.WithTimeout(ctx, 10*time.Second)
		code, newUID, err := fault.forwardHeld(forward)
		cancel()
		if err != nil {
			return err
		}
		if *c.AllowCommittedLedgerDeletion {
			if code != http.StatusCreated || newUID == "" || newUID == before.ActiveUID {
				return errors.New("late Create did not persist a distinct replacement UID after explicit committed loss")
			}
			if err := waitServiceReady(ctx, survivor, http.StatusServiceUnavailable); err != nil {
				return err
			}
			original, err := api.direct.GetConfigMap(ctx, c.Fixture.Namespace, admissioncontract.GenesisName)
			if err != nil {
				return err
			}
			state, err := receipt.QualifyGenesis(original)
			if err != nil || state != before.State || original.Immutable == nil || !*original.Immutable {
				return errors.New("committed Genesis was rebound after late ledger recreation")
			}
			restarted, restartProxy, err := spawn("")
			if err != nil {
				return err
			}
			refusedProxy = restartProxy
			select {
			case exitErr := <-restarted.done:
				restarted.joined = true
				if exitErr == nil {
					return errors.New("restarted Service did not refuse committed UID loss")
				}
				select {
				case marker := <-restarted.refusal:
					if marker != fmt.Sprintf("SERVICE_REFUSED_CHANGED %d %d", index, restarted.command.Process.Pid) {
						return errors.New("Service startup refusal marker identity mismatch")
					}
				default:
					return errors.New("Service restart exit did not prove a typed pre-component original-identity refusal")
				}
			case <-ctx.Done():
				return errors.New("restarted Service did not stop within committed-loss bound")
			}
			if _, err := serviceHTTPStatus(ctx, restarted.address, "/readyz"); err == nil {
				return errors.New("refused Service startup exposed an HTTP listener")
			}
			replacement, err := api.direct.GetConfigMap(ctx, c.Fixture.Namespace, admissioncontract.ActiveLedgerName)
			if err != nil || string(replacement.UID) != newUID || receipt.QualifyLedgerIdentity(replacement, admissioncontract.Active) != nil {
				return errors.New("retained replacement ledger UID changed")
			}
			t.Logf("case=%s original_active_uid=%s replacement_uid=%s readiness=503 preserved=true", c.Name, before.ActiveUID, newUID)
		} else if code != http.StatusConflict || newUID != "" {
			return errors.New("late original Create was not rejected while committed ledger existed")
		}
	}
	// Join every actual writer and drain the proxy handlers before the final
	// effect/identity assertions. Checking counters before shutdown could miss
	// a last-moment forbidden operation and incorrectly report PASS.
	if err := stopOwned(); err != nil {
		return err
	}
	if refusedProxy != nil {
		refusedProxy.mu.Lock()
		writes := 0
		for stage, attempts := range refusedProxy.attempts {
			if stage != "read" {
				writes += attempts
			}
		}
		refusedProxy.mu.Unlock()
		if writes != 0 {
			return errors.New("refused Service restart wrote API state")
		}
	}
	if !*c.AllowCommittedLedgerDeletion {
		after, err := serviceCommittedSnapshot(ctx, api, m, c)
		if err != nil || before.ActiveUID != after.ActiveUID || before.QueueUID != after.QueueUID {
			return errors.New("original committed pair changed across Service fault/handoff")
		}
	}
	if err := serviceOriginalInstallation(ctx, api, m, c); err != nil {
		return err
	}
	if err := serviceEmptyWorkloads(ctx, api, c); err != nil {
		return err
	}
	for _, proxy := range proxies {
		if err := proxy.check(); err != nil {
			return err
		}
	}
	t.Logf("PASS actual-Service-controlplane case=%s namespace_uid=%s genesis_uid=%s active_uid=%s queue_uid=%s lease_uid=%s old_holder=%s new_holder=%s old_pid=%d survivor_pid=%d; no build/runner/budget qualification",
		c.Name, c.Fixture.NamespaceUID, c.Fixture.GenesisUID, before.ActiveUID, before.QueueUID, lease.UID, *lease.Spec.HolderIdentity, *nextLease.Spec.HolderIdentity, children[owner].command.Process.Pid, survivor.command.Process.Pid)
	return nil
}

func TestRealAPIActualServiceControlPlane(t *testing.T) {
	path := os.Getenv(serviceProcessManifestEnv)
	enabled, err := processOptIn(path, os.Getenv(serviceProcessChildEnv), os.Getenv(serviceProcessHashEnv), os.Getenv(serviceProcessDeadlineEnv), os.Getenv(serviceProcessProxyEnv), os.Getenv(serviceProcessTokenEnv), os.Getenv(serviceProcessListenEnv))
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Skip("set KOVA_GENESIS_SERVICE_MANIFEST to an explicitly reviewed private fresh seven-case manifest")
	}
	if deadline, ok := t.Deadline(); !ok || time.Until(deadline) < serviceProcessTotalLimit+time.Minute {
		t.Fatal("Service process gate requires -test.timeout=25m or longer")
	}
	m, hash, err := loadServiceProcessManifest(path, "")
	if err != nil {
		t.Fatalf("Service manifest refused before effects: %v", err)
	}
	if err := checkServiceProcessExecutable(m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), serviceProcessTotalLimit)
	defer cancel()
	cfg, api, err := openServiceProcessConfig(ctx, m)
	if err != nil {
		t.Fatal("original Service Kind target qualification failed")
	}
	// Validate every fresh runner/control namespace and immutable Secret before
	// any child may write. Empty Lists only veto; the installer owns freshness.
	for _, c := range m.Cases {
		if err := processPreflight(ctx, api, m.target(), c.Fixture); err != nil {
			t.Fatalf("Service case %s preflight refused: %v", c.Name, err)
		}
		if err := serviceEmptyWorkloads(ctx, api, c); err != nil {
			t.Fatal(err)
		}
		if _, err := getServiceLease(ctx, api, c); !apierrors.IsNotFound(err) {
			t.Fatal("Service fixture already has a Lease or its absence is unknown")
		}
	}
	for index, c := range m.Cases {
		if _, _, err := loadServiceProcessManifest(path, hash); err != nil {
			t.Fatal("Service manifest changed; retained all API evidence")
		}
		if _, _, err := openServiceProcessConfig(ctx, m); err != nil {
			t.Fatal("Service Kind identity changed; retained all API evidence")
		}
		caseCtx, caseCancel := context.WithTimeout(ctx, serviceProcessCaseLimit)
		err := runActualServiceCase(caseCtx, t, path, hash, m, index, cfg, api)
		caseCancel()
		if err != nil || t.Failed() {
			t.Fatalf("actual Service case %s failed; all API evidence retained: %v", c.Name, err)
		}
	}
	t.Log("all actual Service control-plane cases passed; all API objects retained; no image/runner/end-to-end or API-load qualification claimed")
}
