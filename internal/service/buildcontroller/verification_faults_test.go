package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	registryHealthy int32 = iota
	registryUnavailable
	registryTimeout
)

func faultableVerifiedImage(t *testing.T) (host, ref, digest string, mode *atomic.Int32) {
	t.Helper()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	mode = &atomic.Int32{}
	backend := registry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			switch mode.Load() {
			case registryUnavailable:
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			case registryTimeout:
				<-r.Context().Done()
				return
			}
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	host = strings.TrimPrefix(server.URL, "http://")
	tag, err := name.NewTag(host+"/demo:dev", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageConfig, err := empty.Image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	imageConfig.OS, imageConfig.Architecture = "linux", "amd64"
	image, err := mutate.ConfigFile(empty.Image, imageConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, image); err != nil {
		t.Fatal(err)
	}
	imageDigest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return host, tag.Name(), imageDigest.String(), mode
}

func verifyingWithReceipt(name, ref, digest string) *kovav1.KovaBuild {
	build := lifecycleBuild(name, kovav1.PhaseVerifying)
	build.Spec.Targets = buildTargets(ref)
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	build.Status.VerificationStartedAt = &now
	build.Status.VerificationDeadlineAt = &deadline
	build.Status.VerificationResults = []kovav1.BuildVerificationResult{{
		Format: "oci", Image: ref, Platform: "linux/amd64", PushedDigest: digest, State: "pending",
	}}
	return build
}

func TestRegistryFaultDuringVerificationRetriesAfterLeaderRestart(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int32
	}{
		{name: "config 503", mode: registryUnavailable},
		{name: "config request timeout", mode: registryTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, ref, digest, mode := faultableVerifiedImage(t)
			build := verifyingWithReceipt("registry-fault", ref, digest)
			crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
			kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
				t.Fatal("persisted push receipt must not re-export or re-submit to the runner")
				return nil
			}}
			cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: 150 * time.Millisecond, VerificationWindow: time.Minute}
			key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
			mode.Store(tc.mode)
			firstLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
			if _, err := firstLeader.Reconcile(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			stored := storedLifecycleBuild(t, crClient, build.Name)
			if stored.Status.Phase != kovav1.PhaseVerifying || stored.Status.VerificationAttempts != 1 ||
				stored.Status.VerificationResults[0].State != "pending" || stored.Status.VerificationResults[0].PushedDigest != digest ||
				stored.Status.VerificationLastError == "" || len(kubeClient.deleted) != 0 {
				t.Fatalf("transient verification status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
			}
			mode.Store(registryHealthy)
			past := metav1.NewTime(time.Now().Add(-time.Second))
			stored.Status.VerificationNextAttemptAt = &past
			if err := crClient.Status().Update(context.Background(), stored); err != nil {
				t.Fatal(err)
			}
			cfg.VerificationAttemptTimeout = 3 * time.Second
			newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
			if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			stored = storedLifecycleBuild(t, crClient, build.Name)
			if stored.Status.Phase != kovav1.PhaseSucceeded || stored.Status.VerificationAttempts != 2 ||
				len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest || len(kubeClient.deleted) != 0 {
				t.Fatalf("recovered verification status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
			}
		})
	}
}

func TestRegistryTimeoutUntilVerificationDeadlineFailsBeforeCleanup(t *testing.T) {
	host, ref, digest, mode := faultableVerifiedImage(t)
	build := verifyingWithReceipt("registry-deadline", ref, digest)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
		t.Fatal("persisted push receipt must not re-export or re-submit to the runner")
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: 100 * time.Millisecond}
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
	mode.Store(registryTimeout)
	firstLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	initializeAdmissionForTest(t, &firstLeader)
	if _, err := firstLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseVerifying || stored.Status.VerificationResults[0].State != "pending" ||
		stored.Status.VerificationResults[0].PushedDigest != digest || stored.Status.VerificationLastError == "" || len(kubeClient.deleted) != 0 {
		t.Fatalf("timeout status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	deadline := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationDeadlineAt = &deadline
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	// Restart after the durable deadline: a retry must not POST to the runner
	// or mistake a transport timeout for a valid output.
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "ResultVerificationFailed" ||
		stored.Status.VerificationResults[0].State != "failed" || stored.Status.VerificationResults[0].PushedDigest != digest ||
		!strings.Contains(stored.Status.VerificationLastError, "deadline") || len(kubeClient.deleted) != 0 {
		t.Fatalf("expired status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 {
		t.Fatalf("terminal cleanup deleted=%#v, want runner Pod", kubeClient.deleted)
	}
}

func TestOverwrittenTagWithGarbageCollectedDigestFailsClosedAtDeadline(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	backend := registry.New()
	var oldDigest string
	var oldDigestReads, tagReads atomic.Int32
	var blockOldDigest atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockOldDigest.Load() && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			switch r.URL.Path {
			case "/v2/demo/manifests/" + oldDigest:
				oldDigestReads.Add(1)
				http.NotFound(w, r)
				return
			case "/v2/demo/manifests/shared":
				tagReads.Add(1)
			}
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	tag, err := name.NewTag(host+"/demo:shared", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageForJob := func(job string) string {
		config, err := empty.Image.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		config.OS, config.Architecture = "linux", "amd64"
		config.Config.Labels = map[string]string{"job": job}
		image, err := mutate.ConfigFile(empty.Image, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := remote.Write(tag, image); err != nil {
			t.Fatal(err)
		}
		digest, err := image.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return digest.String()
	}
	oldDigest = imageForJob("build-a")
	newDigest := imageForJob("build-b")
	if oldDigest == newDigest {
		t.Fatal("two builds must have distinct pushed manifests")
	}
	current, err := remote.Get(tag)
	if err != nil || current.Descriptor.Digest.String() != newDigest {
		t.Fatalf("current tag descriptor=%v err=%v, want build B digest %s", current, err, newDigest)
	}
	// Model a registry that GC'd build A after B moved the shared tag. A's
	// exact push receipt must never be replaced with B's still-valid manifest.
	blockOldDigest.Store(true)
	build := verifyingWithReceipt("gc-overwrite", tag.Name(), oldDigest)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
		t.Fatal("persisted push receipt must not be re-exported or re-submitted")
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: time.Second}
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if _, err := r.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseVerifying || stored.Status.VerificationResults[0].State != "pending" ||
		stored.Status.VerificationResults[0].PushedDigest != oldDigest || len(stored.Status.Outputs) != 0 || len(kubeClient.deleted) != 0 ||
		oldDigestReads.Load() == 0 || tagReads.Load() != 0 {
		t.Fatalf("after GC: status=%#v deleted=%#v old-digest reads=%d tag reads=%d", stored.Status, kubeClient.deleted, oldDigestReads.Load(), tagReads.Load())
	}
	deadline := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationDeadlineAt = &deadline
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "ResultVerificationFailed" ||
		stored.Status.VerificationResults[0].State != "failed" || stored.Status.VerificationResults[0].PushedDigest != oldDigest ||
		len(stored.Status.Outputs) != 0 || len(kubeClient.deleted) != 0 || tagReads.Load() != 0 {
		t.Fatalf("expired GC result: status=%#v deleted=%#v tag reads=%d", stored.Status, kubeClient.deleted, tagReads.Load())
	}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 {
		t.Fatalf("terminal runner cleanup deleted=%#v, want one Pod", kubeClient.deleted)
	}
}

type failSucceededStatusClient struct {
	client.Client
	failures int
}

func (c *failSucceededStatusClient) Status() client.SubResourceWriter {
	return &failSucceededStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type failSucceededStatusWriter struct {
	client.SubResourceWriter
	parent *failSucceededStatusClient
}

func (w *failSucceededStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseSucceeded && w.parent.failures > 0 {
		w.parent.failures--
		return errors.New("injected terminal status write failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

type assertReceiptBeforeDeleteKube struct {
	*fakeKube
	check func() error
}

func (k *assertReceiptBeforeDeleteKube) DeletePodWithUID(ctx context.Context, namespace, name string, uid types.UID) error {
	if err := k.check(); err != nil {
		return err
	}
	return k.fakeKube.DeletePodWithUID(ctx, namespace, name, uid)
}

func TestTerminalWriteFailureKeepsExactReceiptAndPodUntilRestart(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	build := verifyingWithReceipt("terminal-write-fault", ref, digest)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	statusClient := &failSucceededStatusClient{Client: crClient, failures: 1}
	kubeClient := &assertReceiptBeforeDeleteKube{fakeKube: &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
		t.Fatal("digest-bearing receipt must not re-export or re-submit")
		return nil
	}}, check: func() error {
		stored := storedLifecycleBuild(t, crClient, build.Name)
		if stored.Status.Phase != kovav1.PhaseSucceeded || len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest {
			return fmt.Errorf("runner cleanup before durable terminal receipt: %#v", stored.Status)
		}
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: time.Second}
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
	recorder := record.NewFakeRecorder(2)
	firstLeader := KovaBuildReconciler{Client: statusClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg, Recorder: recorder}
	initializeAdmissionForTest(t, &firstLeader)
	if _, err := firstLeader.Reconcile(context.Background(), key); err == nil || !strings.Contains(err.Error(), "injected terminal status write failure") {
		t.Fatalf("first reconcile error=%v, want injected terminal write failure", err)
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("completion event before durable terminal write: %s", event)
	default:
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseVerifying || stored.Status.VerificationResults[0].State != "succeeded" ||
		stored.Status.VerificationResults[0].PushedDigest != digest || len(stored.Status.Outputs) != 1 ||
		stored.Status.Outputs[0].ManifestDigest != digest || len(kubeClient.deleted) != 0 {
		t.Fatalf("persisted status after terminal write fault=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	deadline := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationDeadlineAt = &deadline
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseSucceeded || len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest || len(kubeClient.deleted) != 0 {
		t.Fatalf("recovered terminal status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 {
		t.Fatalf("cleanup deleted=%#v, want exact runner Pod after terminal receipt", kubeClient.deleted)
	}
}
