// Package realapitest provides an explicit, fail-closed Kubernetes API target
// for opt-in integration tests. It never loads the user's default kubeconfig.
package realapitest

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Guard struct {
	Client       client.Client
	Namespace    string
	namespaceUID types.UID
	systemUID    types.UID
	kubeconfig   string
	configHash   [sha256.Size]byte
}

// Open requires a dedicated local Kind cluster and creates a namespace that
// did not previously exist. Set a different explicit namespace for each test
// package: an existing namespace is never adopted, even after a failed run.
func Open(t *testing.T, scheme *runtime.Scheme) *Guard {
	t.Helper()
	path := os.Getenv("KOVA_REAL_API_KUBECONFIG")
	if path == "" {
		t.Skip("set KOVA_REAL_API_KUBECONFIG for the opt-in real API test")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("KOVA_REAL_API_KUBECONFIG must be an absolute path")
	}
	namespace := os.Getenv("KOVA_REAL_API_NAMESPACE")
	if !strings.HasPrefix(namespace, "kova-rc-") || len(validation.IsDNS1123Label(namespace)) != 0 {
		t.Fatal("KOVA_REAL_API_NAMESPACE must be an explicit kova-rc- DNS label")
	}
	expectedContext := os.Getenv("KOVA_REAL_API_CONTEXT")
	if !strings.HasPrefix(expectedContext, "kind-kova-rc-api-") {
		t.Fatal("KOVA_REAL_API_CONTEXT must name a dedicated kind-kova-rc-api-* context")
	}
	expectedSystemUID := types.UID(os.Getenv("KOVA_REAL_API_KUBE_SYSTEM_UID"))
	if expectedSystemUID == "" {
		t.Fatal("KOVA_REAL_API_KUBE_SYSTEM_UID must be explicit")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := clientcmd.Load(data)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CurrentContext != expectedContext {
		t.Fatalf("kubeconfig context %q does not equal explicit target %q", loaded.CurrentContext, expectedContext)
	}
	config, err := clientcmd.NewNonInteractiveClientConfig(*loaded, expectedContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := url.Parse(config.Host)
	if err != nil {
		t.Fatal(err)
	}
	host := endpoint.Hostname()
	ip := net.ParseIP(host)
	if endpoint.Scheme != "https" || (host != "localhost" && (ip == nil || !ip.IsLoopback())) {
		t.Fatalf("real API target must be a loopback HTTPS Kind API server, got %q", config.Host)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	g := &Guard{Client: c, Namespace: namespace, systemUID: expectedSystemUID, kubeconfig: path, configHash: sha256.Sum256(data)}
	ctx := t.Context()
	g.checkConfig(t)
	var system corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: "kube-system"}, &system); err != nil {
		t.Fatal(err)
	}
	if system.UID != expectedSystemUID {
		t.Fatalf("wrong cluster: kube-system UID = %q, expected %q", system.UID, expectedSystemUID)
	}
	t.Logf("target context=%s kube-system UID=%s RV=%s", expectedContext, system.UID, system.ResourceVersion)
	var existing corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, &existing); !apierrors.IsNotFound(err) {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("refusing to adopt existing namespace %s UID=%s RV=%s", namespace, existing.UID, existing.ResourceVersion)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatalf("create fresh namespace %s: %v; leave uncertain result for operator inspection", namespace, err)
	}
	if ns.UID == "" || ns.ResourceVersion == "" {
		t.Fatal("namespace create returned no UID or resourceVersion; leave namespace for operator inspection")
	}
	g.namespaceUID = ns.UID
	t.Logf("created namespace=%s UID=%s RV=%s", namespace, ns.UID, ns.ResourceVersion)
	g.Check(t)
	return g
}

func (g *Guard) checkConfig(t *testing.T) {
	t.Helper()
	current, err := os.ReadFile(g.kubeconfig)
	if err != nil || sha256.Sum256(current) != g.configHash {
		t.Fatalf("kubeconfig changed or became unreadable during test: %v", err)
	}
}

// Check must precede each group of writes. On any identity drift, stop and
// leave objects in place rather than attempting a name-only cleanup.
func (g *Guard) Check(t *testing.T) {
	t.Helper()
	g.checkConfig(t)
	ctx := t.Context()
	for name, expected := range map[string]types.UID{"kube-system": g.systemUID, g.Namespace: g.namespaceUID} {
		var ns corev1.Namespace
		if err := g.Client.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
			t.Fatalf("namespace identity check %s: %v", name, err)
		}
		if ns.UID != expected {
			t.Fatalf("namespace identity drift %s: UID=%s, expected=%s", name, ns.UID, expected)
		}
	}
}

// DeleteOwned removes exactly the API object this test created. The namespace
// itself is left for the operator's exact-cluster cleanup after both tests.
func (g *Guard) DeleteOwned(t *testing.T, obj client.Object) {
	t.Helper()
	g.Check(t)
	if obj.GetNamespace() != g.Namespace || obj.GetUID() == "" {
		t.Fatal("refusing to clean object without a recorded namespace and UID")
	}
	uid := obj.GetUID()
	if err := g.Client.Delete(t.Context(), obj, client.Preconditions{UID: &uid}); err != nil {
		t.Fatalf("UID-preconditioned delete %s/%s UID=%s: %v", obj.GetNamespace(), obj.GetName(), uid, err)
	}
	key := client.ObjectKeyFromObject(obj)
	if err := g.Client.Get(t.Context(), key, obj); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted %s/%s UID=%s but GET result was %v; leave namespace for inspection", key.Namespace, key.Name, uid, err)
	}
	t.Logf("deleted owned %s/%s UID=%s", key.Namespace, key.Name, uid)
}

func (g *Guard) CreateOwned(t *testing.T, obj client.Object) {
	t.Helper()
	g.Check(t)
	if obj.GetNamespace() != g.Namespace {
		t.Fatal("object namespace does not match dedicated test namespace")
	}
	if err := g.Client.Create(t.Context(), obj); err != nil {
		t.Fatalf("create %s/%s: %v; leave uncertain result for operator inspection", obj.GetNamespace(), obj.GetName(), err)
	}
	if obj.GetUID() == "" || obj.GetResourceVersion() == "" {
		t.Fatalf("create %s/%s returned no UID or resourceVersion", obj.GetNamespace(), obj.GetName())
	}
	t.Logf("created %s/%s UID=%s RV=%s", obj.GetNamespace(), obj.GetName(), obj.GetUID(), obj.GetResourceVersion())
}

func (g *Guard) GetOwned(t *testing.T, obj client.Object, expectedUID types.UID) {
	t.Helper()
	g.Check(t)
	key := client.ObjectKeyFromObject(obj)
	if key.Namespace != g.Namespace || expectedUID == "" {
		t.Fatal("object read has no recorded test namespace and UID")
	}
	if err := g.Client.Get(t.Context(), key, obj); err != nil {
		t.Fatal(err)
	}
	if obj.GetUID() != expectedUID {
		t.Fatalf("object identity drift %s/%s UID=%s, expected=%s", key.Namespace, key.Name, obj.GetUID(), expectedUID)
	}
	t.Logf("read %s/%s UID=%s RV=%s", key.Namespace, key.Name, obj.GetUID(), obj.GetResourceVersion())
}

func (g *Guard) UpdateOwned(t *testing.T, obj client.Object, expectedUID types.UID) {
	t.Helper()
	g.Check(t)
	if obj.GetNamespace() != g.Namespace || obj.GetUID() != expectedUID || obj.GetResourceVersion() == "" {
		t.Fatal("refusing update without owned UID and resourceVersion")
	}
	oldRV := obj.GetResourceVersion()
	if err := g.Client.Update(t.Context(), obj); err != nil {
		t.Fatal(fmt.Errorf("update %s/%s UID=%s RV=%s: %w", obj.GetNamespace(), obj.GetName(), expectedUID, oldRV, err))
	}
	t.Logf("updated %s/%s UID=%s RV=%s -> %s", obj.GetNamespace(), obj.GetName(), expectedUID, oldRV, obj.GetResourceVersion())
}
