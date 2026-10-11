package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This offline accounting fixture runs the concrete production handler,
// Guard, Reserve, PinReceipt and receipt readback against in-memory objects.
// It measures API method calls, not elapsed time or a real Kubernetes result.
type admissionCallCounter struct {
	client.Client
	calls map[string]int
}

func (c *admissionCallCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	resource := "build"
	switch obj.(type) {
	case *corev1.Namespace:
		resource = "namespace"
	case *corev1.Secret:
		resource = "secret"
	case *corev1.ConfigMap:
		resource = "receipt"
		switch key.Name {
		case admissioncontract.GenesisName:
			resource = "genesis"
		case admissioncontract.ActiveLedgerName:
			resource = "active"
		case admissioncontract.QueueLedgerName:
			resource = "queue"
		}
	}
	c.calls["get/"+resource]++
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *admissionCallCounter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	resource := "build"
	if _, ok := obj.(*corev1.ConfigMap); ok {
		resource = "receipt"
	}
	c.calls["create/"+resource]++
	return c.Client.Create(ctx, obj, opts...)
}

func (c *admissionCallCounter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.calls["patch/queue"]++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

type countedHTTPReceipts struct{ client client.Client }

type countedHTTPReviewer struct{ counter *admissionCallCounter }

func (r countedHTTPReviewer) Create(_ context.Context, review *authorizationv1.SubjectAccessReview, _ metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error) {
	r.counter.calls["create/sar"]++
	copy := review.DeepCopy()
	copy.Status.Allowed = true
	return copy, nil
}

func (r countedHTTPReceipts) Create(ctx context.Context, cm *corev1.ConfigMap, _ metav1.CreateOptions) (*corev1.ConfigMap, error) {
	// A typed Create response does not itself issue a GET. The ordinary
	// fixture adds one for convenience; omit that mock-only extra call here.
	cm.UID = "counted-receipt-original"
	if err := r.client.Create(ctx, cm); err != nil {
		return nil, err
	}
	return cm.DeepCopy(), nil
}

func (r countedHTTPReceipts) Get(ctx context.Context, name string, _ metav1.GetOptions) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := r.client.Get(ctx, client.ObjectKey{Namespace: "receipts-http", Name: name}, &cm)
	return &cm, err
}

func TestGenesisHTTPFreshAdmissionCallCountBaseline(t *testing.T) {
	srv, base, authAPI, _, writer := newGenesisReceiptHTTPServer(t)
	// Fill one ordinary outstanding queue entry before the measured request.
	if rec := postGenesisReceiptBuild(t, srv, "baseline-existing"); rec.Code != http.StatusAccepted {
		t.Fatalf("initial fixture admission returned %d", rec.Code)
	}
	counter := &admissionCallCounter{Client: base, calls: map[string]int{}}
	writer.Client, authAPI.Client, srv.reader = counter, counter, counter
	authorizer, err := serviceauth.NewSubjectAccessReviewAuthorizer(countedHTTPReviewer{counter: counter})
	if err != nil {
		t.Fatal(err)
	}
	srv.authz = authorizer
	if err := srv.WithRecoveryReceipts(countedHTTPReceipts{client: counter}); err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := json.Marshal(srv.genesisLedger.Bootstrap.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "control-counted", Name: "receipt", UID: "secret-counted-original"},
		Immutable: &immutable, Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{admissioncontract.ReceiptSecretDataKey: receiptRaw}}
	if err := base.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	secretChecks := 0
	srv.genesisLedger.ReceiptCheck = func(ctx context.Context) error {
		secretChecks++
		var observed corev1.Secret
		if err := counter.Get(ctx, client.ObjectKeyFromObject(secret), &observed); err != nil {
			return err
		}
		return admissioncontract.ValidateReceiptSecret(&observed, secret.Namespace, secret.Name, string(secret.UID), receiptRaw)
	}
	rec := postGenesisReceiptBuild(t, srv, "baseline-measured-fresh")
	if rec.Code != http.StatusAccepted || writer.crCreates != 2 || authAPI.queuePatches != 4 {
		t.Fatalf("fresh measured admission: HTTP=%d CRCreates=%d queuePatches=%d", rec.Code, writer.crCreates, authAPI.queuePatches)
	}
	// These are observed upper bounds, not required redundant reads. A safe
	// reduction may pass without preserving the old count; authority/CAS
	// semantics remain covered by the separate protocol regression suites.
	observedBaseline := map[string]int{"get/namespace": 45, "get/genesis": 20, "get/active": 11,
		"get/queue": 15, "get/secret": 20, "get/build": 1, "get/receipt": 1,
		"patch/queue": 2, "create/receipt": 1, "create/build": 1, "create/sar": 1}
	for category, calls := range counter.calls {
		maximum, known := observedBaseline[category]
		if !known || calls > maximum {
			t.Fatalf("fresh admission call budget grew: %s=%d, baseline=%d known=%t", category, calls, maximum, known)
		}
	}
	for _, category := range []string{"create/receipt", "create/build", "create/sar"} {
		if counter.calls[category] != 1 {
			t.Fatalf("fresh admission did not perform one %s: %d", category, counter.calls[category])
		}
	}
	if secretChecks == 0 || secretChecks > 20 || secretChecks%2 != 0 {
		t.Fatalf("fresh admission external receipt checks=%d, baseline maximum=20", secretChecks)
	}
	getCalls, totalCalls := 0, 0
	for key, calls := range counter.calls {
		totalCalls += calls
		if strings.HasPrefix(key, "get/") {
			getCalls += calls
		}
	}
	// Static-token authentication contributes no TokenReview. The actual SAR
	// authorizer above contributes one Create; the original diagnostic
	// baseline had 118 no-contention calls. Lower counts are permitted.
	// This is an offline method count, not real API timing or acceptance.
	t.Logf("offline fresh admission: current Guard.Check=%d totalGET=%d totalCalls=%d categories=%v; observed original baseline GuardGET=100/totalCalls=118", secretChecks/2, getCalls, totalCalls, counter.calls)
}
