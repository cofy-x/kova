package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Exercise the runtime constructors and receipt closure against an in-memory
// RoundTripper, without a Kubernetes server, credentials, or remote requests.
// The real 20/40 limiter is retained; all five calls fit its initial burst.
func TestGenesisRuntimeHTTPReadersShareOneRateLimiter(t *testing.T) {
	receipt, _ := genesisTestReceiptAndConfig()
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	secret := &corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "control-counted", Name: "receipt", UID: "secret-counted-original"},
		Immutable:  &immutable, Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{admissioncontract.ReceiptSecretDataKey: raw}}
	var calls atomic.Int32
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		var object any
		switch request.URL.Path {
		case "/api/v1/namespaces/control-counted/secrets/receipt":
			object = secret
		case "/api/v1/namespaces/" + receipt.Namespace:
			object = &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
				ObjectMeta: metav1.ObjectMeta{Name: receipt.Namespace, UID: "namespace-original"}}
		case "/api/v1/namespaces/" + receipt.Namespace + "/configmaps/" + admissioncontract.QueueLedgerName:
			object = &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
				ObjectMeta: metav1.ObjectMeta{Namespace: receipt.Namespace, Name: admissioncontract.QueueLedgerName}}
		case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
			object = &authorizationv1.SubjectAccessReview{TypeMeta: metav1.TypeMeta{APIVersion: "authorization.k8s.io/v1", Kind: "SubjectAccessReview"},
				Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}
		default:
			t.Errorf("unexpected mock request method=%s path=%s", request.Method, request.URL.Path)
			return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`{}`)), Header: http.Header{}}, nil
		}
		encoded, marshalErr := json.Marshal(object)
		if marshalErr != nil {
			return nil, marshalErr
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(encoded))),
			Header: http.Header{"Content-Type": []string{"application/json"}}}, nil
	})
	metrics := newKubeClientMetricSet()
	registry := testKubeMetricRegistry(t, metrics)
	config := singleAttemptWrites(&rest.Config{Host: "https://offline.invalid", Transport: transport,
		ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"}})
	leader, readiness, httpConfig := configureKubeClientRateLimitsWithMetrics(config, 20, 40, metrics)
	if httpConfig.QPS != 20 || httpConfig.Burst != 40 ||
		httpConfig.RateLimiter == leader.RateLimiter || httpConfig.RateLimiter == readiness.RateLimiter || httpConfig.RateLimiter == config.RateLimiter {
		t.Fatal("runtime HTTP traffic class lost its independent 20/40 budget")
	}
	clientset, err := kubernetes.NewForConfig(httpConfig)
	if err != nil {
		t.Fatal(err)
	}
	if clientset.CoreV1().RESTClient().GetRateLimiter() != httpConfig.RateLimiter {
		t.Fatal("typed CoreV1 client copied a different limiter")
	}
	direct := admissiongenesis.DirectClient{Client: clientset}
	options := genesisReceiptOptions{SecretNamespace: secret.Namespace, SecretName: secret.Name, SecretUID: string(secret.UID)}
	if err := options.checkRaw(raw, direct)(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := direct.GetNamespace(t.Context(), receipt.Namespace); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := kube.NewClientForConfig(httpConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kubeClient.GetSecretData(t.Context(), secret.Namespace, secret.Name, admissioncontract.ReceiptSecretDataKey); err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	crClient, err := ctrlclient.New(httpConfig, ctrlclient.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	if err := crClient.Get(context.Background(), ctrlclient.ObjectKey{Namespace: receipt.Namespace, Name: admissioncontract.QueueLedgerName}, &corev1.ConfigMap{}); err != nil {
		t.Fatal(err)
	}
	if clientset.AuthorizationV1().RESTClient().GetRateLimiter() != httpConfig.RateLimiter {
		t.Fatal("typed authorization client copied a different limiter")
	}
	authorizer, err := serviceauth.NewSubjectAccessReviewAuthorizer(clientset.AuthorizationV1().SubjectAccessReviews())
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(t.Context(), serviceauth.Principal{Username: "counted-user"},
		serviceauth.Attributes{Verb: "create", Namespace: receipt.Namespace, Resource: serviceauth.ServiceBuildResource}); err != nil {
		t.Fatal(err)
	}
	waits := kubeMetricCount(t, registry, "kova_service_kube_limiter_wait_seconds", map[string]string{
		"traffic_class": kubeClassHTTP, "outcome": "ok"})
	if calls.Load() != 5 || waits != 5 {
		t.Fatalf("shared actual HTTP config clients: wire=%d limiterWait=%d, want both 5", calls.Load(), waits)
	}
	t.Log("runtime receipt Secret, direct Namespace, kube wrapper, controller-runtime named GET and typed SAR all used the same HTTP 20/40 limiter; no remote API")
}
