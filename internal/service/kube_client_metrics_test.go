package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type contextWaitLimiter struct{}

func (contextWaitLimiter) TryAccept() bool { return false }
func (contextWaitLimiter) Accept()         {}
func (contextWaitLimiter) Stop()           {}
func (contextWaitLimiter) QPS() float32    { return 0 }
func (contextWaitLimiter) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type unreadResponseBody struct{}

func (unreadResponseBody) Read([]byte) (int, error) { panic("wire telemetry read response body") }
func (unreadResponseBody) Close() error             { return nil }

func testKubeMetricRegistry(t *testing.T, metrics *kubeClientMetricSet) *prometheus.Registry {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.limiterWait, metrics.wireRTT, metrics.wireCount)
	return registry
}

func kubeMetricCount(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			actual := make(map[string]string)
			for _, label := range metric.GetLabel() {
				actual[label.GetName()] = label.GetValue()
			}
			if len(actual) != len(labels) {
				continue
			}
			match := true
			for key, value := range labels {
				if actual[key] != value {
					match = false
				}
			}
			if match {
				if metric.GetHistogram() != nil {
					return metric.GetHistogram().GetSampleCount()
				}
				if metric.GetCounter() != nil {
					return uint64(metric.GetCounter().GetValue())
				}
			}
		}
	}
	return 0
}

func TestKubeLimiterCanceledWaitIsOneLabeledObservation(t *testing.T) {
	metrics := newKubeClientMetricSet()
	registry := testKubeMetricRegistry(t, metrics)
	limiter := &observedKubeRateLimiter{
		RateLimiter:  contextWaitLimiter{},
		trafficClass: kubeClassController,
		metrics:      metrics,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want context canceled", err)
	}
	if limiter.TryAccept() {
		t.Fatal("never limiter accepted a token")
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_limiter_wait_seconds", map[string]string{
		"traffic_class": kubeClassController, "outcome": "canceled",
	}); got != 1 {
		t.Fatalf("canceled Wait observations = %d, want 1", got)
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_limiter_wait_seconds", map[string]string{
		"traffic_class": kubeClassController, "outcome": "ok",
	}); got != 0 {
		t.Fatalf("successful Wait observations = %d, want 0", got)
	}
}

func TestKubeWireLabelsHaveFixedVocabularyAndNoNames(t *testing.T) {
	for _, tc := range []struct {
		method, path, verb, resource string
	}{
		{"GET", "/api/v1/namespaces/secret-tenant/pods", "list", "pods"},
		{"GET", "/api/v1/namespaces/secret-tenant/pods?watch=true&token=private", "watch", "pods"},
		{"GET", "/api/v1/namespaces/secret-tenant/pods/private-pod/status", "get", "pods/status"},
		{"GET", "/api/v1/nodes/private-node/proxy/stats/summary", "get", "nodes/proxy"},
		{"POST", "/apis/authentication.k8s.io/v1/tokenreviews", "create", "tokenreviews"},
		{"POST", "/apis/authorization.k8s.io/v1/subjectaccessreviews", "create", "subjectaccessreviews"},
		{"DELETE", "/apis/kova.cofy.dev/v1alpha1/namespaces/secret-tenant/kovabuilds/private-build", "delete", "kovabuilds"},
		{"GET", "/apis", "get", "discovery"},
		{"GET", "/apis/arbitrary.example/v1/namespaces/secret-tenant/private-resources/private-name", "get", "other"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "https://kube.example"+tc.path, nil)
			verb, resource := kubeRequestLabels(request)
			if verb != tc.verb || resource != tc.resource {
				t.Fatalf("labels = %q/%q, want %q/%q", verb, resource, tc.verb, tc.resource)
			}
			if strings.Contains(verb+resource, "secret") || strings.Contains(verb+resource, "private") {
				t.Fatal("object name or query leaked into normalized labels")
			}
		})
	}
	for _, tc := range []struct {
		code int
		want string
	}{
		{200, "200"}, {429, "429"}, {502, "5xx_other"}, {599, "5xx_other"}, {700, "unknown"},
	} {
		if got := kubeResponseStatus(&http.Response{StatusCode: tc.code}, nil); got != tc.want {
			t.Fatalf("status %d normalized to %q, want %q", tc.code, got, tc.want)
		}
	}
	if got := kubeResponseStatus(nil, context.DeadlineExceeded); got != "transport_error" {
		t.Fatalf("transport error status = %q", got)
	}
}

func TestKubeClientsetCopiesShareHTTPClassLimiterAndMetrics(t *testing.T) {
	metrics := newKubeClientMetricSet()
	registry := testKubeMetricRegistry(t, metrics)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"Pod","apiVersion":"v1","metadata":{"name":"private-pod"}}`))
	}))
	defer server.Close()
	config := singleAttemptWrites(&rest.Config{Host: server.URL})
	leader, readiness, httpConfig := configureKubeClientRateLimitsWithMetrics(config, 20, 40, metrics)
	first := rest.CopyConfig(httpConfig)
	second := rest.CopyConfig(httpConfig)
	if first.RateLimiter != second.RateLimiter || first.WrapTransport == nil || second.WrapTransport == nil {
		t.Fatal("copied HTTP client configs lost their shared limiter or transport hook")
	}
	if first.RateLimiter == config.RateLimiter || first.RateLimiter == leader.RateLimiter || first.RateLimiter == readiness.RateLimiter {
		t.Fatal("traffic classes unexpectedly share a limiter")
	}
	for _, clientConfig := range []*rest.Config{first, second} {
		client, err := kubernetes.NewForConfig(clientConfig)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.CoreV1().Pods("secret-tenant").Get(context.Background(), "private-pod", metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("wire GET calls = %d, want 2", calls.Load())
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_wire_requests_total", map[string]string{
		"traffic_class": kubeClassHTTP, "verb": "get", "resource": "pods", "status": "200",
	}); got != 2 {
		t.Fatalf("HTTP class wire GET count = %d, want 2", got)
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_limiter_wait_seconds", map[string]string{
		"traffic_class": kubeClassHTTP, "outcome": "ok",
	}); got != 2 {
		t.Fatalf("HTTP class limiter Wait count = %d, want 2", got)
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_wire_round_trip_seconds", map[string]string{
		"traffic_class": kubeClassHTTP, "resource": "pods",
	}); got != 2 {
		t.Fatalf("HTTP class RTT count = %d, want 2", got)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if strings.Contains(label.GetName()+label.GetValue(), "secret-tenant") || strings.Contains(label.GetName()+label.GetValue(), "private-pod") {
					t.Fatal("namespace or object name leaked into metric labels")
				}
			}
		}
	}
}

func TestKubeWatchWireRTTStopsAtHeadersWithoutReadingStream(t *testing.T) {
	metrics := newKubeClientMetricSet()
	registry := testKubeMetricRegistry(t, metrics)
	config := &rest.Config{RateLimiter: contextWaitLimiter{}}
	instrumentKubeClientConfig(config, kubeClassReadiness, metrics)
	transport := config.WrapTransport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: unreadResponseBody{}}, nil
	}))
	request := httptest.NewRequest(http.MethodGet, "https://kube.example/api/v1/namespaces/private-ns/pods?watch=true&token=private", nil)
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := kubeMetricCount(t, registry, "kova_service_kube_wire_round_trip_seconds", map[string]string{
		"traffic_class": kubeClassReadiness, "resource": "pods",
	}); got != 1 {
		t.Fatalf("watch setup RTT observations = %d, want 1", got)
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_wire_requests_total", map[string]string{
		"traffic_class": kubeClassReadiness, "verb": "watch", "resource": "pods", "status": "200",
	}); got != 1 {
		t.Fatalf("watch setup wire count = %d, want 1", got)
	}
}

func TestKubeMutationStillHasOneWireAttemptWithInstrumentation(t *testing.T) {
	metrics := newKubeClientMetricSet()
	registry := testKubeMetricRegistry(t, metrics)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	group := schema.GroupVersion{Group: "kova.cofy.dev", Version: "v1alpha1"}
	config := singleAttemptWrites(&rest.Config{Host: server.URL, APIPath: "/apis", ContentConfig: rest.ContentConfig{
		GroupVersion: &group, NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}})
	_, _, httpConfig := configureKubeClientRateLimitsWithMetrics(config, 20, 40, metrics)
	client, err := rest.RESTClientFor(httpConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Post().Resource("kovabuilds").Body([]byte(`{"kind":"KovaBuild","apiVersion":"kova.cofy.dev/v1alpha1"}`)).Do(context.Background()).Error(); err == nil {
		t.Fatal("expected 503 response")
	}
	if calls.Load() != 1 {
		t.Fatalf("wire mutation attempts = %d, want 1", calls.Load())
	}
	if got := kubeMetricCount(t, registry, "kova_service_kube_wire_requests_total", map[string]string{
		"traffic_class": kubeClassHTTP, "verb": "create", "resource": "kovabuilds", "status": "503",
	}); got != 1 {
		t.Fatalf("wire mutation metric count = %d, want 1", got)
	}
}
