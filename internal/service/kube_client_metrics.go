package service

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// These four classes have independent budgets. Every client copied from a
// class's REST config keeps its one shared limiter and its class label.
const (
	kubeClassController = "controller"
	kubeClassLeader     = "leader"
	kubeClassReadiness  = "readiness"
	kubeClassHTTP       = "http"
)

var kubeDurationBuckets = []float64{
	0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
	0.5, 1, 2, 5, 10, 30, 60,
}

type kubeClientMetricSet struct {
	limiterWait *prometheus.HistogramVec
	wireRTT     *prometheus.HistogramVec
	wireCount   *prometheus.CounterVec
}

func newKubeClientMetricSet() *kubeClientMetricSet {
	return &kubeClientMetricSet{
		limiterWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kova", Subsystem: "service_kube", Name: "limiter_wait_seconds",
			Help:    "Elapsed time inside one Kubernetes client rate-limiter Wait call, including canceled waits.",
			Buckets: kubeDurationBuckets,
		}, []string{"traffic_class", "outcome"}),
		wireRTT: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kova", Subsystem: "service_kube", Name: "wire_round_trip_seconds",
			Help:    "One Kubernetes HTTP RoundTrip through response headers; excludes limiter wait and body consumption.",
			Buckets: kubeDurationBuckets,
		}, []string{"traffic_class", "resource"}),
		wireCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kova", Subsystem: "service_kube", Name: "wire_requests_total",
			Help: "Kubernetes HTTP wire attempts by fixed traffic class, verb, resource and normalized status.",
		}, []string{"traffic_class", "verb", "resource", "status"}),
	}
}

var serviceKubeClientMetrics = newKubeClientMetricSet()

func init() {
	crmetrics.Registry.MustRegister(
		serviceKubeClientMetrics.limiterWait,
		serviceKubeClientMetrics.wireRTT,
		serviceKubeClientMetrics.wireCount,
	)
}

type observedKubeRateLimiter struct {
	flowcontrol.RateLimiter
	trafficClass string
	metrics      *kubeClientMetricSet
}

// Wait is the only limiter operation the REST request path uses. In
// particular, a RoundTripper cannot see this wait: client-go performs it
// before constructing the wire attempt. Do not call Wait again for telemetry.
func (limiter *observedKubeRateLimiter) Wait(ctx context.Context) error {
	started := time.Now()
	err := limiter.RateLimiter.Wait(ctx)
	outcome := "ok"
	switch {
	case errors.Is(err, context.Canceled):
		outcome = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		outcome = "deadline"
	case err != nil:
		outcome = "error"
	}
	limiter.metrics.limiterWait.WithLabelValues(limiter.trafficClass, outcome).Observe(time.Since(started).Seconds())
	return err
}

func instrumentKubeClientConfig(config *rest.Config, trafficClass string, metrics *kubeClientMetricSet) {
	config.RateLimiter = &observedKubeRateLimiter{
		RateLimiter: config.RateLimiter, trafficClass: trafficClass, metrics: metrics,
	}
	// Wrap composes after singleAttemptWrites. That earlier wrapper removes
	// Retry-After on writes; this outer wrapper observes each actual RoundTrip
	// without creating a retry or reading any response body.
	config.Wrap(func(transport http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			verb, resource := kubeRequestLabels(request)
			started := time.Now()
			response, err := transport.RoundTrip(request)
			metrics.wireRTT.WithLabelValues(trafficClass, resource).Observe(time.Since(started).Seconds())
			metrics.wireCount.WithLabelValues(trafficClass, verb, resource, kubeResponseStatus(response, err)).Inc()
			return response, err
		})
	})
}

var kubeResources = map[string]struct{}{
	"configmaps": {}, "deployments": {}, "events": {}, "leases": {},
	"namespaces": {}, "nodes": {}, "pods": {}, "replicasets": {},
	"secrets": {}, "serviceaccounts": {}, "subjectaccessreviews": {},
	"tokenreviews": {}, "kovabuilds": {},
}

// kubeRequestLabels turns the URL into two values from a fixed vocabulary.
// Object names, namespaces, paths, query values and API groups never become
// metric labels. A watch records only its setup RoundTrip, not stream lifetime.
func kubeRequestLabels(request *http.Request) (verb, resource string) {
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	rest := []string(nil)
	switch {
	case len(parts) >= 2 && parts[0] == "api" && parts[1] != "":
		rest = parts[2:]
	case len(parts) >= 3 && parts[0] == "apis" && parts[1] != "" && parts[2] != "":
		rest = parts[3:]
	}
	resource = "discovery"
	collection := false
	if len(rest) > 0 && rest[0] != "" {
		// /namespaces/<name>/<resource> is a namespaced resource;
		// /namespaces/<name> itself is the namespace object.
		if len(rest) >= 3 && rest[0] == "namespaces" {
			rest = rest[2:]
		}
		if _, known := kubeResources[rest[0]]; known {
			resource = rest[0]
			if len(rest) >= 3 {
				switch resource + "/" + rest[2] {
				case "pods/status", "pods/log", "nodes/proxy", "kovabuilds/status":
					resource += "/" + rest[2]
				default:
					resource = "other"
				}
			}
		} else {
			resource = "other"
		}
		collection = len(rest) == 1
	}
	switch request.Method {
	case http.MethodGet:
		watch := request.URL.Query().Get("watch")
		if watch == "true" || watch == "1" {
			verb = "watch"
		} else if collection {
			verb = "list"
		} else {
			verb = "get"
		}
	case http.MethodPost:
		verb = "create"
	case http.MethodPut:
		verb = "update"
	case http.MethodPatch:
		verb = "patch"
	case http.MethodDelete:
		if collection {
			verb = "delete_collection"
		} else {
			verb = "delete"
		}
	case http.MethodHead:
		verb = "head"
	case http.MethodOptions:
		verb = "options"
	default:
		verb = "other"
	}
	return verb, resource
}

func kubeResponseStatus(response *http.Response, err error) string {
	if err != nil {
		return "transport_error"
	}
	if response == nil {
		return "unknown"
	}
	// Preserve the common admission, auth and retry codes. Every other code
	// falls into a fixed class so a server cannot create arbitrary label values.
	switch response.StatusCode {
	case 200, 201, 202, 204, 206, 400, 401, 403, 404, 409, 410, 422, 429, 500, 503, 504:
		return strconv.Itoa(response.StatusCode)
	}
	switch response.StatusCode / 100 {
	case 1:
		return "1xx_other"
	case 2:
		return "2xx_other"
	case 3:
		return "3xx_other"
	case 4:
		return "4xx_other"
	case 5:
		return "5xx_other"
	default:
		return "unknown"
	}
}
