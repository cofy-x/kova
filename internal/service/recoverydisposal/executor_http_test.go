package recoverydisposal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kjson "sigs.k8s.io/json"
)

type executorWireWrite struct {
	method string
	path   string
	body   []byte
}

// This local HTTP harness verifies the real controller-runtime client's
// serialization and bounded one-shot behavior, not Kubernetes API semantics,
// physical retirement, RBAC, namespace admission or release qualification.
func TestExecuteControllerRuntimeHTTPConditionalWireRequests(t *testing.T) {
	for _, lost := range []string{"", "DELETE", "PATCH"} {
		t.Run("lost="+lost, func(t *testing.T) {
			f := newExecutorFixture(t)
			objects := map[string]*unstructured.Unstructured{}
			for _, obj := range f.api.objects {
				objects[executorHTTPPath(obj)] = obj.DeepCopy()
			}
			var mu sync.Mutex
			writes := []executorWireWrite{}
			serverErr := error(nil)
			wireCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				wireCalls++
				w.Header().Set("Content-Type", "application/json")
				body, err := io.ReadAll(io.LimitReader(request.Body, 32769))
				if err != nil || len(body) > 32768 {
					serverErr = errors.New("unbounded HTTP mutation body")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				obj := objects[request.URL.Path]
				if obj == nil {
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
						Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound})
					return
				}
				switch request.Method {
				case http.MethodGet:
					_ = json.NewEncoder(w).Encode(obj.Object)
				case http.MethodDelete:
					var opts metav1.DeleteOptions
					if json.Unmarshal(body, &opts) != nil || obj.GetKind() == "Namespace" || opts.Preconditions == nil ||
						opts.Preconditions.UID == nil || opts.Preconditions.ResourceVersion == nil ||
						*opts.Preconditions.UID != obj.GetUID() || *opts.Preconditions.ResourceVersion != obj.GetResourceVersion() ||
						*opts.Preconditions.ResourceVersion == "123" || opts.GracePeriodSeconds != nil ||
						opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
						serverErr = errors.New("HTTP Delete lacked fresh exact UID/RV conditions")
						w.WriteHeader(http.StatusConflict)
						return
					}
					writes = append(writes, executorWireWrite{request.Method, request.URL.Path, body})
					if len(obj.GetFinalizers()) == 0 {
						delete(objects, request.URL.Path)
					} else {
						obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now().UTC()})
						obj.SetResourceVersion(nextExecutionRV(obj.GetResourceVersion()))
					}
					if lost == request.Method {
						w.WriteHeader(http.StatusInternalServerError)
						_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status: metav1.StatusFailure, Reason: metav1.StatusReasonInternalError, Message: "simulated lost response", Code: http.StatusInternalServerError})
						return
					}
					_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess})
				case http.MethodPatch:
					if obj.GetKind() == "Namespace" || request.Header.Get("Content-Type") != "application/json-patch+json" {
						serverErr = errors.New("HTTP Patch must be exact-object JSONPatch")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					patch, err := jsonpatch.DecodePatch(body)
					encoded, _ := json.Marshal(obj.Object)
					if err != nil {
						serverErr = err
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					updated, err := patch.Apply(encoded)
					if err != nil {
						serverErr = errors.New("HTTP JSONPatch CAS did not match")
						w.WriteHeader(http.StatusConflict)
						return
					}
					result := &unstructured.Unstructured{}
					if _, err := kjson.UnmarshalStrict(updated, &result.Object, kjson.DisallowDuplicateFields); err != nil {
						serverErr = err
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					result.SetResourceVersion(nextExecutionRV(obj.GetResourceVersion()))
					writes = append(writes, executorWireWrite{request.Method, request.URL.Path, body})
					if len(result.GetFinalizers()) == 0 {
						delete(objects, request.URL.Path)
					} else {
						objects[request.URL.Path] = result
					}
					if lost == request.Method {
						w.WriteHeader(http.StatusInternalServerError)
						_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status: metav1.StatusFailure, Reason: metav1.StatusReasonInternalError, Message: "simulated lost response", Code: http.StatusInternalServerError})
						return
					}
					_ = json.NewEncoder(w).Encode(result.Object)
				default:
					serverErr = fmt.Errorf("unexpected method %s", request.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, {Group: kovav1.Group, Version: kovav1.Version}})
			for _, gvk := range []schema.GroupVersionKind{{Version: "v1", Kind: "Namespace"}, {Version: "v1", Kind: "ConfigMap"},
				{Version: "v1", Kind: "Pod"}, {Group: kovav1.Group, Version: kovav1.Version, Kind: "KovaBuild"}} {
				scope := meta.RESTScopeNamespace
				if gvk.Kind == "Namespace" {
					scope = meta.RESTScopeRoot
				}
				mapper.Add(gvk, scope)
			}
			api, err := client.New(&rest.Config{Host: server.URL, Timeout: executionCallTimeout, QPS: 10000, Burst: 10000}, client.Options{Scheme: runtime.NewScheme(), Mapper: mapper})
			if err != nil {
				t.Fatal(err)
			}
			report, err := Execute(context.Background(), api, f.plan, f.grant, f.archives)
			mu.Lock()
			defer mu.Unlock()
			if serverErr != nil {
				t.Fatal(serverErr)
			}
			if report.APICalls != wireCalls {
				t.Fatalf("unexpected wire retry: interface=%d wire=%d", report.APICalls, wireCalls)
			}
			if lost == "" {
				if err != nil || report.Stage != "exact-disposal-pending" || len(writes) != 5 {
					t.Fatalf("HTTP disposal: %#v %v", report, err)
				}
			} else {
				if !errors.Is(err, ErrExecutionUnknown) || report.Stage != "unknown" {
					t.Fatalf("HTTP lost response: %#v %v", report, err)
				}
				count := 0
				for _, write := range writes {
					if write.method == lost {
						count++
					}
				}
				if count != 1 || writes[len(writes)-1].method != lost {
					t.Fatal("HTTP mutation error retried or execution continued")
				}
			}
			for _, write := range writes {
				if write.method == "PATCH" {
					var ops []struct{ Op, Path string }
					if json.Unmarshal(write.body, &ops) != nil || len(ops) != 4 || ops[0].Op != "test" || ops[0].Path != "/metadata/uid" ||
						ops[1].Op != "test" || ops[1].Path != "/metadata/resourceVersion" || ops[2].Op != "test" ||
						ops[2].Path != "/metadata/finalizers" || ops[3].Op != "replace" || ops[3].Path != "/metadata/finalizers" {
						t.Fatal("wire patch changed fields beyond exact finalizer permission")
					}
				}
			}
		})
	}
}

func executorHTTPPath(obj *unstructured.Unstructured) string {
	gvk := obj.GroupVersionKind()
	base := "/api/" + gvk.Version
	if gvk.Group != "" {
		base = "/apis/" + gvk.Group + "/" + gvk.Version
	}
	if obj.GetNamespace() != "" {
		base += "/namespaces/" + obj.GetNamespace()
	}
	resource := strings.ToLower(gvk.Kind) + "s"
	return base + "/" + resource + "/" + obj.GetName()
}
