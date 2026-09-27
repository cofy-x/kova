package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func TestManagerMutationTransportBlocksClientGoWireRetry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wrap       bool
		wantCalls  int32
		wantFailed bool
	}{
		{name: "client-go-default-retries-post", wantCalls: 2},
		{name: "manager-wrapper-sends-once", wrap: true, wantCalls: 1, wantFailed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method=%s", r.Method)
				}
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"kind":"KovaBuild","apiVersion":"kova.cofy.dev/v1alpha1","metadata":{"name":"probe"}}`))
			}))
			defer server.Close()
			group := schema.GroupVersion{Group: "kova.cofy.dev", Version: "v1alpha1"}
			config := &rest.Config{Host: server.URL, APIPath: "/apis", ContentConfig: rest.ContentConfig{
				GroupVersion: &group, NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}}
			if tc.wrap {
				config = singleAttemptWrites(config)
			}
			client, err := rest.RESTClientFor(config)
			if err != nil {
				t.Fatal(err)
			}
			err = client.Post().Resource("kovabuilds").Body([]byte(`{"kind":"KovaBuild","apiVersion":"kova.cofy.dev/v1alpha1"}`)).Do(context.Background()).Error()
			if (err != nil) != tc.wantFailed {
				t.Fatalf("request error=%v, wanted failure=%t", err, tc.wantFailed)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("wire POSTs=%d, want %d", got, tc.wantCalls)
			}
		})
	}
}
