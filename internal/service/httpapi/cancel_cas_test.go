package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Capture the actual HTTP handler patch, then let the fake API enforce its tests.
type cancelCASClient struct {
	client.Client
	t          *testing.T
	wires      [][]byte
	before     func()
	patchError error
	replyError error
}

func (c *cancelCASClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*kovav1.KovaBuild); !ok {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}
	if patch.Type() != types.JSONPatchType {
		c.t.Fatalf("cancel patch type=%s, want JSON Patch", patch.Type())
	}
	body, err := patch.Data(obj)
	if err != nil {
		c.t.Fatal(err)
	}
	c.wires = append(c.wires, body)
	if c.before != nil {
		c.before()
	}
	if c.patchError != nil {
		return c.patchError
	}
	if err := c.Client.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	return c.replyError
}

func newCancelCASServer(t *testing.T, phase string, annotations map[string]string) (*Server, *cancelCASClient, *kovav1.KovaBuild) {
	t.Helper()
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "original", Namespace: srv.cfg.Namespace, UID: "original-uid", Annotations: annotations},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
		Status:     kovav1.KovaBuildStatus{Phase: phase},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	capture := &cancelCASClient{Client: srv.client, t: t}
	srv.client = capture
	return srv, capture, build.DeepCopy()
}

func cancelCASRequest(srv *Server) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/builds/original/cancel", nil)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	srv.routes().ServeHTTP(response, request)
	return response
}

func cancelCASStored(t *testing.T, api client.Reader, original *kovav1.KovaBuild) *kovav1.KovaBuild {
	t.Helper()
	var stored kovav1.KovaBuild
	if err := api.Get(context.Background(), client.ObjectKeyFromObject(original), &stored); err != nil {
		t.Fatal(err)
	}
	return &stored
}

func assertCancelCASWire(t *testing.T, capture *cancelCASClient, original *kovav1.KovaBuild) {
	t.Helper()
	if len(capture.wires) != 1 {
		t.Fatalf("cancel patch attempts=%d, want exactly one", len(capture.wires))
	}
	var operations []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(capture.wires[0], &operations); err != nil {
		t.Fatal(err)
	}
	if len(operations) != 3 {
		t.Fatalf("cancel operations=%s", capture.wires[0])
	}
	for i, pin := range []struct{ path, value string }{
		{"/metadata/uid", string(original.UID)},
		{"/metadata/resourceVersion", original.ResourceVersion},
	} {
		var actual string
		if operations[i].Op != "test" || operations[i].Path != pin.path ||
			json.Unmarshal(operations[i].Value, &actual) != nil || actual != pin.value || actual == "" {
			t.Fatalf("cancel did not test original %s: %s", pin.path, capture.wires[0])
		}
	}
	if operations[2].Op != "add" || operations[2].Path != "/metadata/annotations" {
		t.Fatalf("cancel wrote outside annotations: %s", capture.wires[0])
	}
}

func TestCancelCASPreservesAnnotationsAndNonterminalPhase(t *testing.T) {
	const requestedAt = "2026-10-11T00:00:00Z"
	for _, test := range []struct {
		name, phase string
		annotations map[string]string
	}{
		{"queued nil", kovav1.PhaseQueued, nil},
		{"queued existing", kovav1.PhaseQueued, map[string]string{"other.example/key": "keep"}},
		{"starting empty", kovav1.PhaseStarting, map[string]string{kovav1.CancellationRequestedAnnotation: ""}},
		{"running", kovav1.PhaseRunning, map[string]string{"other.example/key": "keep"}},
		{"verifying", kovav1.PhaseVerifying, nil},
		{"idempotent", kovav1.PhaseQueued, map[string]string{kovav1.CancellationRequestedAnnotation: requestedAt, "other.example/key": "keep"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, test.phase, test.annotations)
			response := cancelCASRequest(srv)
			if response.Code != http.StatusAccepted {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			assertCancelCASWire(t, capture, original)
			stored := cancelCASStored(t, capture.Client, original)
			if stored.UID != original.UID || stored.Status.Phase != original.Status.Phase || !reflect.DeepEqual(stored.Spec, original.Spec) {
				t.Fatal("cancellation changed identity, spec, or controller-owned phase")
			}
			for key, value := range original.Annotations {
				if key != kovav1.CancellationRequestedAnnotation && stored.Annotations[key] != value {
					t.Fatalf("cancel lost annotation %s", key)
				}
			}
			value := stored.Annotations[kovav1.CancellationRequestedAnnotation]
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				t.Fatalf("invalid cancellation timestamp %q", value)
			}
			if old := original.Annotations[kovav1.CancellationRequestedAnnotation]; old != "" && value != old {
				t.Fatalf("idempotent cancellation changed timestamp %q to %q", old, value)
			}
		})
	}
}

func TestCancelCASRejectsConcurrentBuildChangesWithoutRetry(t *testing.T) {
	for _, change := range []string{"same name replacement with same RV", "annotation update", "terminal phase update"} {
		t.Run(change, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, kovav1.PhaseQueued, map[string]string{"other": "original"})
			var changed *kovav1.KovaBuild
			capture.before = func() {
				changed = cancelCASStored(t, capture.Client, original)
				switch change {
				case "same name replacement with same RV":
					if err := capture.Client.Delete(context.Background(), changed); err != nil {
						t.Fatal(err)
					}
					changed.UID, changed.ResourceVersion = "replacement-uid", ""
					changed.Spec.Requester.Username = "another-owner"
					if err := capture.Client.Create(context.Background(), changed); err != nil {
						t.Fatal(err)
					}
					if changed.ResourceVersion != original.ResourceVersion {
						t.Fatal("replacement test must isolate UID by keeping the same RV")
					}
				case "annotation update":
					changed.Annotations["other"] = "concurrent"
					changed.Annotations["new.example/key"] = "concurrent"
					if err := capture.Client.Update(context.Background(), changed); err != nil {
						t.Fatal(err)
					}
				case "terminal phase update":
					changed.Status.Phase = kovav1.PhaseSucceeded
					if err := capture.Client.Status().Update(context.Background(), changed); err != nil {
						t.Fatal(err)
					}
				}
			}
			assertAPIError(t, cancelCASRequest(srv), http.StatusInternalServerError, apiv1.ErrorCodeInternal, true)
			assertCancelCASWire(t, capture, original)
			if stored := cancelCASStored(t, capture.Client, original); !reflect.DeepEqual(stored, changed) {
				t.Fatalf("cancel changed concurrent object: got=%#v want=%#v", stored, changed)
			}
		})
	}
}

func TestCancelCASConflictAndUnknownDoNotRetry(t *testing.T) {
	for _, test := range []struct {
		name       string
		patchError error
		replyError error
	}{
		{"conflict", apierrors.NewConflict(schema.GroupResource{Group: kovav1.Group, Resource: "kovabuilds"}, "original", errors.New("changed")), nil},
		{"unknown before apply", context.DeadlineExceeded, nil},
		{"unknown after apply", nil, context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, kovav1.PhaseQueued, nil)
			capture.patchError, capture.replyError = test.patchError, test.replyError
			assertAPIError(t, cancelCASRequest(srv), http.StatusInternalServerError, apiv1.ErrorCodeInternal, true)
			assertCancelCASWire(t, capture, original)
			stored := cancelCASStored(t, capture.Client, original)
			if stored.Status.Phase != original.Status.Phase || stored.UID != original.UID ||
				(stored.Annotations[kovav1.CancellationRequestedAnnotation] != "") != (test.replyError != nil) {
				t.Fatal("unknown cancellation changed lifecycle or retried its write")
			}
		})
	}
}

type cancelUnpinnedReader struct {
	client.Reader
	missing string
}

func (r cancelUnpinnedReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if _, ok := obj.(*kovav1.KovaBuild); ok {
		if r.missing == "uid" {
			obj.SetUID("")
		} else {
			obj.SetResourceVersion("")
		}
	}
	return nil
}

func TestCancelCASRequiresOriginalIdentity(t *testing.T) {
	for _, missing := range []string{"uid", "resourceVersion"} {
		t.Run(missing, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, kovav1.PhaseQueued, nil)
			srv.reader = cancelUnpinnedReader{Reader: srv.reader, missing: missing}
			assertAPIError(t, cancelCASRequest(srv), http.StatusInternalServerError, apiv1.ErrorCodeInternal, false)
			if len(capture.wires) != 0 || !reflect.DeepEqual(cancelCASStored(t, capture.Client, original), original) {
				t.Fatal("cancel wrote without a complete original identity")
			}
		})
	}
}

func TestCancelCASTerminalAndAuthorizationGuardsDoNotPatch(t *testing.T) {
	for _, phase := range []string{kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled, kovav1.PhaseFailedVerifying} {
		t.Run(phase, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, phase, nil)
			if response := cancelCASRequest(srv); response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if len(capture.wires) != 0 || !reflect.DeepEqual(cancelCASStored(t, capture.Client, original), original) {
				t.Fatal("cancel mutated a fixed outcome")
			}
		})
	}
	for _, phase := range []string{kovav1.PhaseQueued, kovav1.PhaseSucceeded} {
		t.Run("unauthorized "+phase, func(t *testing.T) {
			srv, capture, original := newCancelCASServer(t, phase, nil)
			// Authenticate a different requester; terminal reads also require authorization.
			srv.auth = authenticatorFunc(func(context.Context, string) (serviceauth.Principal, error) {
				return serviceauth.Principal{Username: "another-owner"}, nil
			})
			srv.authz = authorizerFunc(func(_ context.Context, _ serviceauth.Principal, attrs serviceauth.Attributes) error {
				if attrs.Verb != "delete" || attrs.Name != original.Name {
					t.Fatalf("cancel authorization attributes=%#v", attrs)
				}
				return errors.New("denied")
			})
			assertAPIError(t, cancelCASRequest(srv), http.StatusForbidden, apiv1.ErrorCodeForbidden, false)
			if len(capture.wires) != 0 || !reflect.DeepEqual(cancelCASStored(t, capture.Client, original), original) {
				t.Fatal("cancel bypassed original requester authorization")
			}
		})
	}
}
