package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestReadinessAndSubmissionRequireValidAdmissionLedgers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, srv *Server)
	}{
		{name: "missing queue", mutate: func(t *testing.T, srv *Server) { deleteReadinessLedger(t, srv, queueadmission.ConfigMapName) }},
		{name: "missing active", mutate: func(t *testing.T, srv *Server) { deleteReadinessLedger(t, srv, "kova-service-admission") }},
		{name: "corrupt queue", mutate: func(t *testing.T, srv *Server) {
			corruptReadinessLedger(t, srv, queueadmission.ConfigMapName, "queue.json")
		}},
		{name: "corrupt active", mutate: func(t *testing.T, srv *Server) {
			corruptReadinessLedger(t, srv, "kova-service-admission", "reservations.json")
		}},
		{name: "queue limit drift", mutate: func(_ *testing.T, srv *Server) { srv.cfg.MaxQueuedJobs++ }},
		{name: "active limit drift", mutate: func(_ *testing.T, srv *Server) { srv.cfg.MaxActiveJobs++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t, &fakeKube{})
			if got := readinessCode(srv); got != http.StatusOK {
				t.Fatalf("initial readiness status = %d", got)
			}
			tc.mutate(t, srv)
			if got := readinessCode(srv); got != http.StatusServiceUnavailable {
				t.Fatalf("readiness status after ledger fault = %d", got)
			}
			req := multipartBuildRequest(t, map[string]string{"target": "registry.local/example:dev"})
			req.Header.Set("Authorization", "Bearer token")
			rec := httptest.NewRecorder()
			srv.routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("submission status after ledger fault = %d body=%s", rec.Code, rec.Body.String())
			}
			var builds kovav1.KovaBuildList
			if err := srv.reader.List(context.Background(), &builds, client.InNamespace(srv.cfg.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(builds.Items) != 0 {
				t.Fatalf("unready service created %d builds", len(builds.Items))
			}
		})
	}
}

type unavailableReadinessReader struct {
	client.Reader
	failOperation string
}

func (r unavailableReadinessReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if r.failOperation == "list" {
		return errors.New("dedicated readiness list unavailable")
	}
	return r.Reader.List(ctx, list, options...)
}

func (r unavailableReadinessReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if r.failOperation == "get" {
		return errors.New("dedicated readiness ledger get unavailable")
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func TestReadinessUsesDedicatedReaderWithoutChangingAdmissionReader(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	if got := readinessCode(srv); got != http.StatusOK {
		t.Fatalf("initial readiness status = %d", got)
	}
	for _, operation := range []string{"list", "get"} {
		srv.readinessReader = unavailableReadinessReader{Reader: srv.reader, failOperation: operation}
		if got := readinessCode(srv); got != http.StatusServiceUnavailable {
			t.Fatalf("readiness did not use its dedicated %s reader: %d", operation, got)
		}
		if err := srv.checkAdmissionLedgers(context.Background()); err != nil {
			t.Fatalf("hot admission reader was changed by readiness %s failure: %v", operation, err)
		}
	}
}

func TestMissingQueueLedgerIsNotRecreatedOnServiceRestart(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	deleteReadinessLedger(t, srv, queueadmission.ConfigMapName)
	if err := srv.initializeAdmission(context.Background()); err == nil {
		t.Fatal("startup recreated a missing queue ledger while active ledger existed")
	}
	var cm corev1.ConfigMap
	if err := srv.reader.Get(context.Background(), kubeObjectKey(srv.cfg.Namespace, queueadmission.ConfigMapName), &cm); !apierrors.IsNotFound(err) {
		t.Fatalf("queue ledger after refused restart: %v", err)
	}
}

func TestMissingActiveLedgerIsNotRecreatedOnServiceRestartEvenWhenEmpty(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	deleteReadinessLedger(t, srv, "kova-service-admission")
	if err := srv.initializeAdmission(context.Background()); err == nil {
		t.Fatal("startup recreated a missing active ledger while queue ledger existed")
	}
	var cm corev1.ConfigMap
	if err := srv.reader.Get(context.Background(), kubeObjectKey(srv.cfg.Namespace, "kova-service-admission"), &cm); !apierrors.IsNotFound(err) {
		t.Fatalf("active ledger after refused restart: %v", err)
	}
}

func readinessCode(srv *Server) int {
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec.Code
}

func deleteReadinessLedger(t *testing.T, srv *Server, name string) {
	t.Helper()
	var cm corev1.ConfigMap
	if err := srv.reader.Get(context.Background(), kubeObjectKey(srv.cfg.Namespace, name), &cm); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Delete(context.Background(), &cm); err != nil {
		t.Fatal(err)
	}
}

func corruptReadinessLedger(t *testing.T, srv *Server, name, dataKey string) {
	t.Helper()
	var cm corev1.ConfigMap
	if err := srv.reader.Get(context.Background(), kubeObjectKey(srv.cfg.Namespace, name), &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data[dataKey] = "{invalid"
	if err := srv.client.Update(context.Background(), &cm); err != nil {
		t.Fatal(err)
	}
}
