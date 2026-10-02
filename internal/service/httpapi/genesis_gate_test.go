package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type genesisCheckFunc func(context.Context) error

func (f genesisCheckFunc) Check(ctx context.Context) error { return f(ctx) }

func TestGenesisHTTPGuardViewsRequireOneOriginalBinding(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	httpGuard := &admissiongenesis.Guard{Original: admissiongenesis.Binding{NamespaceUID: "original"}}
	readinessGuard := &admissiongenesis.Guard{Original: admissiongenesis.Binding{NamespaceUID: "replacement"}}
	if err := srv.WithGenesisGuards(httpGuard, readinessGuard); err == nil || srv.genesis != nil {
		t.Fatal("mismatched Genesis guards were installed")
	}
	readinessGuard.Original = httpGuard.Original
	if err := srv.WithGenesisGuards(httpGuard, readinessGuard); err != nil || srv.genesis != httpGuard || srv.readinessGenesis != readinessGuard {
		t.Fatalf("matching Genesis guard views were not installed: %v", err)
	}
}

func TestGenesisReadinessUsesIndependentGuard(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.genesis = genesisCheckFunc(func(context.Context) error { return errors.New("HTTP API budget exhausted") })
	srv.readinessGenesis = genesisCheckFunc(func(context.Context) error { return nil })
	if code := readinessCode(srv); code != http.StatusOK {
		t.Fatalf("HTTP admission budget blocked readiness: %d", code)
	}
	if err := srv.checkGenesis(context.Background()); err == nil {
		t.Fatal("HTTP admission guard did not use its own failed budget")
	}
}

func TestGenesisHTTPStartupNeverRunsLegacyBothMissingBootstrap(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	deleteReadinessLedger(t, srv, "kova-service-admission")
	deleteReadinessLedger(t, srv, queueadmission.ConfigMapName)
	loss := errors.New("committed pair lost")
	srv.withGenesisChecker(genesisCheckFunc(func(context.Context) error { return loss }))
	if err := srv.startAdmission(context.Background()); !errors.Is(err, loss) {
		t.Fatalf("Genesis mode did not refuse both-ledger loss: %v", err)
	}
	for _, name := range []string{"kova-service-admission", queueadmission.ConfigMapName} {
		var cm corev1.ConfigMap
		if err := srv.reader.Get(context.Background(), kubeObjectKey(srv.cfg.Namespace, name), &cm); !apierrors.IsNotFound(err) {
			t.Fatalf("legacy bootstrap recreated committed %s ledger: %v", name, err)
		}
	}
	if code := readinessCode(srv); code != http.StatusServiceUnavailable {
		t.Fatalf("readiness after committed loss = %d", code)
	}
}

func TestGenesisHTTPSubmissionGatesReplayAndPostReserveCreate(t *testing.T) {
	t.Run("replay refuses before queue mutation", func(t *testing.T) {
		srv := newTestServer(t, &fakeKube{})
		calls := 0
		srv.withGenesisChecker(genesisCheckFunc(func(context.Context) error {
			calls++
			return errors.New("original Namespace replaced")
		}))
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, queueRequest(t, "replay"))
		if rec.Code != http.StatusServiceUnavailable || calls != 1 {
			t.Fatalf("unsafe replay response=%d checks=%d body=%s", rec.Code, calls, rec.Body.String())
		}
		var builds kovav1.KovaBuildList
		if err := srv.reader.List(context.Background(), &builds, client.InNamespace(srv.cfg.Namespace)); err != nil {
			t.Fatal(err)
		}
		if len(builds.Items) != 0 {
			t.Fatal("unsafe replay created a CR")
		}
	})
	t.Run("loss after reserve holds intent and blocks CR Create", func(t *testing.T) {
		srv := newTestServer(t, &fakeKube{})
		calls := 0
		srv.withGenesisChecker(genesisCheckFunc(func(context.Context) error {
			calls++
			if calls == 3 {
				return errors.New("original ledger replaced after reserve")
			}
			return nil
		}))
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, queueRequest(t, "post-reserve"))
		id := rec.Header().Get("X-Kova-Build-ID")
		if rec.Code != http.StatusServiceUnavailable || calls != 3 || id == "" {
			t.Fatalf("post-reserve refusal=%d checks=%d id=%q body=%s", rec.Code, calls, id, rec.Body.String())
		}
		if _, found, err := srv.queueStore().Lookup(context.Background(), id); err != nil || !found {
			t.Fatalf("uncertain queue intent was released: found=%t err=%v", found, err)
		}
		var build kovav1.KovaBuild
		if err := srv.reader.Get(context.Background(), client.ObjectKey{Namespace: srv.cfg.Namespace, Name: id}, &build); !apierrors.IsNotFound(err) {
			t.Fatalf("CR Create passed changed binding: %v", err)
		}
	})
}

func TestGenesisHTTPRunnerActionsAndCancelRefuseWithoutOriginalPair(t *testing.T) {
	kube := &fakeKube{}
	srv := newTestServer(t, kube)
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: srv.cfg.Namespace},
		Spec:   kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
		Status: kovav1.KovaBuildStatus{RunnerPodName: "kova-job-job"}}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	srv.withGenesisChecker(genesisCheckFunc(func(context.Context) error { return errors.New("original pair unavailable") }))
	for _, route := range []string{"/v1/builds/job/export", "/v1/builds/job/preheat", "/v1/builds/job/cancel"} {
		req := httptest.NewRequest(http.MethodPost, route, nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s escaped Genesis gate: status=%d body=%s", route, rec.Code, rec.Body.String())
		}
	}
	if len(kube.execs) != 0 {
		t.Fatalf("runner POST escaped Genesis gate: %d execs", len(kube.execs))
	}
	var persisted kovav1.KovaBuild
	if err := srv.reader.Get(context.Background(), client.ObjectKeyFromObject(build), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Annotations[kovav1.CancellationRequestedAnnotation] != "" {
		t.Fatal("cancel annotation persisted without original pair")
	}
}
