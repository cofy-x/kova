package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type staleBuildListClient struct{ client.Client }

func (c staleBuildListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if builds, ok := list.(*kovav1.KovaBuildList); ok {
		builds.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

type uncertainBuildCreate struct {
	client.Client
	mu      sync.Mutex
	creates int
	persist bool
}

type pausedBuildCreate struct {
	client.Client
	entered chan struct{}
	release chan struct{}
	creates atomic.Int32
}

func (c *pausedBuildCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*kovav1.KovaBuild); !ok {
		return c.Client.Create(ctx, obj, opts...)
	}
	if c.creates.Add(1) == 1 {
		close(c.entered)
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *uncertainBuildCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*kovav1.KovaBuild); !ok {
		return c.Client.Create(ctx, obj, opts...)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	if c.creates == 1 {
		if c.persist {
			if err := c.Client.Create(ctx, obj, opts...); err != nil {
				return err
			}
		}
		return context.DeadlineExceeded
	}
	return c.Client.Create(ctx, obj, opts...)
}

func dualQueueServers(t *testing.T, global, per int, usernames [2]string, writer client.Client, reader client.Reader) ([2]*Server, client.Client) {
	t.Helper()
	if reader == nil {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		if err := kovav1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		reader = crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	}
	base := reader.(client.Client)
	if writer == nil {
		writer = staleBuildListClient{Client: base}
	}
	cfg := testConfig(t.TempDir())
	cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester = global, per
	var servers [2]*Server
	for i, username := range usernames {
		authenticator, err := serviceauth.New(serviceauth.ModeStatic, "token", username, nil)
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = NewServer(cfg, &fakeKube{}, writer, reader, authenticator, serviceauth.AllowAllAuthorizer{})
	}
	if err := servers[0].initializeAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	return servers, base
}

func queueRequest(t *testing.T, idempotencyKey string) *http.Request {
	t.Helper()
	body := map[string]any{
		"source_uri":    "oci://registry.local/sources/test@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"source_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"targets":       requestTargets([]string{"registry.local/example:dev"}),
		"concurrency":   1, "idempotency_key": idempotencyKey,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/builds", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestConcurrentHTTPQueueAdmissionAcrossTwoReplicasWithStaleCache(t *testing.T) {
	for _, tc := range []struct {
		name      string
		users     [2]string
		global    int
		per       int
		maxAccept int
	}{
		{name: "same-requester", users: [2]string{"alice", "alice"}, global: 3, per: 2, maxAccept: 2},
		{name: "global", users: [2]string{"alice", "bob"}, global: 3, per: 3, maxAccept: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, base := dualQueueServers(t, tc.global, tc.per, tc.users, nil, nil)
			handlers := [2]http.Handler{servers[0].routes(), servers[1].routes()}
			const requests = 40
			prepared := make([]*http.Request, requests)
			for i := range prepared {
				prepared[i] = queueRequest(t, fmt.Sprintf("key-%d", i))
			}
			var wg sync.WaitGroup
			results := make(chan int, requests)
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					rec := httptest.NewRecorder()
					handlers[i%2].ServeHTTP(rec, prepared[i])
					results <- rec.Code
				}(i)
			}
			wg.Wait()
			close(results)
			accepted := 0
			for status := range results {
				switch status {
				case http.StatusAccepted:
					accepted++
				case http.StatusTooManyRequests:
				default:
					t.Fatalf("unexpected concurrent status %d", status)
				}
			}
			if accepted == 0 || accepted > tc.maxAccept {
				t.Fatalf("accepted=%d, cap=%d", accepted, tc.maxAccept)
			}
			var builds kovav1.KovaBuildList
			if err := base.List(context.Background(), &builds, client.InNamespace("jobs")); err != nil {
				t.Fatal(err)
			}
			if len(builds.Items) != accepted {
				t.Fatalf("CRs=%d accepted=%d", len(builds.Items), accepted)
			}
			var ledger corev1.ConfigMap
			if err := base.Get(context.Background(), client.ObjectKey{Namespace: "jobs", Name: queueadmission.ConfigMapName}, &ledger); err != nil {
				t.Fatal(err)
			}
			var snapshot struct {
				Intents map[string]any `json:"intents"`
			}
			if err := json.Unmarshal([]byte(ledger.Data["queue.json"]), &snapshot); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Intents) != accepted {
				t.Fatalf("queue intents=%d accepted=%d", len(snapshot.Intents), accepted)
			}
		})
	}
}

func TestUnknownCRCreateIsNeverRetriedByDuplicate(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-observed", true: "persisted"}[persist], func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := kovav1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
			writer := &uncertainBuildCreate{Client: base, persist: persist}
			servers, _ := dualQueueServers(t, 1, 1, [2]string{"alice", "alice"}, writer, base)
			first := httptest.NewRecorder()
			servers[0].routes().ServeHTTP(first, queueRequest(t, "same-key"))
			assertAPIError(t, first, http.StatusServiceUnavailable, apiv1.ErrorCodeQueueAdmissionPending, true)
			id := first.Header().Get("X-Kova-Build-ID")
			if id == "" {
				t.Fatal("unknown Create did not expose its intent ID")
			}
			second := httptest.NewRecorder()
			servers[1].routes().ServeHTTP(second, queueRequest(t, "same-key"))
			want := http.StatusServiceUnavailable
			if persist {
				want = http.StatusOK
			}
			if second.Code != want {
				t.Fatalf("retry status=%d want=%d body=%s", second.Code, want, second.Body.String())
			}
			if writer.creates != 1 {
				t.Fatalf("duplicate issued %d Create calls", writer.creates)
			}
			if !persist {
				get := httptest.NewRequest(http.MethodGet, "/v1/builds/"+id, nil)
				get.Header.Set("Authorization", "Bearer token")
				result := httptest.NewRecorder()
				servers[1].routes().ServeHTTP(result, get)
				assertAPIError(t, result, http.StatusServiceUnavailable, apiv1.ErrorCodeQueueAdmissionPending, true)
				full := httptest.NewRecorder()
				servers[1].routes().ServeHTTP(full, queueRequest(t, "another-key"))
				assertAPIError(t, full, http.StatusTooManyRequests, apiv1.ErrorCodeQueueCapacityExceeded, true)
			}
		})
	}
}

func TestConcurrentSameKeyRetryDoesNotIssueSecondCreate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	writer := &pausedBuildCreate{Client: base, entered: make(chan struct{}), release: make(chan struct{})}
	servers, _ := dualQueueServers(t, 1, 1, [2]string{"alice", "alice"}, writer, base)
	firstResult := make(chan *httptest.ResponseRecorder, 1)
	firstRequest := queueRequest(t, "same-key")
	go func() {
		result := httptest.NewRecorder()
		servers[0].routes().ServeHTTP(result, firstRequest)
		firstResult <- result
	}()
	select {
	case <-writer.entered:
	case result := <-firstResult:
		t.Fatalf("original Create never entered: status=%d body=%s", result.Code, result.Body.String())
	case <-time.After(5 * time.Second):
		t.Fatal("original Create did not enter")
	}
	second := httptest.NewRecorder()
	servers[1].routes().ServeHTTP(second, queueRequest(t, "same-key"))
	assertAPIError(t, second, http.StatusServiceUnavailable, apiv1.ErrorCodeQueueAdmissionPending, true)
	if writer.creates.Load() != 1 {
		t.Fatalf("same-key retry issued %d Create calls", writer.creates.Load())
	}
	close(writer.release)
	first := <-firstResult
	if first.Code != http.StatusAccepted {
		t.Fatalf("original Create status=%d body=%s", first.Code, first.Body.String())
	}
	third := httptest.NewRecorder()
	servers[1].routes().ServeHTTP(third, queueRequest(t, "same-key"))
	if third.Code != http.StatusOK || writer.creates.Load() != 1 {
		t.Fatalf("completed idempotent retry status=%d Create calls=%d", third.Code, writer.creates.Load())
	}
}

func TestIdempotentExistingBuildBypassesFullQueueWithoutNewCreate(t *testing.T) {
	servers, base := dualQueueServers(t, 1, 1, [2]string{"alice", "alice"}, nil, nil)
	first := httptest.NewRecorder()
	servers[0].routes().ServeHTTP(first, queueRequest(t, "same-key"))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := httptest.NewRecorder()
	servers[1].routes().ServeHTTP(second, queueRequest(t, "same-key"))
	if second.Code != http.StatusOK {
		t.Fatalf("idempotent retry status=%d body=%s", second.Code, second.Body.String())
	}
	conflicting := multipartBuildRequest(t, map[string]string{"idempotency_key": "same-key", "target": "registry.local/other:dev"})
	conflicting.Header.Set("Authorization", "Bearer token")
	conflictResult := httptest.NewRecorder()
	servers[1].routes().ServeHTTP(conflictResult, conflicting)
	assertAPIError(t, conflictResult, http.StatusConflict, apiv1.ErrorCodeConflict, false)
	full := httptest.NewRecorder()
	servers[1].routes().ServeHTTP(full, queueRequest(t, "different-key"))
	assertAPIError(t, full, http.StatusTooManyRequests, apiv1.ErrorCodeQueueCapacityExceeded, true)
	var builds kovav1.KovaBuildList
	if err := base.List(context.Background(), &builds); err != nil || len(builds.Items) != 1 {
		t.Fatalf("idempotent retries created %d CRs: %v", len(builds.Items), err)
	}
}
