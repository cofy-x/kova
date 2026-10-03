package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestAdmissionGenesisCLIRequiresExplicitTrustInputsBeforeAPI(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "no explicit global namespace", args: []string{
			"kova", "--kubeconfig", "unused", "admission-genesis", "render-genesis",
		}, want: "explicit global --namespace and --kubeconfig"},
		{name: "no expected namespace UID", args: []string{
			"kova", "--kubeconfig", "unused", "--namespace", "jobs-57", "admission-genesis", "render-genesis",
		}, want: "explicit --namespace-uid"},
		{name: "no expected Genesis UID", args: []string{
			"kova", "--kubeconfig", "unused", "--namespace", "jobs-57", "admission-genesis", "export-receipt-secret",
		}, want: "explicit --genesis-uid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewCLIApp().Run(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected refusal %q before API access, got %v", tc.want, err)
			}
		})
	}
}

func TestAdmissionGenesisCLIUsesExplicitGlobalFlagsAndGETOnlyAPI(t *testing.T) {
	receipt := admissioncontract.Receipt{
		Namespace: "jobs-57", GenesisName: admissioncontract.GenesisName, GenesisUID: "genesis-original",
		Contract: admissioncontract.Contract{
			Version: 3, NamespaceUID: "namespace-original", ReceiptNamespace: "receipts-57",
			ReceiptNamespaceUID: "receipts-original", Generation: strings.Repeat("a", 32),
			WorkerPoolID: "worker-pool-original", RunnerImage: "example.com/kova/runner@sha256:" + strings.Repeat("a", 64),
			ActiveLedgerName: admissioncontract.ActiveLedgerName, ActiveLedgerSchema: 2,
			QueueLedgerName: admissioncontract.QueueLedgerName, QueueLedgerSchema: 2,
			Limits: admissioncontract.Limits{MaxActiveJobs: 20, MaxActiveJobsPerRequester: 4,
				WorkerSlots: 20, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100},
		},
	}
	state, err := json.Marshal(admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseInitializing})
	if err != nil {
		t.Fatal(err)
	}
	genesis := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: receipt.Namespace, Name: receipt.GenesisName, UID: types.UID(receipt.GenesisUID), ResourceVersion: "1",
	}, Data: map[string]string{admissioncontract.GenesisDataKey: string(state)}}
	exists := false
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/jobs-57":
			_ = json.NewEncoder(w).Encode(corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
				Name: receipt.Namespace, UID: types.UID(receipt.Contract.NamespaceUID),
			}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/receipts-57":
			_ = json.NewEncoder(w).Encode(corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
				Name: receipt.Contract.ReceiptNamespace, UID: types.UID(receipt.Contract.ReceiptNamespaceUID),
			}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/jobs-57/configmaps/"+receipt.GenesisName:
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, receipt.GenesisName).Status())
				return
			}
			_ = json.NewEncoder(w).Encode(genesis)
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()
	kubeconfig := filepath.Join(t.TempDir(), "config")
	configuration := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: server.URL}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	}
	if err := clientcmd.WriteToFile(configuration, kubeconfig); err != nil {
		t.Fatal(err)
	}
	common := []string{"kova", "--kubeconfig", kubeconfig, "--namespace", receipt.Namespace,
		"admission-genesis"}
	identity := []string{"--namespace-uid", receipt.Contract.NamespaceUID, "--generation", receipt.Contract.Generation,
		"--receipt-namespace", receipt.Contract.ReceiptNamespace,
		"--receipt-namespace-uid", receipt.Contract.ReceiptNamespaceUID,
		"--worker-pool-id", receipt.Contract.WorkerPoolID, "--runner-image", receipt.Contract.RunnerImage,
		"--max-active-jobs", "20", "--max-active-jobs-per-requester", "4", "--worker-slots", "20",
		"--max-queued-jobs", "1000", "--max-queued-jobs-per-requester", "100"}
	app := NewCLIApp()
	var output bytes.Buffer
	app.Writer = &output
	app.ErrWriter = &output
	args := append(append(append([]string{}, common...), "render-genesis"), identity...)
	if err := app.Run(args); err != nil {
		t.Fatalf("render: %v", err)
	}
	var rendered corev1.ConfigMap
	if err := json.Unmarshal(output.Bytes(), &rendered); err != nil || rendered.Name != receipt.GenesisName {
		t.Fatalf("invalid rendered Genesis: %v, %s", err, output.String())
	}
	exists = true
	output.Reset()
	args = append(append(append([]string{}, common...), "export-receipt-secret"), identity...)
	args = append(args, "--genesis-uid", receipt.GenesisUID, "--service-namespace", "service-57", "--secret-name", "receipt-57")
	if err := app.Run(args); err != nil {
		t.Fatalf("export: %v", err)
	}
	var secret corev1.Secret
	if err := json.Unmarshal(output.Bytes(), &secret); err != nil || secret.Name != "receipt-57" || secret.Immutable == nil || !*secret.Immutable {
		t.Fatalf("invalid rendered receipt Secret: %v, %s", err, output.String())
	}
	if len(calls) != 6 || calls[0] != "GET /api/v1/namespaces/jobs-57" ||
		calls[1] != "GET /api/v1/namespaces/receipts-57" ||
		calls[2] != "GET /api/v1/namespaces/jobs-57/configmaps/"+receipt.GenesisName ||
		calls[3] != calls[0] || calls[4] != calls[1] || calls[5] != calls[2] {
		t.Fatalf("helper issued unexpected API calls: %v", calls)
	}
}
