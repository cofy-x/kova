package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/realapitest"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errGenesisAPILostResponse = errors.New("test-only lost active-ledger Create response")
	errGenesisAPIInterrupted  = errors.New("test-only interruption before queue-ledger Create")
)

// The API server persists the first ledger, but the bootstrap caller sees no
// Create response. Its direct named read must resolve that single attempt.
type lostActiveCreateResponse struct {
	admissiongenesis.CoreAPI
	activeCreateAttempts int
	activeUID            types.UID
}

func (c *lostActiveCreateResponse) CreateConfigMap(ctx context.Context, namespace string, request *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	if request.Name != admissioncontract.ActiveLedgerName {
		return c.CoreAPI.CreateConfigMap(ctx, namespace, request)
	}
	c.activeCreateAttempts++
	created, err := c.CoreAPI.CreateConfigMap(ctx, namespace, request)
	if err != nil || created == nil || created.UID == "" {
		return created, err
	}
	c.activeUID = created.UID
	return nil, errGenesisAPILostResponse
}

// This opt-in gate uses only a supplied dedicated Kind API/Namespace and
// synthetic CR status. It creates no Namespace, cluster, registry, Pod, or
// runner and cannot be a substitute for operator stopped-writer evidence.
// Any unknown/failed mutation leaves the named resources for inspection.
func TestRealAPIGenesisBootstrapAndStatusRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	target := realapitest.OpenExistingGenesis(t, scheme)
	receiptNamespace := os.Getenv("KOVA_GENESIS_REAL_API_RECEIPT_NAMESPACE")
	receiptNamespaceUID := os.Getenv("KOVA_GENESIS_REAL_API_RECEIPT_NAMESPACE_UID")
	if receiptNamespace == "" || receiptNamespaceUID == "" {
		t.Skip("dedicated receipt Namespace name and original UID are required for this opt-in API test")
	}
	if receiptNamespace == target.Namespace {
		t.Fatal("receipt Namespace must be separate from runner Namespace")
	}
	ctx := t.Context()
	if err := genesisOldWorkVeto(ctx, target.Client, target.Namespace); err != nil {
		t.Fatalf("dedicated test Namespace is not empty of old work: %v", err)
	}
	for _, name := range []string{admissioncontract.GenesisName, admissioncontract.ActiveLedgerName, admissioncontract.QueueLedgerName} {
		var existing corev1.ConfigMap
		if err := target.Client.Get(ctx, client.ObjectKey{Namespace: target.Namespace, Name: name}, &existing); !apierrors.IsNotFound(err) {
			t.Fatalf("refusing to adopt existing ConfigMap %s: %v", name, err)
		}
	}
	const secretName = "kova-genesis-api-receipt"
	var existingSecret corev1.Secret
	if err := target.Client.Get(ctx, client.ObjectKey{Namespace: target.Namespace, Name: secretName}, &existingSecret); !apierrors.IsNotFound(err) {
		t.Fatalf("refusing to adopt existing receipt Secret: %v", err)
	}

	restConfig := singleAttemptWrites(target.RESTConfig(t))
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	direct := admissiongenesis.DirectClient{Client: clientset}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Namespace: target.Namespace, MaxActiveJobs: 2, MaxActiveJobsPerRequester: 1,
		WorkerSlots: 2, MaxQueuedJobs: 10, MaxQueuedJobsPerRequester: 2,
		WorkerPoolID:      "worker-pool-" + hex.EncodeToString(nonce[:]),
		RunnerImage:       "example.com/kova/runner@sha256:" + strings.Repeat("a", 64),
		RunnerImageDigest: "sha256:" + strings.Repeat("a", 64)}
	spec := admissioncontract.InstallationSpec{Namespace: target.Namespace,
		NamespaceUID: os.Getenv("KOVA_GENESIS_REAL_API_NAMESPACE_UID"), Generation: hex.EncodeToString(nonce[:]),
		ReceiptNamespace: receiptNamespace, ReceiptNamespaceUID: receiptNamespaceUID,
		WorkerPoolID: cfg.WorkerPoolID, RunnerImage: cfg.RunnerImage,
		Limits: admissioncontract.Limits{MaxActiveJobs: cfg.MaxActiveJobs,
			MaxActiveJobsPerRequester: cfg.MaxActiveJobsPerRequester, WorkerSlots: cfg.WorkerSlots,
			MaxQueuedJobs: cfg.MaxQueuedJobs, MaxQueuedJobsPerRequester: cfg.MaxQueuedJobsPerRequester}}
	genesisManifest, err := spec.RenderInitializingGenesis(ctx, direct)
	if err != nil {
		t.Fatal(err)
	}
	target.Check(t)
	genesis, err := direct.CreateConfigMap(ctx, target.Namespace, genesisManifest)
	if err != nil || genesis == nil || genesis.UID == "" {
		t.Fatalf("create-only Genesis result is uncertain; leave Namespace for inspection: %v", err)
	}
	receipt, err := spec.ReceiptFor(string(genesis.UID))
	if err != nil {
		t.Fatal(err)
	}
	secretManifest, err := receipt.ExportReceiptSecret(ctx, direct, target.Namespace, secretName)
	if err != nil {
		t.Fatal(err)
	}
	target.Check(t)
	secret, err := clientset.CoreV1().Secrets(target.Namespace).Create(ctx, secretManifest, metav1.CreateOptions{})
	if err != nil || secret == nil || secret.UID == "" {
		t.Fatalf("create-only receipt Secret result is uncertain; leave Namespace for inspection: %v", err)
	}
	path := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(path, secret.Data[admissioncontract.ReceiptSecretDataKey], 0600); err != nil {
		t.Fatal(err)
	}
	options := genesisReceiptOptions{File: path, SecretNamespace: target.Namespace,
		SecretName: secretName, SecretUID: string(secret.UID)}
	receiptRaw, checkSecret, err := options.loadAndCheck(ctx, direct)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a lost HTTP response after the first ledger really persisted,
	// then stop at the next ledger's pre-effect gate. A fresh runtime must
	// converge from the original provisional pin, not create a replacement.
	lostResponse := &lostActiveCreateResponse{CoreAPI: direct}
	effects := 0
	interruptBeforeQueue := func(ctx context.Context) error {
		if err := checkSecret(ctx); err != nil {
			return err
		}
		effects++
		if effects == 3 {
			return errGenesisAPIInterrupted
		}
		return nil
	}
	target.Check(t)
	if _, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, lostResponse, target.Client, interruptBeforeQueue); !errors.Is(err, errGenesisAPIInterrupted) {
		t.Fatalf("bootstrap did not stop before second ledger Create: %v; leave Namespace for inspection", err)
	}
	if effects != 3 || lostResponse.activeCreateAttempts != 1 || lostResponse.activeUID == "" {
		t.Fatalf("lost-response/stop fixture did not reach exactly one active Create: effects=%d creates=%d UID=%s", effects, lostResponse.activeCreateAttempts, lostResponse.activeUID)
	}
	partialGenesis, err := direct.GetConfigMap(ctx, target.Namespace, genesis.Name)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := receipt.QualifyGenesis(partialGenesis)
	if err != nil || partial.Phase != admissioncontract.PhaseInitializing || partial.ActiveLedgerUID != string(lostResponse.activeUID) || partial.QueueLedgerUID != "" {
		t.Fatalf("original Genesis did not retain only the provisional active pin: %v, %+v", err, partial)
	}
	if _, err := direct.GetConfigMap(ctx, target.Namespace, admissioncontract.QueueLedgerName); !apierrors.IsNotFound(err) {
		t.Fatalf("queue ledger was written before interruption: %v", err)
	}
	partialActive, err := direct.GetConfigMap(ctx, target.Namespace, admissioncontract.ActiveLedgerName)
	if err != nil || partialActive.UID != lostResponse.activeUID {
		t.Fatalf("lost active Create response did not resolve to the original UID: %v, %+v", err, partialActive)
	}
	target.Check(t)
	guard, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, direct, target.Client, checkSecret)
	if err != nil {
		t.Fatalf("real API could not recover and commit original Genesis: %v; leave Namespace for inspection", err)
	}
	if guard.Original.ActiveLedgerUID != string(lostResponse.activeUID) {
		t.Fatal("restart replaced the active ledger after a lost Create response")
	}
	if err := guard.Check(ctx); err != nil {
		t.Fatal(err)
	}
	committed, err := direct.GetConfigMap(ctx, target.Namespace, genesis.Name)
	if err != nil || committed.UID != genesis.UID || committed.Immutable == nil || !*committed.Immutable {
		t.Fatalf("original Genesis was not immutably committed: %v, %+v", err, committed)
	}
	active, err := direct.GetConfigMap(ctx, target.Namespace, admissioncontract.ActiveLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := direct.GetConfigMap(ctx, target.Namespace, admissioncontract.QueueLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	if string(active.UID) != guard.Original.ActiveLedgerUID || string(queue.UID) != guard.Original.QueueLedgerUID {
		t.Fatal("committed ledger UIDs differ from the original Guard binding")
	}
	// Exercise the actual API server's JSON Patch UID/RV/data CAS, without a
	// build, Pod, registry, or queue intent.
	proposed := strings.Replace(active.Data[admissioncontract.ActiveLedgerDataKey], `"fence":1`, `"fence":2`, 1)
	if proposed == active.Data[admissioncontract.ActiveLedgerDataKey] {
		t.Fatal("active ledger fixture lacks a fence to advance")
	}
	target.Check(t)
	if err := guard.PatchLedgerData(ctx, active, admissioncontract.Active, proposed); err != nil {
		t.Fatalf("real API conditional active-ledger Patch failed: %v", err)
	}
	active, err = direct.GetConfigMap(ctx, target.Namespace, active.Name)
	if err != nil || active.Data[admissioncontract.ActiveLedgerDataKey] != proposed || string(active.UID) != guard.Original.ActiveLedgerUID {
		t.Fatalf("conditional Patch did not retain original ledger identity: %v", err)
	}

	// The serving CRD, not merely its stored schema, must persist both new
	// status witnesses. The synthetic Pod UID is never used to perform work.
	writeClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: target.Namespace, Name: "kova-genesis-api-status"},
		Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "genesis-api-probe"},
			Targets: []kovav1.KovaBuildTargetSpec{{Target: "registry.invalid/example:probe", Platform: "linux/amd64"}},
			Source: kovav1.KovaBuildSourceSpec{URI: "oci://registry.invalid/source@sha256:" + strings.Repeat("a", 64),
				Digest: "sha256:" + strings.Repeat("a", 64)},
			Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1}}}
	target.Check(t)
	if err := writeClient.Create(ctx, build); err != nil || build.UID == "" {
		t.Fatalf("synthetic CR Create result is uncertain; leave Namespace for inspection: %v", err)
	}
	witness := &kovav1.AdmissionGenesisWitness{NamespaceUID: receipt.Contract.NamespaceUID,
		GenesisUID: receipt.GenesisUID, Generation: receipt.Contract.Generation,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID), BuildUID: string(build.UID),
		PodName: "kova-job-genesis-api-status", PodUID: "synthetic-pod-uid",
		PodTemplateDigest: "sha256:" + strings.Repeat("c", 64),
		PodCreateAttempt:  strings.Repeat("b", 32), RunnerRequestID: "synthetic-request"}
	stop := &kovav1.AdmissionGenesisStopIntent{BuildUID: string(build.UID), PodUID: witness.PodUID,
		PodCreateAttempt: witness.PodCreateAttempt, RunnerRequestID: witness.RunnerRequestID, Reason: "Deleted"}
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.RunnerPodName = witness.PodName
	build.Status.AdmissionGenesisWitness = witness
	build.Status.AdmissionGenesisStopIntent = stop
	target.Check(t)
	if err := writeClient.Status().Update(ctx, build); err != nil {
		t.Fatalf("status witness write failed; leave Namespace for inspection: %v", err)
	}
	var observed kovav1.KovaBuild
	if err := target.Client.Get(ctx, client.ObjectKeyFromObject(build), &observed); err != nil ||
		observed.UID != build.UID || !reflect.DeepEqual(observed.Status.AdmissionGenesisWitness, witness) ||
		!reflect.DeepEqual(observed.Status.AdmissionGenesisStopIntent, stop) {
		t.Fatalf("serving CRD pruned Genesis status witnesses: %v, %+v", err, observed.Status)
	}

	// An original ledger loss must refuse authority. Exact-UID cleanup then
	// removes only this gate's known successful writes; Namespace is retained.
	target.DeleteOwned(t, queue)
	if err := guard.Check(ctx); err == nil {
		t.Fatal("committed queue loss retained admission authority")
	}
	target.DeleteOwned(t, build)
	target.DeleteOwned(t, secret)
	target.DeleteOwned(t, active)
	target.DeleteOwned(t, genesis)
	t.Logf("PASS original Namespace UID=%s Genesis UID=%s receipt UID=%s", types.UID(spec.NamespaceUID), genesis.UID, secret.UID)
}
