package service

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
	ctx := t.Context()
	if err := genesisOldWorkVeto(ctx, target.Client, target.Namespace); err != nil {
		t.Fatalf("dedicated test Namespace is not empty of old work: %v", err)
	}
	for _, name := range []string{admissiongenesis.GenesisName, admissiongenesis.ActiveLedgerName, admissiongenesis.QueueLedgerName} {
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
		WorkerSlots: 2, MaxQueuedJobs: 10, MaxQueuedJobsPerRequester: 2}
	spec := admissiongenesis.InstallationSpec{Namespace: target.Namespace,
		NamespaceUID: os.Getenv("KOVA_GENESIS_REAL_API_NAMESPACE_UID"), Generation: hex.EncodeToString(nonce[:]),
		Limits: admissiongenesis.Limits{MaxActiveJobs: cfg.MaxActiveJobs,
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
	if err := os.WriteFile(path, secret.Data[admissiongenesis.ReceiptSecretDataKey], 0600); err != nil {
		t.Fatal(err)
	}
	options := genesisReceiptOptions{File: path, SecretNamespace: target.Namespace,
		SecretName: secretName, SecretUID: string(secret.UID)}
	receiptRaw, checkSecret, err := options.loadAndCheck(ctx, direct)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, direct, target.Client, checkSecret)
	if err != nil {
		t.Fatalf("real API could not commit original Genesis: %v; leave Namespace for inspection", err)
	}
	if err := guard.Check(ctx); err != nil {
		t.Fatal(err)
	}
	committed, err := direct.GetConfigMap(ctx, target.Namespace, genesis.Name)
	if err != nil || committed.UID != genesis.UID || committed.Immutable == nil || !*committed.Immutable {
		t.Fatalf("original Genesis was not immutably committed: %v, %+v", err, committed)
	}
	active, err := direct.GetConfigMap(ctx, target.Namespace, admissiongenesis.ActiveLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := direct.GetConfigMap(ctx, target.Namespace, admissiongenesis.QueueLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	if string(active.UID) != guard.Original.ActiveLedgerUID || string(queue.UID) != guard.Original.QueueLedgerUID {
		t.Fatal("committed ledger UIDs differ from the original Guard binding")
	}
	// Exercise the actual API server's JSON Patch UID/RV/data CAS, without a
	// build, Pod, registry, or queue intent.
	proposed := strings.Replace(active.Data[admissiongenesis.ActiveLedgerDataKey], `"fence":0`, `"fence":1`, 1)
	if proposed == active.Data[admissiongenesis.ActiveLedgerDataKey] {
		t.Fatal("active ledger fixture lacks a fence to advance")
	}
	target.Check(t)
	if err := guard.PatchLedgerData(ctx, active, admissiongenesis.Active, proposed); err != nil {
		t.Fatalf("real API conditional active-ledger Patch failed: %v", err)
	}
	active, err = direct.GetConfigMap(ctx, target.Namespace, active.Name)
	if err != nil || active.Data[admissiongenesis.ActiveLedgerDataKey] != proposed || string(active.UID) != guard.Original.ActiveLedgerUID {
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
		PodCreateAttempt: strings.Repeat("b", 32), RunnerRequestID: "synthetic-request"}
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
