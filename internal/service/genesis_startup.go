package service

import (
	"context"
	"fmt"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// prepareGenesisRuntime is intentionally not called by CLICommand yet. The
// legacy HTTP/controller paths remain the only routable mode until every
// side-effect and evidence-only gate is wired and reviewed together.
func prepareGenesisRuntime(ctx context.Context, cfg config.Config, receiptRaw []byte,
	api admissiongenesis.CoreAPI, directReader client.Reader) (*admissiongenesis.Guard, error) {
	receipt, err := admissiongenesis.ParseReceipt(receiptRaw)
	if err != nil {
		return nil, err
	}
	if err := validateGenesisRuntimeConfig(cfg, receipt); err != nil {
		return nil, err
	}
	if directReader == nil {
		return nil, fmt.Errorf("admission Genesis requires a direct CR/Pod reader")
	}
	activeEmpty, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		return nil, err
	}
	queueEmpty, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		return nil, err
	}
	bootstrap := admissiongenesis.Bootstrapper{
		API: api, Receipt: receipt,
		Active: admissiongenesis.LedgerTemplate{Role: admissiongenesis.Active,
			DataKey: admissiongenesis.ActiveLedgerDataKey, EmptyData: activeEmpty,
			Validate: func(cm *corev1.ConfigMap) error { return buildcontroller.ValidateAdmissionLedgerForGenesis(cm, cfg) }},
		Queue: admissiongenesis.LedgerTemplate{Role: admissiongenesis.Queue,
			DataKey: admissiongenesis.QueueLedgerDataKey, EmptyData: queueEmpty,
			Validate: func(cm *corev1.ConfigMap) error {
				return queueadmission.ValidateQueueLedgerForGenesis(cm, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
			}},
		Preflight: func(ctx context.Context) error { return genesisOldWorkVeto(ctx, directReader, receipt.Namespace) },
	}
	binding, err := bootstrap.EnsureFresh(ctx)
	if err != nil {
		return nil, err
	}
	return admissiongenesis.NewGuard(ctx, bootstrap, binding)
}

func validateGenesisRuntimeConfig(cfg config.Config, receipt admissiongenesis.Receipt) error {
	limits := receipt.Contract.Limits
	if cfg.Namespace != receipt.Namespace ||
		cfg.MaxActiveJobs != limits.MaxActiveJobs ||
		cfg.MaxActiveJobsPerRequester != limits.MaxActiveJobsPerRequester ||
		cfg.WorkerSlots != limits.WorkerSlots ||
		cfg.MaxQueuedJobs != limits.MaxQueuedJobs ||
		cfg.MaxQueuedJobsPerRequester != limits.MaxQueuedJobsPerRequester {
		return fmt.Errorf("admission Genesis receipt differs from runtime namespace or capacity configuration")
	}
	if _, collision := cfg.RunnerEnv[daemonclient.RunnerPodUIDEnv]; collision {
		return fmt.Errorf("admission Genesis runner environment collides with reserved Pod UID fence")
	}
	return nil
}

// A List can veto visible old work, never prove quiescence. The installation
// contract separately requires every old writer stopped before provisioning.
func genesisOldWorkVeto(ctx context.Context, reader client.Reader, namespace string) error {
	var builds kovav1.KovaBuildList
	if err := reader.List(ctx, &builds, client.InNamespace(namespace), client.Limit(1)); err != nil {
		return err
	}
	if len(builds.Items) != 0 {
		return fmt.Errorf("admission Genesis initialization sees a pre-existing KovaBuild in %s", namespace)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels["kova.cofy.dev/build-id"] != "" || strings.HasPrefix(pod.Name, "kova-job-") {
			return fmt.Errorf("admission Genesis initialization sees a pre-existing runner Pod %s/%s", namespace, pod.Name)
		}
		for _, owner := range pod.OwnerReferences {
			if owner.APIVersion == kovav1.Group+"/"+kovav1.Version && owner.Kind == "KovaBuild" {
				return fmt.Errorf("admission Genesis initialization sees a pre-existing KovaBuild-owned Pod %s/%s", namespace, pod.Name)
			}
		}
	}
	return nil
}
