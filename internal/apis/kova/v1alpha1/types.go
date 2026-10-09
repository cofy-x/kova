package v1alpha1

import (
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/buildobservation"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	MaxLogicalTargets   = buildcontract.MaxLogicalTargets
	MaxConcreteOutputs  = buildcontract.MaxConcreteOutputs
	MaxBuildConcurrency = buildcontract.MaxBuildConcurrency
)

const (
	Group   = "kova.cofy.dev"
	Version = "v1alpha1"

	CancellationRequestedAnnotation = "kova.cofy.dev/cancellation-requested-at"
	CleanupFinalizer                = "kova.cofy.dev/cleanup"

	PhaseQueued    = "Queued"
	PhaseStarting  = "Starting"
	PhaseRunning   = "Running"
	PhaseVerifying = "Verifying"
	// PhaseFailedVerifying preserves a runner's failed outcome while bounded
	// partial-output receipts are still being verified. Older controllers do
	// not recognize this phase and therefore cannot turn it into Succeeded.
	PhaseFailedVerifying = "FailedVerifying"
	PhaseSucceeded       = "Succeeded"
	PhaseFailed          = "Failed"
	PhaseCancelled       = "Cancelled"
)

var SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=kb
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Runner",type=string,JSONPath=`.status.runnerPodName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="spec is immutable"
// +kubebuilder:validation:XValidation:rule="self.spec.build.concurrency == 0 || self.spec.build.concurrency <= size(self.spec.targets)",message="concurrency must not exceed the logical target count"
// +kubebuilder:validation:XValidation:rule="self.spec.targets.all(target, self.spec.targets.filter(candidate, candidate.target == target.target).size() == 1)",message="targets must be unique"
// +kubebuilder:validation:XValidation:rule="self.spec.targets.all(target, !target.target.endsWith('_nydus_v3'))",message="target tag suffix _nydus_v3 is reserved for Kova Nydus outputs"
type KovaBuild struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec KovaBuildSpec `json:"spec,omitempty"`
	// +kubebuilder:validation:Optional
	Status KovaBuildStatus `json:"status,omitempty"`
}

type KovaBuildSpec struct {
	// +kubebuilder:validation:Required
	Requester KovaBuildRequester `json:"requester"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	Targets []KovaBuildTargetSpec `json:"targets"`
	// +kubebuilder:validation:Required
	Source KovaBuildSourceSpec `json:"source,omitempty"`
	Build  KovaBuildOptions    `json:"build,omitempty"`
	// +kubebuilder:validation:MaxLength=256
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type KovaBuildTargetSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[^[:space:]@]+:[^[:space:]@/]+$`
	Target string `json:"target"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=linux/amd64;linux/arm64
	Platform string `json:"platform"`
}

type KovaBuildRequester struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Username string `json:"username"`
	// +kubebuilder:validation:MaxLength=253
	UID string `json:"uid,omitempty"`
}

type KovaBuildSourceSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^(oci|https)://.+$`
	URI string `json:"uri"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest"`
}

type KovaBuildOptions struct {
	// +kubebuilder:default=oci
	// +kubebuilder:validation:Enum=oci;nydus;both
	Format string `json:"format,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	Concurrency int `json:"concurrency,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Timeout     int    `json:"timeout,omitempty"`
	OOMCooldown string `json:"oomCooldown,omitempty"`
	FailFast    bool   `json:"failFast,omitempty"`
	Verbose     bool   `json:"verbose,omitempty"`
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:MaxLength=2048
	Vars []string `json:"vars,omitempty"`
}

type KovaBuildStatus struct {
	// +kubebuilder:validation:Enum=Queued;Starting;Running;Verifying;FailedVerifying;Succeeded;Failed;Cancelled
	Phase                string `json:"phase,omitempty"`
	ObservedGeneration   int64  `json:"observedGeneration,omitempty"`
	AllocatedConcurrency int32  `json:"allocatedConcurrency,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	RunnerPodName string `json:"runnerPodName,omitempty"`
	// AdmissionGenesisWitness is recorded only for a runner created under an
	// externally committed admission Genesis. It binds a directly observed Pod
	// UID and the runner request to the original CR and ledger installation.
	AdmissionGenesisWitness *AdmissionGenesisWitness `json:"admissionGenesisWitness,omitempty"`
	// AdmissionGenesisStopIntent is durably recorded before a UID-scoped
	// forced stop. An absent Pod alone can never manufacture this intent.
	AdmissionGenesisStopIntent *AdmissionGenesisStopIntent `json:"admissionGenesisStopIntent,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	Message                   string       `json:"message,omitempty"`
	StartedAt                 *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt                *metav1.Time `json:"finishedAt,omitempty"`
	PollFailureSince          *metav1.Time `json:"pollFailureSince,omitempty"`
	PollFailureCount          int32        `json:"pollFailureCount,omitempty"`
	VerificationStartedAt     *metav1.Time `json:"verificationStartedAt,omitempty"`
	VerificationDeadlineAt    *metav1.Time `json:"verificationDeadlineAt,omitempty"`
	VerificationNextAttemptAt *metav1.Time `json:"verificationNextAttemptAt,omitempty"`
	VerificationAttempts      int32        `json:"verificationAttempts,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	VerificationLastError string `json:"verificationLastError,omitempty"`
	// +kubebuilder:validation:MaxItems=200
	VerificationResults []BuildVerificationResult `json:"verificationResults,omitempty"`
	// +kubebuilder:validation:MaxItems=200
	Outputs []BuildOutput `json:"outputs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=2
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// AdmissionGenesisWitness is durable, independent evidence for a runner that
// may continue to report an accepted build after an admission ledger is lost.
// Its fields are copied from the pre-Create Pod stamps and a direct Pod read;
// status alone never authorizes a new build submission or cleanup.
type AdmissionGenesisWitness struct {
	// +kubebuilder:validation:MaxLength=256
	NamespaceUID string `json:"namespaceUID"`
	// +kubebuilder:validation:MaxLength=256
	GenesisUID string `json:"genesisUID"`
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{32}$`
	Generation string `json:"generation"`
	// +kubebuilder:validation:MaxLength=256
	ActiveLedgerUID string `json:"activeLedgerUID"`
	// +kubebuilder:validation:MaxLength=256
	QueueLedgerUID string `json:"queueLedgerUID"`
	// +kubebuilder:validation:MaxLength=256
	BuildUID string `json:"buildUID"`
	// +kubebuilder:validation:MaxLength=253
	PodName string `json:"podName"`
	// +kubebuilder:validation:MaxLength=256
	PodUID string `json:"podUID"`
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{32}$`
	PodCreateAttempt string `json:"podCreateAttempt"`
	// PodTemplateDigest was recorded only after the original Pod's immutable
	// pre-Create receipt was qualified. It lets evidence-only observation after
	// ledger loss reject a changed execution template without those ledgers.
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	PodTemplateDigest string `json:"podTemplateDigest"`
	// +kubebuilder:validation:MaxLength=256
	RunnerRequestID string `json:"runnerRequestID"`
}

// AdmissionGenesisStopIntent binds an explicit forced-stop decision to the
// independently witnessed original runner. It is written only after the
// original admission pair and active charge are directly requalified.
type AdmissionGenesisStopIntent struct {
	// +kubebuilder:validation:MaxLength=256
	BuildUID string `json:"buildUID"`
	// +kubebuilder:validation:MaxLength=256
	PodUID string `json:"podUID"`
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{32}$`
	PodCreateAttempt string `json:"podCreateAttempt"`
	// +kubebuilder:validation:MaxLength=256
	RunnerRequestID string `json:"runnerRequestID"`
	// +kubebuilder:validation:Enum=Cancelled;Deleted;BuildTimedOut
	Reason string `json:"reason"`
}

type BuildOutput struct {
	// +kubebuilder:validation:Enum=oci;nydus
	Format string `json:"format"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	ManifestDigest string `json:"manifestDigest"`
	// +kubebuilder:validation:Enum=linux/amd64;linux/arm64
	Platform string `json:"platform"`
}

// BuildVerificationResult is a bounded, durable receipt for one concrete output.
// PushedDigest is evidence from the runner's exact push, not a mutable tag lookup.
type BuildVerificationResult struct {
	// +kubebuilder:validation:Enum=oci;nydus
	Format string `json:"format"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:Enum=linux/amd64;linux/arm64
	Platform string `json:"platform"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	PushedDigest string `json:"pushedDigest,omitempty"`
	// +kubebuilder:validation:Enum=pending;succeeded;failed
	State string `json:"state"`
	// +kubebuilder:validation:MaxLength=2048
	Error string `json:"error,omitempty"`
	// BuildObservation is optional, bounded diagnosis retained with this exact
	// output. Missing/invalid observations never decide artifact correctness.
	BuildObservation *buildobservation.Observation `json:"buildObservation,omitempty"`
}

// +kubebuilder:object:root=true
type KovaBuildList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KovaBuild `json:"items"`
}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &KovaBuild{}, &KovaBuildList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
