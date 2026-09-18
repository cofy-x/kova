package v1alpha1

import (
	"github.com/cofy-x/kova/internal/buildcontract"

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

	PhaseQueued    = "Queued"
	PhaseStarting  = "Starting"
	PhaseRunning   = "Running"
	PhaseSucceeded = "Succeeded"
	PhaseFailed    = "Failed"
	PhaseCancelled = "Cancelled"
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
// +kubebuilder:validation:XValidation:rule="self.spec.targets.all(target, self.spec.targets.filter(candidate, candidate == target).size() == 1)",message="targets must be unique"
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
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=512
	// +kubebuilder:validation:items:Pattern=`^[^[:space:]@]+:[^[:space:]@/]+$`
	Targets []string `json:"targets"`
	// +kubebuilder:validation:Required
	Source KovaBuildSourceSpec `json:"source,omitempty"`
	Build  KovaBuildOptions    `json:"build,omitempty"`
	// +kubebuilder:validation:MaxLength=256
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
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
	// +kubebuilder:validation:Enum=Queued;Starting;Running;Succeeded;Failed;Cancelled
	Phase                string `json:"phase,omitempty"`
	ObservedGeneration   int64  `json:"observedGeneration,omitempty"`
	AllocatedConcurrency int32  `json:"allocatedConcurrency,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	RunnerPodName string `json:"runnerPodName,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	Message    string       `json:"message,omitempty"`
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// +kubebuilder:validation:MaxItems=200
	Outputs []BuildOutput `json:"outputs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=1
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

type BuildOutput struct {
	// +kubebuilder:validation:Enum=oci;nydus
	Format string `json:"format"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	ManifestDigest string `json:"manifestDigest"`
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
