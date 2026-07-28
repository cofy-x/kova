package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "kova.cofy.dev"
	Version = "v1alpha1"

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
	Username string `json:"username"`
	UID      string `json:"uid,omitempty"`
}

type KovaBuildSourceSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(file|s3)://.+$`
	URI string `json:"uri"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest"`
}

type KovaBuildOptions struct {
	// +kubebuilder:validation:Enum=oci;nydus;both
	Format string `json:"format,omitempty"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Target string `json:"target,omitempty"`
	// +kubebuilder:validation:Minimum=1
	Concurrency int `json:"concurrency,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Timeout int `json:"timeout,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Retry       int      `json:"retry,omitempty"`
	OOMCooldown string   `json:"oomCooldown,omitempty"`
	FailFast    bool     `json:"failFast,omitempty"`
	SkipFail    bool     `json:"skipFail,omitempty"`
	Verbose     bool     `json:"verbose,omitempty"`
	Vars        []string `json:"vars,omitempty"`
}

type KovaBuildStatus struct {
	// +kubebuilder:validation:Enum=Queued;Starting;Running;Succeeded;Failed;Cancelled
	Phase                string             `json:"phase,omitempty"`
	ObservedGeneration   int64              `json:"observedGeneration,omitempty"`
	AllocatedConcurrency int32              `json:"allocatedConcurrency,omitempty"`
	RunnerPodName        string             `json:"runnerPodName,omitempty"`
	Message              string             `json:"message,omitempty"`
	StartedAt            *metav1.Time       `json:"startedAt,omitempty"`
	FinishedAt           *metav1.Time       `json:"finishedAt,omitempty"`
	ResultSummary        BuildResultSummary `json:"resultSummary,omitempty"`
	ResultArtifactURI    string             `json:"resultArtifactURI,omitempty"`
	// +kubebuilder:validation:MaxItems=100
	Results []BuildResult `json:"results,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

type BuildResultSummary struct {
	Total     int32 `json:"total,omitempty"`
	Succeeded int32 `json:"succeeded,omitempty"`
	Failed    int32 `json:"failed,omitempty"`
}

type BuildResult struct {
	Format         string `json:"format"`
	Status         string `json:"status"`
	Repository     string `json:"repository"`
	ManifestDigest string `json:"manifestDigest,omitempty"`
	MediaType      string `json:"mediaType,omitempty"`
	Size           int64  `json:"size,omitempty"`
	Error          string `json:"error,omitempty"`
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
