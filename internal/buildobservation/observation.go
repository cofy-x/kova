// Package buildobservation defines optional, finite observations, not build
// correctness receipts or accepted/active BuildKit worker-session counters.
package buildobservation

import (
	"encoding/json"
)

const (
	MaxVertices          = 4096
	MaxIntervals         = 16384
	MaxSeconds           = 86400
	MaxNanoseconds int64 = MaxSeconds * 1000000000
)

// Observation contains only fixed vocabulary and bounded scalar values.
// Vertex/export/push unions can overlap and are never additive execution stages.
type Observation struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	SchemaVersion int32 `json:"schemaVersion"`
	// +kubebuilder:validation:Enum=observed;incomplete;unavailable
	Availability string `json:"availability"`
	// +kubebuilder:validation:Enum=missing_stream;malformed_stream;limit_exceeded;partial_stream;command_failed;cancelled;invalid_observation
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4096
	VertexCount int32 `json:"vertexCount,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16384
	VertexIntervalCount int32 `json:"vertexIntervalCount,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16384
	CachedVertexIntervalCount int32 `json:"cachedVertexIntervalCount,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400000000000
	VertexUnionNanoseconds *int64 `json:"vertexUnionNanoseconds,omitempty"`
	// +kubebuilder:validation:Enum=observed;unavailable
	ExportAvailability string `json:"exportAvailability"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400000000000
	ExportUnionNanoseconds *int64 `json:"exportUnionNanoseconds,omitempty"`
	// +kubebuilder:validation:Enum=observed;unavailable
	PushAvailability string `json:"pushAvailability"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400000000000
	PushUnionNanoseconds *int64 `json:"pushUnionNanoseconds,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400000000000
	ExportPushOverlapNanoseconds *int64 `json:"exportPushOverlapNanoseconds,omitempty"`
	// +kubebuilder:validation:Enum=observed;incomplete;unavailable
	NydusAvailability string `json:"nydusAvailability"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400000000000
	NydusWallNanoseconds *int64 `json:"nydusWallNanoseconds,omitempty"`
	// +kubebuilder:validation:Enum=unavailable
	WorkerSessionsAvailability string `json:"workerSessionsAvailability"`
}

func Unavailable(reason string) *Observation {
	return &Observation{SchemaVersion: 1, Availability: "unavailable", Reason: reason,
		ExportAvailability: "unavailable", PushAvailability: "unavailable",
		NydusAvailability: "unavailable", WorkerSessionsAvailability: "unavailable"}
}

// Normalize copies only the known finite schema. Invalid optional telemetry is
// unavailable; it must not reject an otherwise valid pushed-digest receipt.
func Normalize(in *Observation) *Observation {
	if in == nil {
		return nil
	}
	if !valid(in) {
		return Unavailable("invalid_observation")
	}
	out := new(Observation)
	in.DeepCopyInto(out)
	return out
}

// Decode keeps old entries without telemetry compatible and refuses invalid
// optional telemetry without changing their build success/digest semantics.
func Decode(raw json.RawMessage) *Observation {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if len(raw) > 4096 {
		return Unavailable("invalid_observation")
	}
	var observation Observation
	if err := json.Unmarshal(raw, &observation); err != nil {
		return Unavailable("invalid_observation")
	}
	return Normalize(&observation)
}

func valid(o *Observation) bool {
	if o.SchemaVersion != 1 || o.WorkerSessionsAvailability != "unavailable" {
		return false
	}
	switch o.Availability {
	case "observed", "incomplete", "unavailable":
	default:
		return false
	}
	switch o.Reason {
	case "", "missing_stream", "malformed_stream", "limit_exceeded", "partial_stream", "command_failed", "cancelled", "invalid_observation":
	default:
		return false
	}
	if (o.Availability == "observed") != (o.Reason == "") {
		return false
	}
	if o.VertexCount < 0 || o.VertexCount > MaxVertices || o.VertexIntervalCount < 0 || o.VertexIntervalCount > MaxIntervals || o.CachedVertexIntervalCount < 0 || o.CachedVertexIntervalCount > o.VertexIntervalCount {
		return false
	}
	for _, value := range []*int64{o.VertexUnionNanoseconds, o.ExportUnionNanoseconds, o.PushUnionNanoseconds, o.ExportPushOverlapNanoseconds, o.NydusWallNanoseconds} {
		if value != nil && (*value < 0 || *value > MaxNanoseconds) {
			return false
		}
	}
	if !phaseValid(o.ExportAvailability, o.ExportUnionNanoseconds, false) || !phaseValid(o.PushAvailability, o.PushUnionNanoseconds, false) || !phaseValid(o.NydusAvailability, o.NydusWallNanoseconds, true) {
		return false
	}
	if (o.ExportPushOverlapNanoseconds != nil) != (o.ExportUnionNanoseconds != nil && o.PushUnionNanoseconds != nil) {
		return false
	}
	if o.ExportPushOverlapNanoseconds != nil && (*o.ExportPushOverlapNanoseconds > *o.ExportUnionNanoseconds || *o.ExportPushOverlapNanoseconds > *o.PushUnionNanoseconds) {
		return false
	}
	if o.Availability == "unavailable" && (o.VertexCount != 0 || o.VertexIntervalCount != 0 || o.CachedVertexIntervalCount != 0 || o.VertexUnionNanoseconds != nil || o.ExportUnionNanoseconds != nil || o.PushUnionNanoseconds != nil || o.ExportPushOverlapNanoseconds != nil) {
		return false
	}
	if o.Availability == "observed" && (o.VertexCount == 0 || o.VertexIntervalCount == 0 || o.VertexUnionNanoseconds == nil) {
		return false
	}
	return true
}

func phaseValid(availability string, seconds *int64, incomplete bool) bool {
	switch availability {
	case "observed":
		return seconds != nil
	case "incomplete":
		return incomplete && seconds != nil
	case "unavailable":
		return seconds == nil
	default:
		return false
	}
}

// DeepCopyInto is also used by generated Kubernetes receipt DeepCopy code.
func (in *Observation) DeepCopyInto(out *Observation) {
	*out = *in
	clone := func(p *int64) *int64 {
		if p == nil {
			return nil
		}
		value := *p
		return &value
	}
	out.VertexUnionNanoseconds = clone(in.VertexUnionNanoseconds)
	out.ExportUnionNanoseconds = clone(in.ExportUnionNanoseconds)
	out.PushUnionNanoseconds = clone(in.PushUnionNanoseconds)
	out.ExportPushOverlapNanoseconds = clone(in.ExportPushOverlapNanoseconds)
	out.NydusWallNanoseconds = clone(in.NydusWallNanoseconds)
}

func (in *Observation) DeepCopy() *Observation {
	if in == nil {
		return nil
	}
	out := new(Observation)
	in.DeepCopyInto(out)
	return out
}
