// Package recoveryinventory takes bounded, read-only archival observations of
// a stopped epoch. It does not prove retirement, write an archive, sign a plan,
// or authorize disposal. Its six unfiltered Lists cover KovaBuilds, Pods and
// ConfigMaps in both old namespaces, not all Kubernetes resource types.
package recoveryinventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	MaxObjects       = 8192
	MaxObjectBytes   = 2 * 1024 * 1024
	MaxArchiveBytes  = 64 * 1024 * 1024
	RequiredAPICalls = 12 // three identity reads before and after six Lists
	callTimeout      = 10 * time.Second
)

var ErrUnqualified = errors.New("recovery inventory is not qualified")

// DirectAPI must be uncached; callers must also bound transport response size.
// The interface intentionally exposes no Kubernetes write operation.
type DirectAPI interface {
	Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error
	List(context.Context, client.ObjectList, ...client.ListOption) error
}

type NamespacePin struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// Input is pinned independently of the response. BarrierEvidenceDigest binds
// the external observer's record; this collector cannot establish its truth.
type Input struct {
	IncidentID            string       `json:"incidentId"`
	SystemNamespaceUID    string       `json:"systemNamespaceUid"`
	Runner                NamespacePin `json:"runner"`
	Receipts              NamespacePin `json:"receipts"`
	BarrierEvidenceDigest string       `json:"barrierEvidenceDigest"`
	ObjectLimit           int          `json:"objectLimit"`
	ObjectByteLimit       int          `json:"objectByteLimit"`
	ArchiveByteLimit      int64        `json:"archiveByteLimit"`
}

type ObjectRef struct {
	Group           string `json:"group"`
	Version         string `json:"version"`
	Resource        string `json:"resource"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
	Digest          string `json:"digest"`
	Bytes           int    `json:"bytes"`
}

type PageWitness struct {
	Group           string `json:"group"`
	Version         string `json:"version"`
	Resource        string `json:"resource"`
	Namespace       string `json:"namespace"`
	ResourceVersion string `json:"resourceVersion"`
	Count           int    `json:"count"`
}

type Manifest struct {
	Version      string        `json:"version"`
	Stage        string        `json:"stage"`
	Input        Input         `json:"input"`
	Pages        []PageWitness `json:"pages"`
	Objects      []ObjectRef   `json:"objects"`
	ArchiveBytes int64         `json:"archiveBytes"`
	APICalls     int           `json:"apiCalls"`
}

// Snapshot contains sensitive object bodies for an external archive, never
// for ordinary logs. Bodies correspond to Manifest.Objects in sorted order.
// Successful collection is not an atomic cross-resource snapshot or a saved
// durable archive. All objects, including unrelated ConfigMaps, are retained.
type Snapshot struct {
	Manifest       Manifest
	ManifestDigest string
	Bodies         []json.RawMessage
}

type resource struct{ group, version, name, kind string }

var resources = []resource{
	{"", "v1", "configmaps", "ConfigMap"},
	{"", "v1", "pods", "Pod"},
	{kovav1.Group, kovav1.Version, "kovabuilds", "KovaBuild"},
}

// Collect rejects partial pages, namespace replacement/deletion, unknown read
// outcomes, or any exceeded bound. It returns no partial snapshot on failure.
// An external write/in-flight barrier and complete out-of-namespace archival
// verification remain prerequisites before any later namespace disposal.
func Collect(ctx context.Context, api DirectAPI, in Input) (Snapshot, error) {
	if api == nil || !validInput(in) {
		return Snapshot{}, ErrUnqualified
	}
	s := Snapshot{Manifest: Manifest{Version: "1", Stage: "inventory-observed-not-retired", Input: in,
		Pages: make([]PageWitness, 0, 6), Objects: make([]ObjectRef, 0)}}
	bodies := map[string]json.RawMessage{}
	uids := map[string]bool{}
	names := map[string]bool{}
	add := func(r resource, obj *unstructured.Unstructured) error {
		if len(s.Manifest.Objects) >= in.ObjectLimit || !opaque(string(obj.GetUID())) || !opaque(obj.GetResourceVersion()) ||
			len(validation.IsDNS1123Subdomain(obj.GetName())) != 0 || uids[string(obj.GetUID())] {
			return ErrUnqualified
		}
		raw, err := json.Marshal(obj.Object)
		if err != nil || len(raw) > in.ObjectByteLimit || s.Manifest.ArchiveBytes+int64(len(raw)) > in.ArchiveByteLimit {
			return ErrUnqualified
		}
		ref := ObjectRef{Group: r.group, Version: r.version, Resource: r.name, Namespace: obj.GetNamespace(), Name: obj.GetName(),
			UID: string(obj.GetUID()), ResourceVersion: obj.GetResourceVersion(), Digest: digest(raw), Bytes: len(raw)}
		key := objectKey(ref)
		nameKey := strings.Join([]string{r.group, r.version, r.name, ref.Namespace, ref.Name}, "/")
		if names[nameKey] {
			return ErrUnqualified
		}
		names[nameKey] = true
		uids[ref.UID] = true
		bodies[key] = raw
		s.Manifest.Objects = append(s.Manifest.Objects, ref)
		s.Manifest.ArchiveBytes += int64(len(raw))
		return nil
	}
	guard := func(archive bool) error {
		for _, pin := range []NamespacePin{{Name: "kube-system", UID: in.SystemNamespaceUID}, in.Runner, in.Receipts} {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
			c, cancel := context.WithTimeout(ctx, callTimeout)
			s.Manifest.APICalls++
			err := api.Get(c, client.ObjectKey{Name: pin.Name}, obj)
			cancel()
			phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
			if err != nil || ctx.Err() != nil || obj.GetAPIVersion() != "v1" || obj.GetKind() != "Namespace" ||
				obj.GetName() != pin.Name || string(obj.GetUID()) != pin.UID || obj.GetNamespace() != "" ||
				obj.GetDeletionTimestamp() != nil || phase != "Active" || !opaque(obj.GetResourceVersion()) {
				return ErrUnqualified
			}
			if archive && pin.Name != "kube-system" {
				if err := add(resource{"", "v1", "namespaces", "Namespace"}, obj); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := guard(true); err != nil {
		return Snapshot{}, fmt.Errorf("%w: initial namespace observation", ErrUnqualified)
	}
	for _, pin := range []NamespacePin{in.Runner, in.Receipts} {
		for _, r := range resources {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{Group: r.group, Version: r.version, Kind: r.kind + "List"})
			c, cancel := context.WithTimeout(ctx, callTimeout)
			s.Manifest.APICalls++
			err := api.List(c, list, client.InNamespace(pin.Name), client.Limit(int64(in.ObjectLimit-len(s.Manifest.Objects)+1)))
			cancel()
			if err != nil || ctx.Err() != nil || list.GetAPIVersion() != (schema.GroupVersion{Group: r.group, Version: r.version}).String() || list.GetKind() != r.kind+"List" || list.GetContinue() != "" || !opaque(list.GetResourceVersion()) ||
				(list.GetRemainingItemCount() != nil && *list.GetRemainingItemCount() != 0) || len(list.Items) > in.ObjectLimit-len(s.Manifest.Objects) {
				return Snapshot{}, fmt.Errorf("%w: incomplete or oversized %s inventory", ErrUnqualified, r.name)
			}
			s.Manifest.Pages = append(s.Manifest.Pages, PageWitness{Group: r.group, Version: r.version, Resource: r.name, Namespace: pin.Name, ResourceVersion: list.GetResourceVersion(), Count: len(list.Items)})
			for i := range list.Items {
				obj := &list.Items[i]
				if obj.GetNamespace() != pin.Name || obj.GetAPIVersion() != (schema.GroupVersion{Group: r.group, Version: r.version}).String() || obj.GetKind() != r.kind {
					return Snapshot{}, ErrUnqualified
				}
				if err := add(r, obj); err != nil {
					return Snapshot{}, fmt.Errorf("%w: object archive bounds or identity", ErrUnqualified)
				}
			}
		}
	}
	if err := guard(false); err != nil {
		return Snapshot{}, fmt.Errorf("%w: final namespace observation", ErrUnqualified)
	}
	sort.Slice(s.Manifest.Objects, func(i, j int) bool { return objectKey(s.Manifest.Objects[i]) < objectKey(s.Manifest.Objects[j]) })
	s.Bodies = make([]json.RawMessage, 0, len(s.Manifest.Objects))
	for _, ref := range s.Manifest.Objects {
		s.Bodies = append(s.Bodies, bodies[objectKey(ref)])
	}
	raw, err := json.Marshal(s.Manifest)
	if err != nil {
		return Snapshot{}, ErrUnqualified
	}
	s.ManifestDigest = digest(raw)
	return s, nil
}

func objectKey(r ObjectRef) string {
	return strings.Join([]string{r.Group, r.Version, r.Resource, r.Namespace, r.Name, r.UID}, "/")
}
func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func opaque(s string) bool {
	if s == "" || len(s) > 256 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func validInput(in Input) bool {
	if !opaque(in.IncidentID) || !opaque(in.SystemNamespaceUID) || !opaque(in.Runner.UID) || !opaque(in.Receipts.UID) ||
		in.Runner.Name == in.Receipts.Name || in.Runner.Name == "kube-system" || in.Receipts.Name == "kube-system" ||
		in.Runner.UID == in.Receipts.UID || in.Runner.UID == in.SystemNamespaceUID || in.Receipts.UID == in.SystemNamespaceUID ||
		len(validation.IsDNS1123Label(in.Runner.Name)) != 0 || len(validation.IsDNS1123Label(in.Receipts.Name)) != 0 ||
		in.ObjectLimit < 2 || in.ObjectLimit > MaxObjects || in.ObjectByteLimit < 1 || in.ObjectByteLimit > MaxObjectBytes ||
		in.ArchiveByteLimit < 1 || in.ArchiveByteLimit > MaxArchiveBytes || len(in.BarrierEvidenceDigest) != 71 || !strings.HasPrefix(in.BarrierEvidenceDigest, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(in.BarrierEvidenceDigest[7:])
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == in.BarrierEvidenceDigest[7:]
}
