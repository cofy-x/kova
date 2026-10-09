package recoveryinventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type inventoryAPI struct {
	t           *testing.T
	in          Input
	gets, lists int
	getHook     func(int, *unstructured.Unstructured) error
	listHook    func(int, *unstructured.UnstructuredList) error
}

func (a *inventoryAPI) Get(ctx context.Context, key client.ObjectKey, object client.Object, _ ...client.GetOption) error {
	a.t.Helper()
	if _, ok := ctx.Deadline(); !ok {
		a.t.Fatal("API read has no deadline")
	}
	a.gets++
	uid := map[string]string{"kube-system": a.in.SystemNamespaceUID, a.in.Runner.Name: a.in.Runner.UID, a.in.Receipts.Name: a.in.Receipts.UID}[key.Name]
	if uid == "" || key.Namespace != "" {
		a.t.Fatalf("unexpected namespace read: %+v", key)
	}
	u := object.(*unstructured.Unstructured)
	*u = *testObject("v1", "Namespace", "", key.Name, uid)
	u.Object["status"] = map[string]any{"phase": "Active"}
	if a.getHook != nil {
		return a.getHook(a.gets, u)
	}
	return nil
}

func (a *inventoryAPI) List(ctx context.Context, objects client.ObjectList, options ...client.ListOption) error {
	a.t.Helper()
	if _, ok := ctx.Deadline(); !ok {
		a.t.Fatal("API list has no deadline")
	}
	var opts client.ListOptions
	for _, option := range options {
		option.ApplyToList(&opts)
	}
	if opts.Namespace != a.in.Runner.Name && opts.Namespace != a.in.Receipts.Name {
		a.t.Fatal("unscoped list")
	}
	if opts.Limit < 1 || opts.Limit > int64(a.in.ObjectLimit) || opts.Continue != "" || opts.LabelSelector != nil || opts.FieldSelector != nil {
		a.t.Fatalf("partial/filtered/unbounded List: %+v", opts)
	}
	a.lists++
	u := objects.(*unstructured.UnstructuredList)
	u.SetResourceVersion(fmt.Sprintf("list-rv-%d", a.lists))
	kind := strings.TrimSuffix(u.GetKind(), "List")
	u.Items = []unstructured.Unstructured{*testObject(u.GetAPIVersion(), kind, opts.Namespace, fmt.Sprintf("object-%d", a.lists), fmt.Sprintf("object-uid-%d", a.lists))}
	if kind == "ConfigMap" {
		u.Items[0].Object["data"] = map[string]any{"unlabelled": "all configmaps are archived"}
		u.Items[0].SetFinalizers([]string{"foreign.example/retain", "kova.cofy.dev/recovery-hold"})
	}
	if kind == "KovaBuild" {
		// Unknown/future fields survive because the archive is unstructured.
		u.Items[0].Object["spec"] = map[string]any{"futureField": map[string]any{"mode": "direct-admin"}}
	}
	if a.listHook != nil {
		return a.listHook(a.lists, u)
	}
	return nil
}

func testObject(apiVersion, kind, ns, name, uid string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetUID(types.UID(uid))
	u.SetResourceVersion("object-rv")
	return u
}
func inventoryInput() Input {
	return Input{IncidentID: "incident-one", SystemNamespaceUID: "system-uid",
		Runner: NamespacePin{Name: "runner-old", UID: "runner-uid"}, Receipts: NamespacePin{Name: "receipt-old", UID: "receipt-uid"},
		BarrierEvidenceDigest: "sha256:" + strings.Repeat("a", 64), ObjectLimit: 32, ObjectByteLimit: MaxObjectBytes, ArchiveByteLimit: MaxArchiveBytes}
}

func TestInventoryArchivesAllSixKindsWithoutAuthority(t *testing.T) {
	in := inventoryInput()
	api := &inventoryAPI{t: t, in: in}
	s, err := Collect(context.Background(), api, in)
	if err != nil {
		t.Fatal(err)
	}
	if api.gets != 6 || api.lists != 6 || s.Manifest.APICalls != RequiredAPICalls || len(s.Manifest.Objects) != 8 || len(s.Bodies) != 8 || len(s.Manifest.Pages) != 6 || s.Manifest.Stage != "inventory-observed-not-retired" {
		t.Fatalf("bad bounded report: %+v", s.Manifest)
	}
	manifestRaw, _ := json.Marshal(s.Manifest)
	if s.ManifestDigest != digest(manifestRaw) {
		t.Fatal("manifest hash mismatch")
	}
	var total int64
	var future, unlabelled int
	for i, ref := range s.Manifest.Objects {
		if i > 0 && objectKey(s.Manifest.Objects[i-1]) >= objectKey(ref) {
			t.Fatal("inventory not strictly sorted")
		}
		body := s.Bodies[i]
		if len(body) != ref.Bytes || digest(body) != ref.Digest {
			t.Fatal("body not bound")
		}
		total += int64(len(body))
		if strings.Contains(string(body), "futureField") {
			future++
		}
		if strings.Contains(string(body), "unlabelled") {
			unlabelled++
		}
	}
	if total != s.Manifest.ArchiveBytes || future != 2 || unlabelled != 2 {
		t.Fatal("inventory omitted unfiltered fields or bytes")
	}
	// Output is deterministic; it does not claim storage or retirement.
	s2, err := Collect(context.Background(), &inventoryAPI{t: t, in: in}, in)
	if err != nil || !reflect.DeepEqual(s, s2) {
		t.Fatal("noncanonical observation")
	}
}

func TestInventoryRejectsInvalidInputBeforeReads(t *testing.T) {
	for name, change := range map[string]func(*Input){
		"same names":    func(i *Input) { i.Receipts.Name = i.Runner.Name },
		"same uids":     func(i *Input) { i.Receipts.UID = i.Runner.UID },
		"system name":   func(i *Input) { i.Runner.Name = "kube-system" },
		"system uid":    func(i *Input) { i.Runner.UID = i.SystemNamespaceUID },
		"bad digest":    func(i *Input) { i.BarrierEvidenceDigest = "sha256:" + strings.Repeat("A", 64) },
		"empty id":      func(i *Input) { i.IncidentID = "" },
		"object count":  func(i *Input) { i.ObjectLimit = MaxObjects + 1 },
		"object bytes":  func(i *Input) { i.ObjectByteLimit = MaxObjectBytes + 1 },
		"archive bytes": func(i *Input) { i.ArchiveByteLimit = MaxArchiveBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			in := inventoryInput()
			change(&in)
			api := &inventoryAPI{t: t, in: in}
			s, err := Collect(context.Background(), api, in)
			if !errors.Is(err, ErrUnqualified) || !reflect.DeepEqual(s, Snapshot{}) || api.gets+api.lists != 0 {
				t.Fatal("invalid input reached API")
			}
		})
	}
}

func TestInventoryFailsClosedWithoutReturningPartialArchive(t *testing.T) {
	for _, name := range []string{"initial replacement", "final replacement", "namespace terminating", "read error", "list error", "continue", "remaining", "missing list rv", "wrong list kind", "wrong namespace", "wrong kind", "duplicate uid", "duplicate name", "object budget", "per object bytes", "total bytes", "missing object rv"} {
		t.Run(name, func(t *testing.T) {
			in := inventoryInput()
			if name == "object budget" {
				in.ObjectLimit = 7
			}
			if name == "per object bytes" {
				in.ObjectByteLimit = 1
			}
			if name == "total bytes" {
				in.ArchiveByteLimit = 1
			}
			api := &inventoryAPI{t: t, in: in}
			api.getHook = func(n int, u *unstructured.Unstructured) error {
				if name == "initial replacement" && n == 2 || name == "final replacement" && n == 5 {
					u.SetUID("replacement")
				}
				if name == "namespace terminating" && n == 5 {
					u.Object["metadata"].(map[string]any)["deletionTimestamp"] = "2026-10-03T00:00:00Z"
				}
				if name == "read error" && n == 6 {
					return errors.New("sensitive upstream payload")
				}
				return nil
			}
			api.listHook = func(n int, u *unstructured.UnstructuredList) error {
				if n != 4 {
					return nil
				}
				switch name {
				case "list error":
					return errors.New("sensitive upstream payload")
				case "continue":
					u.SetContinue("not-empty")
				case "remaining":
					one := int64(1)
					u.SetRemainingItemCount(&one)
				case "missing list rv":
					u.SetResourceVersion("")
				case "wrong list kind":
					u.SetKind("SecretList")
				case "wrong namespace":
					u.Items[0].SetNamespace("other")
				case "wrong kind":
					u.Items[0].SetKind("Secret")
				case "duplicate uid":
					u.Items[0].SetUID("object-uid-1")
				case "duplicate name":
					copy := *u.Items[0].DeepCopy()
					copy.SetUID("different-uid")
					u.Items = append(u.Items, copy)
				case "missing object rv":
					u.Items[0].SetResourceVersion("")
				}
				return nil
			}
			s, err := Collect(context.Background(), api, in)
			if !errors.Is(err, ErrUnqualified) || !reflect.DeepEqual(s, Snapshot{}) || api.gets+api.lists > RequiredAPICalls {
				t.Fatalf("unsafe partial result or call budget: err=%v", err)
			}
			if strings.Contains(err.Error(), "sensitive") {
				t.Fatal("error leaked body")
			}
		})
	}
}

func TestInventoryEmptyPagesAreNotRetirementProof(t *testing.T) {
	in := inventoryInput()
	api := &inventoryAPI{t: t, in: in, listHook: func(_ int, u *unstructured.UnstructuredList) error { u.Items = nil; return nil }}
	s, err := Collect(context.Background(), api, in)
	if err != nil || len(s.Manifest.Objects) != 2 || s.Manifest.Stage != "inventory-observed-not-retired" || len(s.Manifest.Pages) != 6 {
		t.Fatal("empty inventory changed authority")
	}
	// Prove the Kova group is included, not a broad core-only scan.
	if s.Manifest.Pages[2].Group != kovav1.Group || s.Manifest.Pages[2].Resource != "kovabuilds" {
		t.Fatal("KovaBuild inventory omitted")
	}
}

func TestInventoryCanceledContextReturnsNoArchive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := inventoryInput()
	api := &inventoryAPI{t: t, in: in}
	s, err := Collect(ctx, api, in)
	if !errors.Is(err, ErrUnqualified) || !reflect.DeepEqual(s, Snapshot{}) || api.gets+api.lists > 1 {
		t.Fatal("continued canceled scan")
	}
}
