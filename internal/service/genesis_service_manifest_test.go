package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	"github.com/cofy-x/kova/internal/version"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	serviceProcessManifestEnv = "KOVA_GENESIS_SERVICE_MANIFEST"
	serviceProcessChildEnv    = "KOVA_GENESIS_SERVICE_CHILD"
	serviceProcessHashEnv     = "KOVA_GENESIS_SERVICE_MANIFEST_SHA256"
	serviceProcessDeadlineEnv = "KOVA_GENESIS_SERVICE_DEADLINE"
	serviceProcessProxyEnv    = "KOVA_GENESIS_SERVICE_PROXY"
	serviceProcessTokenEnv    = "KOVA_GENESIS_SERVICE_PROXY_TOKEN"
	serviceProcessListenEnv   = "KOVA_GENESIS_SERVICE_LISTEN"
	serviceProcessTotalLimit  = 20 * time.Minute
	serviceProcessCaseLimit   = 140 * time.Second
	serviceProcessChildLimit  = 3 * time.Minute
)

var serviceProcessCases = []string{"lost-active-create", "lost-active-pin", "lost-queue-create", "lost-queue-pin", "lost-commit", "late-active-create", "late-active-create-after-loss"}

type serviceProcessManifest struct {
	SchemaVersion     int                  `json:"schemaVersion"`
	SourceCommit      string               `json:"sourceCommit"`
	TestBinarySHA256  string               `json:"testBinarySHA256"`
	Kubeconfig        string               `json:"kubeconfig"`
	KubeconfigSHA256  string               `json:"kubeconfigSHA256"`
	Context           string               `json:"context"`
	KubeSystemUID     string               `json:"kubeSystemUID"`
	ControlPlaneID    string               `json:"controlPlaneID"`
	OldWritersStopped *bool                `json:"oldWritersStopped"`
	CleanupOnSuccess  *bool                `json:"cleanupOnSuccess"`
	Cases             []serviceProcessCase `json:"cases"`
}

type serviceProcessCase struct {
	Name                         string      `json:"name"`
	AllowCommittedLedgerDeletion *bool       `json:"allowCommittedLedgerDeletion"`
	Fixture                      processCase `json:"fixture"`
}

func serviceProcessManifestField(path []string, key string) bool {
	if len(path) == 0 {
		switch key {
		case "schemaVersion", "sourceCommit", "testBinarySHA256", "kubeconfig", "kubeconfigSHA256", "context", "kubeSystemUID", "controlPlaneID", "oldWritersStopped", "cleanupOnSuccess", "cases":
			return true
		}
	}
	if len(path) == 1 && path[0] == "cases" {
		return key == "name" || key == "allowCommittedLedgerDeletion" || key == "fixture"
	}
	if len(path) == 2 && path[0] == "cases" && path[1] == "fixture" {
		return key != "stage" && key != "point" && processManifestField([]string{"cases"}, key)
	}
	return false
}

func (m serviceProcessManifest) target() processManifest {
	return processManifest{Kubeconfig: m.Kubeconfig, KubeconfigSHA256: m.KubeconfigSHA256,
		Context: m.Context, KubeSystemUID: m.KubeSystemUID, ControlPlaneID: m.ControlPlaneID}
}

func serviceProcessNamespace(name string) bool {
	return strings.HasPrefix(name, "kova-genesis-svc-") && len(validation.IsDNS1123Label(name)) == 0
}

func validateServiceProcessManifest(m serviceProcessManifest) error {
	commit, err := hex.DecodeString(m.SourceCommit)
	if m.SchemaVersion != 1 || err != nil || len(commit) != 20 || strings.ToLower(m.SourceCommit) != m.SourceCommit ||
		!validProcessHash(m.TestBinarySHA256) || !filepath.IsAbs(m.Kubeconfig) || !validProcessHash(m.KubeconfigSHA256) ||
		!dedicatedProcessContext(m.Context) || !admissioncontract.ValidUID(m.KubeSystemUID) || !validProcessHash(m.ControlPlaneID) ||
		m.OldWritersStopped == nil || !*m.OldWritersStopped || m.CleanupOnSuccess == nil || *m.CleanupOnSuccess || len(m.Cases) != len(serviceProcessCases) {
		return errors.New("Service gate requires a source-bound seven-case dedicated Kind manifest, stopped writers, and no API cleanup")
	}
	names, uids, secrets, cases := map[string]bool{}, map[string]bool{m.KubeSystemUID: true}, map[string]bool{}, map[string]bool{}
	for _, c := range m.Cases {
		known := false
		for _, name := range serviceProcessCases {
			known = known || c.Name == name
		}
		f := c.Fixture
		if !known || cases[c.Name] || c.AllowCommittedLedgerDeletion == nil || *c.AllowCommittedLedgerDeletion != (c.Name == "late-active-create-after-loss") ||
			f.Stage != "" || f.Point != "" || !filepath.IsAbs(f.ReceiptFile) || !validProcessHash(f.ReceiptSHA256) ||
			!admissioncontract.ValidUID(f.GenesisUID) || !admissioncontract.ValidUID(f.SecretUID) || len(validation.IsDNS1123Label(f.SecretName)) != 0 || secrets[f.SecretUID] {
			return errors.New("Service gate case identity or explicit destructive-case acknowledgement is invalid")
		}
		for _, pair := range [][2]string{{f.Namespace, f.NamespaceUID}, {f.ReceiptNamespace, f.ReceiptNamespaceUID}, {f.SecretNamespace, f.SecretNamespaceUID}} {
			if !serviceProcessNamespace(pair[0]) || !admissioncontract.ValidUID(pair[1]) || names[pair[0]] || uids[pair[1]] {
				return errors.New("Service gate requires distinct never-used runner, receipt and control Namespace names and UIDs")
			}
			names[pair[0]], uids[pair[1]] = true, true
		}
		if uids[f.GenesisUID] || uids[f.SecretUID] || f.GenesisUID == f.SecretUID {
			return errors.New("Service gate object UIDs alias another identity")
		}
		uids[f.GenesisUID], uids[f.SecretUID], secrets[f.SecretUID], cases[c.Name] = true, true, true, true
	}
	return nil
}

func loadServiceProcessManifest(path, expectedHash string) (serviceProcessManifest, string, error) {
	if !filepath.IsAbs(path) {
		return serviceProcessManifest{}, "", errors.New("Service manifest path must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64*1024 {
		return serviceProcessManifest{}, "", errors.New("Service manifest is missing, symlinked, or oversized")
	}
	raw, err := os.ReadFile(path)
	if err != nil || int64(len(raw)) != info.Size() {
		return serviceProcessManifest{}, "", errors.New("Service manifest changed while reading")
	}
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	if expectedHash != "" && hash != expectedHash {
		return serviceProcessManifest{}, "", errors.New("Service manifest SHA256 changed")
	}
	var m serviceProcessManifest
	if err := admissionjson.Decode(raw, &m, serviceProcessManifestField); err != nil {
		return m, "", err
	}
	if err := validateServiceProcessManifest(m); err != nil {
		return m, "", err
	}
	return m, hash, nil
}

func checkServiceProcessExecutable(m serviceProcessManifest) error {
	if version.Commit != m.SourceCommit {
		return errors.New("Service test binary source stamp differs from manifest; compile the clean candidate with an explicit version.Commit")
	}
	name, err := os.Executable()
	if err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 512*1024*1024 {
		return errors.New("Service test executable is outside its size bound")
	}
	hash := sha256.New()
	if n, err := io.Copy(hash, io.LimitReader(f, info.Size()+1)); err != nil || n != info.Size() {
		return errors.New("Service test executable changed while hashing")
	}
	if hex.EncodeToString(hash.Sum(nil)) != m.TestBinarySHA256 {
		return errors.New("Service test binary SHA256 differs from manifest")
	}
	return nil
}

func openServiceProcessConfig(ctx context.Context, m serviceProcessManifest) (*rest.Config, *processAPI, error) {
	api, err := openProcessAPI(ctx, m.target())
	if err != nil {
		return nil, nil, err
	}
	raw, err := readPinnedProcessFile(m.Kubeconfig, m.KubeconfigSHA256, 64*1024)
	if err != nil {
		return nil, nil, err
	}
	loaded, err := clientcmd.Load(raw)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(*loaded, m.Context, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	// JSON allows the test-only proxy to inspect exactly bounded mutation
	// bodies. Neither credentials nor raw API/Secret responses enter receipts.
	cfg.ContentType, cfg.AcceptContentTypes = "application/json", "application/json"
	return cfg, api, nil
}
