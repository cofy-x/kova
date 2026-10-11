package recoveryoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverydrain"
	"github.com/cofy-x/kova/internal/service/recoveryinventory"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
)

const maxStageBytes = 16 * 1024 * 1024
const maxJournalBytes = 80 * 1024 * 1024
const maxStageAPICalls = 65536

var errOutput = errors.New("recovery output is not durably qualified; preserve this invocation")

// StagePins is an independently selected, canonical invocation record. Its
// whole-file digest is supplied by a separate trusted channel. Roots contain
// no signing material. Inventory and Drain are mutually exclusive.
type StagePins struct {
	Version     string                   `json:"version"`
	Action      string                   `json:"action"`
	Connection  ConnectionRecord         `json:"connection"`
	MaxAPICalls int                      `json:"maxApiCalls"`
	Inventory   *recoveryinventory.Input `json:"inventory"`
	Drain       *recoverydrain.Input     `json:"drain"`
	Roots       []PublicKeyPin           `json:"roots"`
}

type preparedStage struct {
	pins     StagePins
	digest   string
	deadline time.Time
	timeout  time.Duration
}

func prepareStage(ctx context.Context, path, pin, action string) (preparedStage, error) {
	if ctx == nil || ctx.Err() != nil || !validDigest(pin) {
		return preparedStage{}, ErrInput
	}
	raw, err := readRegularFile(path, maxStageBytes, true)
	var p StagePins
	if err != nil || digest(raw) != pin || decodeCanonical(raw, &p) != nil || p.Version != "1" || p.Action != action ||
		!validConnection(p.Connection) || p.MaxAPICalls < 14 || p.MaxAPICalls > maxStageAPICalls {
		return preparedStage{}, ErrInput
	}
	switch action {
	case "collect-exact":
		if p.Inventory == nil || p.Drain != nil || len(p.Roots) != 0 || p.Inventory.SystemNamespaceUID != p.Connection.SystemNamespaceUID ||
			p.Inventory.ObjectLimit < 2 || p.Inventory.ObjectLimit > recoveryinventory.MaxObjects ||
			p.Inventory.ObjectByteLimit < 1 || p.Inventory.ObjectByteLimit > recoveryinventory.MaxObjectBytes ||
			p.Inventory.ArchiveByteLimit < 1 || p.Inventory.ArchiveByteLimit > recoveryinventory.MaxArchiveBytes ||
			!validDigest(p.Inventory.BarrierEvidenceDigest) || !safeToken(p.Inventory.IncidentID) {
			return preparedStage{}, ErrInput
		}
	case "drain-exact":
		if p.Drain == nil || p.Inventory != nil || p.Drain.MaxLiabilities < 1 || p.Drain.MaxLiabilities > 2048 ||
			len(p.Drain.Expected.Receipts) > p.Drain.MaxLiabilities || p.Drain.TrustRoots != nil {
			return preparedStage{}, ErrInput
		}
		r, err := roots(p.Roots)
		if err != nil {
			return preparedStage{}, ErrInput
		}
		p.Drain.TrustRoots = recoverypermit.TrustRoots(r)
		if _, err := recoverypermit.VerifyDrainPermit(p.Drain.Permit, p.Drain.Expected, p.Drain.TrustRoots); err != nil {
			return preparedStage{}, ErrInput
		}
		if len(p.Drain.Queues)+len(p.Drain.Grants)+len(p.Drain.PodCreates) != len(p.Drain.Expected.Receipts) {
			return preparedStage{}, ErrInput
		}
		for _, q := range p.Drain.Queues {
			if _, err := recoveryreceipt.NewQueueConfigMap(q); err != nil {
				return preparedStage{}, ErrInput
			}
		}
		for _, g := range p.Drain.Grants {
			if _, err := recoveryreceipt.NewGrantConfigMap(g); err != nil {
				return preparedStage{}, ErrInput
			}
		}
		for _, v := range p.Drain.PodCreates {
			if _, err := recoveryreceipt.NewPodCreateConfigMap(v); err != nil {
				return preparedStage{}, ErrInput
			}
		}
	default:
		return preparedStage{}, ErrInput
	}
	// Also validate namespace syntax and independent identity topology before
	// connecting. The library remains responsible for full live qualification.
	if _, err := newStageScope(p); err != nil {
		return preparedStage{}, ErrInput
	}
	return preparedStage{pins: p, digest: pin}, nil
}

// A journal is caller-owned, append-only private evidence, not a Kova archive
// service. Reserve and fsync intent before connecting; every wire attempt is
// recorded before sending. A write/sync/readback failure stops the invocation.
type stageJournal struct {
	f          *os.File
	h          hash.Hash
	bytes      int64
	path       string
	info       os.FileInfo
	parentInfo os.FileInfo
}

func reserveJournal(path string, p preparedStage) (*stageJournal, error) {
	if !filepath.IsAbs(path) {
		return nil, errOutput
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 || parent.Mode().Perm()&0077 != 0 {
		return nil, errOutput
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errOutput
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, errOutput
	}
	j := &stageJournal{f: f, h: sha256.New(), path: path, info: info, parentInfo: parent}
	deadline := ""
	if !p.deadline.IsZero() {
		deadline = p.deadline.UTC().Format(time.RFC3339Nano)
	}
	if err := j.append(struct {
		Stage, Action, PinsDigest, Deadline string
		TimeoutNanoseconds                  int64
	}{"prepared-not-executed", p.pins.Action, p.digest, deadline, int64(p.timeout)}); err != nil {
		_ = f.Close()
		return nil, err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		_ = f.Close()
		return nil, errOutput
	}
	openedParent, statErr := d.Stat()
	if statErr != nil || !os.SameFile(parent, openedParent) {
		_ = d.Close()
		_ = f.Close()
		return nil, errOutput
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		_ = f.Close()
		return nil, errOutput
	}
	return j, nil
}

func (j *stageJournal) append(v any) error {
	if j.identity() != nil {
		return errOutput
	}
	raw, err := json.Marshal(v)
	if err != nil || j.bytes+int64(len(raw))+1 > maxJournalBytes {
		return errOutput
	}
	raw = append(raw, '\n')
	n, err := j.f.Write(raw)
	if err != nil || n != len(raw) {
		return errOutput
	}
	_, _ = j.h.Write(raw)
	j.bytes += int64(n)
	if j.f.Sync() != nil {
		return errOutput
	}
	return nil
}

func (j *stageJournal) identity() error {
	current, err := os.Lstat(j.path)
	if err != nil || !os.SameFile(j.info, current) || !current.Mode().IsRegular() || current.Mode().Perm()&0077 != 0 || current.Size() != j.bytes {
		return errOutput
	}
	opened, err := j.f.Stat()
	if err != nil || !os.SameFile(current, opened) || opened.Mode().Perm()&0077 != 0 || opened.Size() != j.bytes {
		return errOutput
	}
	parent, err := os.Lstat(filepath.Dir(j.path))
	if err != nil || !parent.IsDir() || !os.SameFile(j.parentInfo, parent) || parent.Mode().Perm()&0077 != 0 {
		return errOutput
	}
	return nil
}

func (j *stageJournal) finish() (string, error) {
	if j.f.Sync() != nil {
		return "", errOutput
	}
	if j.identity() != nil {
		return "", errOutput
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return "", errOutput
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(j.f, j.bytes+1))
	if err != nil || n != j.bytes || !bytes.Equal(h.Sum(nil), j.h.Sum(nil)) {
		return "", errOutput
	}
	if j.identity() != nil {
		return "", errOutput
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

type stageResult struct {
	Stage            string `json:"stage"`
	Action           string `json:"action"`
	PinsDigest       string `json:"pinsDigest"`
	APICalls         int    `json:"apiCalls"`
	OutputPath       string `json:"outputPath,omitempty"`
	OutputDigest     string `json:"outputDigest,omitempty"`
	IncidentClosed   bool   `json:"incidentClosed"`
	CapacityReleased bool   `json:"capacityReleased"`
	SuccessorCreated bool   `json:"successorCreated"`
}

func executeStage(ctx context.Context, p preparedStage, caPath, credPath, outPath string) (stageResult, error) {
	result := stageResult{Stage: "unknown-preserve", Action: p.pins.Action, PinsDigest: p.digest, OutputPath: outPath}
	if ctx == nil || ctx.Err() != nil {
		return result, ErrInput
	}
	p.deadline, _ = ctx.Deadline()
	j, err := reserveJournal(outPath, p)
	if err != nil {
		return result, err
	}
	defer j.f.Close()
	a, err := newStageAPI(p.pins, caPath, credPath, j)
	if err == nil {
		defer a.base.http.CloseIdleConnections()
		err = a.guardSystem(ctx)
	}
	if err == nil {
		switch p.pins.Action {
		case "collect-exact":
			var snapshot recoveryinventory.Snapshot
			snapshot, err = recoveryinventory.Collect(ctx, a, *p.pins.Inventory)
			if err == nil {
				err = j.append(struct {
					Stage    string
					Snapshot recoveryinventory.Snapshot
				}{"inventory-observed-not-retired", snapshot})
			}
			if err == nil {
				result.Stage = "inventory-observed-not-retired"
			}
		case "drain-exact":
			var report recoverydrain.Report
			report, err = recoverydrain.Drain(ctx, a, *p.pins.Drain)
			// Partial dispositions are evidence, not permission to resume an old
			// invocation or infer a complete drain. Never echo underlying payloads.
			if saveErr := j.append(struct {
				Stage  string
				Report recoverydrain.Report
			}{"drain-report", report}); saveErr != nil {
				err = saveErr
			}
			if err == nil {
				result.Stage = report.Stage
			}
		}
	}
	if err == nil {
		err = a.guardSystem(ctx)
	}
	if a != nil {
		result.APICalls = a.calls
	}
	if err != nil {
		result.Stage = "unknown-preserve"
	}
	if saveErr := j.append(result); saveErr != nil {
		return result, saveErr
	}
	outputDigest, errOutputFinal := j.finish()
	if errOutputFinal != nil {
		result.Stage = "unknown-preserve"
		return result, errOutputFinal
	}
	result.OutputDigest = outputDigest
	if err != nil {
		return result, errAPI
	}
	return result, nil
}

func stageTimeout(value time.Duration) bool { return value > 0 && value <= 15*time.Minute }
