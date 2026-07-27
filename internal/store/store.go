package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cofy-x/kova/internal/logging"
)

// Entry is stored in LMDB and exported as JSONL.
type Entry struct {
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	Elapsed    string `json:"elapsed"`
	Target     string `json:"target"`
	NodeIP     string `json:"node_ip,omitempty"`
	Success    bool   `json:"success"`
	Logs       string `json:"logs,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

func (r *Entry) UnmarshalJSON(data []byte) error {
	type rawResultEntry struct {
		StartedAt  string          `json:"started_at"`
		FinishedAt string          `json:"finished_at"`
		Elapsed    json.RawMessage `json:"elapsed"`
		Target     string          `json:"target"`
		NodeIP     string          `json:"node_ip,omitempty"`
		Success    bool            `json:"success"`
		Logs       string          `json:"logs,omitempty"`
		Reason     string          `json:"reason,omitempty"`
	}

	var raw rawResultEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	elapsed, err := parseElapsedJSON(raw.Elapsed)
	if err != nil {
		return err
	}

	r.StartedAt = raw.StartedAt
	r.FinishedAt = raw.FinishedAt
	r.Elapsed = elapsed
	r.Target = raw.Target
	r.NodeIP = raw.NodeIP
	r.Success = raw.Success
	r.Logs = raw.Logs
	r.Reason = raw.Reason
	return nil
}

func parseElapsedJSON(data json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}

	var elapsedString string
	if err := json.Unmarshal(trimmed, &elapsedString); err == nil {
		return elapsedString, nil
	}

	var elapsedNumber json.Number
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&elapsedNumber); err != nil {
		return "", fmt.Errorf("decode elapsed: %w", err)
	}

	seconds, err := elapsedNumber.Float64()
	if err != nil {
		return "", fmt.Errorf("parse elapsed number: %w", err)
	}
	if seconds < 0 {
		return "", fmt.Errorf("parse elapsed number: negative value %v", seconds)
	}

	return logging.FormatElapsed(time.Duration(seconds * float64(time.Second))), nil
}

type Store struct {
	db   *lmdbResultDB
	logs *failureLogStore
}

type Writer interface {
	UpsertResult(entry Entry) error
}

type OutcomeState uint8

const (
	OutcomeUnknown OutcomeState = iota
	OutcomeFailed
	OutcomeSucceeded
)

type OutcomeCounters struct {
	mu        sync.Mutex
	total     int
	succeeded int
	failed    int
	states    map[string]OutcomeState
}

func NewOutcomeCounters(total int, states map[string]OutcomeState) *OutcomeCounters {
	counters := &OutcomeCounters{
		total:  total,
		states: make(map[string]OutcomeState, len(states)),
	}
	for target, state := range states {
		counters.states[target] = state
		switch state {
		case OutcomeSucceeded:
			counters.succeeded++
		case OutcomeFailed:
			counters.failed++
		}
	}
	return counters
}

func (c *OutcomeCounters) Apply(target string, success bool) (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	previous := c.states[target]
	next := OutcomeFailed
	if success {
		next = OutcomeSucceeded
	}

	if previous != next {
		switch previous {
		case OutcomeSucceeded:
			c.succeeded--
		case OutcomeFailed:
			c.failed--
		}
		switch next {
		case OutcomeSucceeded:
			c.succeeded++
		case OutcomeFailed:
			c.failed++
		}
		c.states[target] = next
	}

	return c.succeeded, c.total, c.failed
}

func (c *OutcomeCounters) Snapshot() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.succeeded, c.total, c.failed
}

// ----------------------------------------------------------------
// persistent build result store
// ----------------------------------------------------------------

func NewStore(resultPath, logsPath string) (*Store, error) {
	db, err := openLMDBResultDB(resultPath)
	if err != nil {
		return nil, err
	}
	return &Store{
		db:   db,
		logs: newFailureLogStore(logsPath),
	}, nil
}

func (rs *Store) UpsertResult(entry Entry) error {
	// Write failure log BEFORE writing to LMDB.
	if !entry.Success && entry.Logs != "" {
		if err := rs.logs.AppendFailure(entry.Target, entry.Logs); err != nil {
			logging.Errorf("Failed to append failure log for %s: %v", entry.Target, err)
		}
		// Clear verbose logs before persisting in LMDB to save space.
		entry.Logs = ""
	}
	return rs.db.Put(entry)
}

func PersistBuildResult(store Writer, counters *OutcomeCounters, entry Entry) (int, int, int, error) {
	if err := store.UpsertResult(entry); err != nil {
		return 0, 0, 0, fmt.Errorf("store result for %s: %w", entry.Target, err)
	}
	succeeded, total, failed := counters.Apply(entry.Target, entry.Success)
	return succeeded, total, failed, nil
}

func (rs *Store) Close() error {
	if rs.db != nil {
		return rs.db.Close()
	}
	return nil
}

func (rs *Store) Get(target string) (Entry, bool, error) {
	return rs.db.Get(target)
}

func Open(path string) (*lmdbResultDB, error) {
	return openLMDBResultDB(path)
}

func resolveOptionalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	return filepath.Abs(path)
}

func (rs *Store) AppendFailure(target string, logs string) error {
	return rs.logs.AppendFailure(target, logs)
}
