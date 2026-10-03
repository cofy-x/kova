package admissiongenesis

import (
	"fmt"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
)

// These are only the immutable capacity/header projections of the two empty
// ledger schemas. The supplied callbacks still validate their full states;
// this independent check prevents a callback wired to the wrong limits from
// making a self-consistent pair that disagrees with the external receipt.
type activeEmptyHeader struct {
	Version         int                    `json:"version"`
	Fence           uint64                 `json:"fence"`
	MaxJobs         int                    `json:"maxJobs"`
	MaxPerRequester int                    `json:"maxPerRequester"`
	WorkerSlots     int                    `json:"workerSlots"`
	Active          map[string]interface{} `json:"active"`
}

type queueEmptyHeader struct {
	Version        int                    `json:"version"`
	GlobalLimit    int                    `json:"globalLimit"`
	RequesterLimit int                    `json:"requesterLimit"`
	Intents        map[string]interface{} `json:"intents"`
}

func allowEmptyActiveHeader(path []string, key string) bool {
	if len(path) != 0 {
		return false
	}
	switch key {
	case "version", "fence", "maxJobs", "maxPerRequester", "workerSlots", "active":
		return true
	}
	return false
}

func allowEmptyQueueHeader(path []string, key string) bool {
	if len(path) != 0 {
		return false
	}
	switch key {
	case "version", "globalLimit", "requesterLimit", "intents":
		return true
	}
	return false
}

func validateCanonicalEmptyLimits(template LedgerTemplate, limits admissioncontract.Limits) error {
	raw := []byte(template.EmptyData)
	switch template.Role {
	case admissioncontract.Active:
		var state activeEmptyHeader
		if err := admissionjson.Decode(raw, &state, allowEmptyActiveHeader); err != nil {
			return fmt.Errorf("active canonical empty header: %w", err)
		}
		if err := admissioncontract.RequireKeys(raw, "version", "fence", "maxJobs", "maxPerRequester", "workerSlots", "active"); err != nil {
			return fmt.Errorf("active canonical empty header: %w", err)
		}
		if state.Version != 1 || state.Fence != 0 || state.Active == nil || len(state.Active) != 0 ||
			state.MaxJobs != limits.MaxActiveJobs || state.MaxPerRequester != limits.MaxActiveJobsPerRequester ||
			state.WorkerSlots != limits.WorkerSlots {
			return fmt.Errorf("active canonical empty capacity differs from admission receipt")
		}
	case admissioncontract.Queue:
		var state queueEmptyHeader
		if err := admissionjson.Decode(raw, &state, allowEmptyQueueHeader); err != nil {
			return fmt.Errorf("queue canonical empty header: %w", err)
		}
		if err := admissioncontract.RequireKeys(raw, "version", "globalLimit", "requesterLimit", "intents"); err != nil {
			return fmt.Errorf("queue canonical empty header: %w", err)
		}
		if state.Version != 1 || state.Intents == nil || len(state.Intents) != 0 ||
			state.GlobalLimit != limits.MaxQueuedJobs || state.RequesterLimit != limits.MaxQueuedJobsPerRequester {
			return fmt.Errorf("queue canonical empty capacity differs from admission receipt")
		}
	default:
		return fmt.Errorf("invalid admission ledger role")
	}
	return nil
}
