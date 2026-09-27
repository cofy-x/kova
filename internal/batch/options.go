package batch

import (
	"context"
	"time"

	"github.com/cofy-x/kova/internal/scheduler"
)

const DefaultBuildkitOOMCooldown = 2 * time.Minute

type Options struct {
	ImageDir  string
	ImageDirs string
	// ImageDirsAlreadyIsolated is set only for a private, single-use runner
	// extraction; ordinary callers keep copy-on-prepare source semantics.
	ImageDirsAlreadyIsolated   bool
	Addrs                      []*scheduler.Addr
	AddrsRaw                   string
	PlatformAddrs              map[string][]*scheduler.Addr
	PlatformAddrsRaw           map[string]string
	Concurrency                int
	Ctx                        context.Context
	Failfast                   bool
	OCI                        bool
	BuildFormat                string
	OOMCooldown                time.Duration
	ResultPath                 string
	LogsPath                   string
	Vars                       map[string]string
	Timeout                    int
	Verbose                    bool
	Target                     string
	Platform                   string
	ExportTargets              []string
	FromResultPath             string
	DragonflySchedulerAddr     string
	PreheatInsecureSkipVerify  bool
	PreheatPlainHTTPRegistries []string
	RegistryPlainHTTP          []string
	DockerConfigPath           string
	Interval                   int
	WithFail                   bool
	SummaryOnly                bool
}
