package batch

import (
	"context"
	"time"

	"github.com/cofy-x/kova/internal/scheduler"
)

const DefaultBuildkitOOMCooldown = 2 * time.Minute

type Options struct {
	ImageDir                  string
	ImageDirs                 string
	Addrs                     []*scheduler.Addr
	AddrsRaw                  string
	Concurrency               int
	Ctx                       context.Context
	Failfast                  bool
	OCI                       bool
	BuildFormat               string
	OOMCooldown               time.Duration
	ResultPath                string
	LogsPath                  string
	Vars                      map[string]string
	Timeout                   int
	Verbose                   bool
	Target                    string
	ExportTargets             []string
	SkipFail                  bool
	FromResultPath            string
	Retry                     int
	DragonflySchedulerAddr    string
	PreheatInsecureSkipVerify bool
	DockerConfigPath          string
	Interval                  int
	WithFail                  bool
}
