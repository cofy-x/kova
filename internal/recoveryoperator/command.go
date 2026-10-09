package recoveryoperator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
	"github.com/urfave/cli/v2"
)

// NewCLIApp is a separate, opt-in operator tool. The ordinary kova user client
// must never link Service/runtime packages or invoke incident disposal.
func NewCLIApp() *cli.App {
	return &cli.App{Name: "kova-recovery", Usage: "explicit old-epoch incident operations; never automatic recovery or capacity release",
		Commands: []*cli.Command{{Name: "dispose-exact", Usage: "qualify an independent exact disposal plan; mutate only with explicit --execute and a qualified grant",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "pins", Required: true, Usage: "canonical independently issued plan/grant expectations, public roots and TLS connection record"},
				&cli.StringFlag{Name: "pins-digest", Required: true, Usage: "independently pinned sha256 of the whole pins file; never learned from that file"},
				&cli.StringFlag{Name: "plan", Required: true, Usage: "externally signed canonical execution plan envelope"},
				&cli.StringFlag{Name: "grant", Required: true, Usage: "separately authorized canonical mutation grant envelope"},
				&cli.StringFlag{Name: "target-archives", Required: true, Usage: "private canonical JSON array of exact target bodies in plan order; not the complete incident archive"},
				&cli.BoolFlag{Name: "execute", Usage: "explicitly authorize this one bounded invocation of exact UID disposal; unknown stops without retry"},
				&cli.StringFlag{Name: "ca-file", Usage: "explicit CA bytes matching the independent connection record; required only with --execute"},
				&cli.StringFlag{Name: "credentials", Usage: "private canonical static bearer token or mTLS credential file; required only with --execute"},
				&cli.DurationFlag{Name: "timeout", Value: time.Minute, Usage: "whole-invocation deadline, greater than zero and at most 15m; grant expiry is also enforced"},
			}, Action: runRecoveryDisposal}}}
}

func runRecoveryDisposal(c *cli.Context) error {
	if c.NArg() != 0 || c.Duration("timeout") <= 0 || c.Duration("timeout") > 15*time.Minute {
		return fmt.Errorf("dispose-exact requires no positional arguments and a timeout in (0, 15m]")
	}
	if c.Bool("execute") && (c.String("ca-file") == "" || c.String("credentials") == "") {
		return fmt.Errorf("explicit --ca-file and --credentials are required with --execute")
	}
	ctx, cancel := context.WithTimeout(c.Context, c.Duration("timeout"))
	defer cancel()
	prepared, err := Prepare(ctx, Files{
		PinsPath: c.String("pins"), PinsDigest: c.String("pins-digest"), PlanPath: c.String("plan"),
		GrantPath: c.String("grant"), ArchivesPath: c.String("target-archives"),
	}, time.Now())
	if err != nil {
		return err
	}
	report := struct {
		Mode             string                                  `json:"mode"`
		Qualification    recoverydisposal.ArchivePreflightReport `json:"qualification"`
		Execution        *recoverydisposal.ExecutionReport       `json:"execution,omitempty"`
		IncidentClosed   bool                                    `json:"incidentClosed"`
		CapacityReleased bool                                    `json:"capacityReleased"`
		SuccessorCreated bool                                    `json:"successorCreated"`
	}{Mode: "local-preflight-no-api", Qualification: prepared.Report()}
	if c.Bool("execute") {
		report.Mode = "explicit-exact-disposal-invocation"
		execution, executeErr := Execute(ctx, prepared, c.String("ca-file"), c.String("credentials"))
		report.Execution = &execution
		err = executeErr
	}
	if outputErr := json.NewEncoder(c.App.Writer).Encode(report); outputErr != nil {
		return outputErr
	}
	return err
}
