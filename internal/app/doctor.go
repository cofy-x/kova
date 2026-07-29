package app

import (
	"fmt"

	"github.com/cofy-x/kova/internal/serviceapi"

	cli "github.com/urfave/cli/v2"
)

type doctorReport struct {
	Service serviceapi.VersionInfo `json:"service"`
	Checks  []doctorCheck          `json:"checks"`
}

type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func doctorCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "doctor",
		Usage: "verify Kova service compatibility, readiness, authentication, and authorization",
		Action: func(c *cli.Context) error {
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			report := doctorReport{}
			info, err := client.Version(c.Context)
			if err != nil {
				return fmt.Errorf("service version check failed: %w", err)
			}
			report.Service = info
			if info.APIVersion != serviceapi.APIVersion {
				report.Checks = append(report.Checks, doctorCheck{Name: "compatibility", Status: "failed", Detail: fmt.Sprintf("server=%s client=%s", info.APIVersion, serviceapi.APIVersion)})
				_ = writeJSON(c, report)
				return fmt.Errorf("Kova service API is incompatible")
			}
			report.Checks = append(report.Checks, doctorCheck{Name: "compatibility", Status: "ok", Detail: info.APIVersion})
			if err := client.Ready(c.Context); err != nil {
				report.Checks = append(report.Checks, doctorCheck{Name: "readiness", Status: "failed", Detail: err.Error()})
				_ = writeJSON(c, report)
				return fmt.Errorf("service readiness check failed: %w", err)
			}
			report.Checks = append(report.Checks, doctorCheck{Name: "readiness", Status: "ok"})
			if _, err := client.ListPage(c.Context, 1, ""); err != nil {
				report.Checks = append(report.Checks, doctorCheck{Name: "authorization", Status: "failed", Detail: err.Error()})
				_ = writeJSON(c, report)
				return fmt.Errorf("service authorization check failed: %w", err)
			}
			report.Checks = append(report.Checks, doctorCheck{Name: "authorization", Status: "ok"})
			return writeJSON(c, report)
		},
	}
}
