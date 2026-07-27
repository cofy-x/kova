package app

import (
	"fmt"

	"github.com/cofy-x/kova/internal/version"

	cli "github.com/urfave/cli/v2"
)

func versionCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "print version and build information",
		Action: func(c *cli.Context) error {
			_, err := fmt.Fprintln(c.App.Writer, version.String("kova"))
			return err
		},
	}
}
