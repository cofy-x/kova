package daemon

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/cofy-x/kova/internal/daemonclient"

	cli "github.com/urfave/cli/v2"
)

func TransportCLICommand() *cli.Command {
	return &cli.Command{
		Name:   "transport",
		Hidden: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "socket", Value: daemonclient.DefaultSocket},
			&cli.StringFlag{Name: "method", Required: true},
			&cli.StringFlag{Name: "path", Required: true},
			&cli.StringFlag{Name: "query"},
			&cli.StringFlag{Name: "input"},
		},
		Action: func(c *cli.Context) error {
			query, err := url.ParseQuery(c.String("query"))
			if err != nil {
				return err
			}
			var body io.Reader = c.App.Reader
			if input := strings.TrimSpace(c.String("input")); input != "" {
				file, err := os.Open(input)
				if err != nil {
					return err
				}
				defer file.Close()
				body = file
			}
			if c.String("method") == http.MethodGet {
				body = nil
			}
			return daemonclient.New(c.String("socket")).Do(
				c.Context, c.String("method"), c.String("path"), query, body, c.App.Writer,
			)
		},
	}
}
