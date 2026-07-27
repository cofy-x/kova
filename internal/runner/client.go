package runner

import (
	"fmt"
	"io"
	"os"

	"github.com/cofy-x/kova/internal/kube"
)

type Client struct {
	Config Config
	Kube   kube.API
	Stdout io.Writer
	Stderr io.Writer
	Stdin  io.Reader
}

func NewClient(config Config) *Client {
	return &Client{
		Config: config,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
	}
}

func (c *Client) kubeClient() (kube.API, error) {
	if c.Kube != nil {
		return c.Kube, nil
	}
	if err := c.Config.requireKubeconfig(); err != nil {
		return nil, err
	}
	client, err := kube.NewClient(c.Config.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	c.Kube = client
	return client, nil
}
