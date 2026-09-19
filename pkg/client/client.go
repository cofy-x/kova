// Package client provides the public Go client for Kova Service HTTP API v1.
package client

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultMaxResponseBytes int64 = 64 << 20
	maximumResponseBytes    int64 = 1 << 30
)

type Config struct {
	BaseURL          string
	Token            string
	Kubeconfig       string
	CAFile           string
	Insecure         bool
	HTTPClient       *http.Client
	MaxResponseBytes int64
}

type Client struct {
	baseURL          *url.URL
	http             *http.Client
	token            string
	maxResponseBytes int64
}

func New(cfg Config) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("service URL must be an absolute http or https URL")
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("service URL scheme must be http or https")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("service URL must not contain credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient, err = authenticatedHTTPClient(cfg, baseURL)
		if err != nil {
			return nil, err
		}
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes < 0 || maxResponseBytes > maximumResponseBytes {
		return nil, fmt.Errorf("maximum response size must be between 0 and %d bytes", maximumResponseBytes)
	}
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	return &Client{baseURL: baseURL, http: httpClient, token: strings.TrimSpace(cfg.Token), maxResponseBytes: maxResponseBytes}, nil
}

func authenticatedHTTPClient(cfg Config, baseURL *url.URL) (*http.Client, error) {
	var restConfig *rest.Config
	var err error
	if strings.TrimSpace(cfg.Token) == "" && strings.TrimSpace(cfg.Kubeconfig) != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load service credentials from kubeconfig: %w", err)
		}
	} else if strings.TrimSpace(cfg.Token) == "" {
		return nil, fmt.Errorf("service authentication requires a bearer token or kubeconfig")
	} else {
		restConfig = &rest.Config{}
	}
	restConfig.Host = baseURL.String()
	restConfig.TLSClientConfig.CAFile = strings.TrimSpace(cfg.CAFile)
	restConfig.TLSClientConfig.CAData = nil
	restConfig.TLSClientConfig.Insecure = cfg.Insecure
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, fmt.Errorf("configure service credentials: %w", err)
	}
	httpClient.Timeout = 0
	return httpClient, nil
}
