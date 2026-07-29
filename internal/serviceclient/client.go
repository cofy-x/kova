package serviceclient

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Config struct {
	BaseURL    string
	Token      string
	Kubeconfig string
	CAFile     string
	Insecure   bool
	HTTPClient *http.Client
}

type Client struct {
	baseURL *url.URL
	http    *http.Client
	token   string
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
	return &Client{baseURL: baseURL, http: httpClient, token: strings.TrimSpace(cfg.Token)}, nil
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
		return nil, fmt.Errorf("service authentication requires KOVA_SERVICE_TOKEN or a kubeconfig")
	} else {
		restConfig = &rest.Config{}
	}
	restConfig.Host = baseURL.String()
	restConfig.TLSClientConfig.CAFile = strings.TrimSpace(cfg.CAFile)
	restConfig.TLSClientConfig.CAData = nil
	restConfig.TLSClientConfig.Insecure = cfg.Insecure
	client, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, fmt.Errorf("configure service credentials: %w", err)
	}
	client.Timeout = 0
	return client, nil
}
