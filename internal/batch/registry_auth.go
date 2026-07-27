package batch

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type registryCredentials map[string]dockerAuthEntry

type dockerAuthConfig struct {
	Auths map[string]dockerAuthEntry `json:"auths"`
}

type dockerAuthEntry struct {
	Auth     string `json:"auth"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func loadRegistryAuth(configPath string) (registryCredentials, error) {
	path := configPath
	if path == "" {
		path = defaultDockerConfigPath()
	}
	if path == "" {
		return registryCredentials{}, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return registryCredentials{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read docker config: %w", err)
	}
	var config dockerAuthConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("decode docker config: %w", err)
	}
	credentials := make(registryCredentials, len(config.Auths))
	for registry, entry := range config.Auths {
		if entry.Username == "" && entry.Password == "" && entry.Auth != "" {
			decoded, decodeErr := base64.StdEncoding.DecodeString(entry.Auth)
			if decodeErr != nil {
				return nil, fmt.Errorf("decode docker auth for %q: %w", registry, decodeErr)
			}
			var found bool
			entry.Username, entry.Password, found = strings.Cut(string(decoded), ":")
			if !found {
				return nil, fmt.Errorf("decode docker auth for %q: credential does not contain a username/password separator", registry)
			}
		}
		credentials[normalizeRegistry(registry)] = entry
	}
	return credentials, nil
}

func defaultDockerConfigPath() string {
	if dir := strings.TrimSpace(os.Getenv("DOCKER_CONFIG")); dir != "" {
		return filepath.Join(dir, "config.json")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".docker", "config.json")
}

func (credentials registryCredentials) forURL(rawURL string) (string, string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", ""
	}
	entry, ok := credentials[normalizeRegistry(parsed.Host)]
	if !ok {
		return "", ""
	}
	return entry.Username, entry.Password
}

func normalizeRegistry(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	return strings.TrimSuffix(value, "/")
}
