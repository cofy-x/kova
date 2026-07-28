package ctxconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	configDirName  = "kova"
	configFileName = "config.json"
	ModeDirect     = "direct"
	ModeService    = "service"
)

type Config struct {
	Current  string             `json:"current,omitempty"`
	Contexts map[string]Context `json:"contexts,omitempty"`
}

type Context struct {
	Mode                  string `json:"mode"`
	Kubeconfig            string `json:"kubeconfig,omitempty"`
	Namespace             string `json:"namespace,omitempty"`
	BuildkitAddr          string `json:"buildkitAddr,omitempty"`
	RunnerImage           string `json:"runnerImage,omitempty"`
	RunnerImagePullPolicy string `json:"runnerImagePullPolicy,omitempty"`
	ImagePullSecret       string `json:"imagePullSecret,omitempty"`
	ServiceURL            string `json:"serviceURL,omitempty"`
	ServiceCAFile         string `json:"serviceCAFile,omitempty"`
	ServiceInsecure       bool   `json:"serviceInsecure,omitempty"`
}

func (c Context) EffectiveMode() string {
	if strings.TrimSpace(c.Mode) == "" {
		return ModeDirect
	}
	return strings.TrimSpace(c.Mode)
}

func (c Context) Validate() error {
	switch c.EffectiveMode() {
	case ModeDirect:
		return nil
	case ModeService:
		if strings.TrimSpace(c.ServiceURL) == "" {
			return fmt.Errorf("service context requires a service URL")
		}
		return nil
	default:
		return fmt.Errorf("unsupported context mode %q", c.Mode)
	}
}

func DefaultPath() (string, error) {
	configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
	if configHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		configHome = filepath.Join(home, ".config")
	}
	return filepath.Join(configHome, configDirName, configFileName), nil
}

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		resolved, err := DefaultPath()
		if err != nil {
			return Config{}, err
		}
		path = resolved
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{Contexts: map[string]Context{}}, nil
		}
		return Config{}, fmt.Errorf("read ctx config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode ctx config: %w", err)
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]Context{}
	}
	return cfg, nil
}

func Save(path string, cfg Config) error {
	if strings.TrimSpace(path) == "" {
		resolved, err := DefaultPath()
		if err != nil {
			return err
		}
		path = resolved
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]Context{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create ctx config directory: %w", err)
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode ctx config: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write ctx config: %w", err)
	}
	return nil
}

func ValidateName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("ctx name is required")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return "", fmt.Errorf("ctx name %q contains unsupported character %q", name, r)
	}
	return name, nil
}

func (c Config) Names() []string {
	names := make([]string, 0, len(c.Contexts))
	for name := range c.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c Config) Resolve(name string) (string, Context, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = c.Current
	}
	if name == "" {
		return "", Context{}, false
	}
	ctx, ok := c.Contexts[name]
	return name, ctx, ok
}
