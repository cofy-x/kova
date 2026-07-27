package source

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var kovaVarPattern = regexp.MustCompile(`\$\{?(KOVA_[A-Za-z0-9_]+)\}?`)

func replaceBuildVariablesInFile(path string, buildVars map[string]string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	replaced, err := replaceBuildVariables(raw, path, buildVars)
	if err != nil {
		return err
	}
	if bytes.Equal(raw, replaced) {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, replaced, info.Mode())
}

func replaceBuildVariables(raw []byte, path string, buildVars map[string]string) ([]byte, error) {
	if len(buildVars) == 0 {
		return raw, nil
	}

	content := string(raw)
	for key, value := range buildVars {
		dollarToken := "$" + key
		braceToken := "${" + key + "}"
		content = strings.ReplaceAll(content, braceToken, value)
		content = strings.ReplaceAll(content, dollarToken, value)
	}

	unresolved := findUnresolvedBuildVariables(content)
	if len(unresolved) > 0 {
		missing := make([]string, 0, len(unresolved))
		for _, token := range unresolved {
			if _, ok := buildVars[token]; !ok {
				missing = append(missing, token)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, fmt.Errorf("%s contains unresolved build variable(s): %s", path, strings.Join(uniqueStrings(missing), ", "))
		}
	}

	return []byte(content), nil
}

func ParseBuildVariables(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}

	buildVars := make(map[string]string, len(items))
	for _, item := range items {
		key, value, found := strings.Cut(item, "=")
		if !found {
			return nil, fmt.Errorf("invalid --var %q, expected KEY=value", item)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("invalid --var %q, key must not be empty", item)
		}
		if !strings.HasPrefix(key, "KOVA_") {
			return nil, fmt.Errorf("invalid --var %q, key must start with KOVA_", item)
		}
		buildVars[key] = value
	}
	return buildVars, nil
}

func findUnresolvedBuildVariables(content string) []string {
	matches := kovaVarPattern.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return nil
	}
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			result = append(result, match[1])
		}
	}
	return result
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	last := ""
	for _, value := range values {
		if value == last {
			continue
		}
		result = append(result, value)
		last = value
	}
	return result
}
