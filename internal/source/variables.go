package source

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	kovaVarPattern           = regexp.MustCompile(`\$\{?(KOVA_[A-Za-z0-9_]+)\}?`)
	buildVariableNamePattern = regexp.MustCompile(`^KOVA_[A-Za-z0-9_]+$`)
)

func replaceBuildVariablesInFile(path string, buildVars map[string]string) error {
	raw, err := readBoundedBuildFile(path)
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
	limit, tooLarge := buildFileLimit(path)
	if int64(len(raw)) > limit {
		return nil, tooLarge
	}
	if len(buildVars) == 0 {
		return raw, nil
	}

	content := string(raw)
	keys := make([]string, 0, len(buildVars))
	for key := range buildVars {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := buildVars[key]
		dollarToken := "$" + key
		braceToken := "${" + key + "}"
		var err error
		content, err = replaceAllBounded(content, braceToken, value, limit, tooLarge)
		if err != nil {
			return nil, err
		}
		content, err = replaceAllBounded(content, dollarToken, value, limit, tooLarge)
		if err != nil {
			return nil, err
		}
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

func buildFileLimit(path string) (int64, error) {
	if filepath.Base(path) == "Dockerfile" {
		return MaxDockerfileBytes, ErrDockerfileTooLarge
	}
	return maxArchiveMetadataBytes, ErrMetadataTooLarge
}

func readBoundedBuildFile(path string) ([]byte, error) {
	limit, tooLarge := buildFileLimit(path)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, tooLarge
	}
	return raw, nil
}

func replaceAllBounded(content, token, value string, limit int64, tooLarge error) (string, error) {
	if len(value) > len(token) {
		growth := int64(len(value) - len(token))
		if int64(strings.Count(content, token)) > (limit-int64(len(content)))/growth {
			return "", tooLarge
		}
	}
	return strings.ReplaceAll(content, token, value), nil
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
		if key != strings.TrimSpace(key) || !buildVariableNamePattern.MatchString(key) {
			return nil, fmt.Errorf("invalid --var %q, key must match KOVA_[A-Za-z0-9_]+", item)
		}
		if _, exists := buildVars[key]; exists {
			return nil, fmt.Errorf("build variable %q is duplicated", key)
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
