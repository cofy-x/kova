package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var maxFailureLogFileSize int64 = 1 << 30

const truncatedFailureLogSuffix = "\n...[truncated to fit logs file limit]\n"

type failureLogEntry struct {
	Target  string `json:"target"`
	Success bool   `json:"success"`
	Logs    string `json:"logs"`
}

type failureLogStore struct {
	path string
	mu   sync.Mutex
}

func newFailureLogStore(path string) *failureLogStore {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return &failureLogStore{path: path}
}

func (s *failureLogStore) AppendFailure(target string, logs string) error {
	if s == nil || strings.TrimSpace(target) == "" || logs == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}

	payload, err := marshalFailureLogLine(failureLogEntry{
		Target:  target,
		Success: false,
		Logs:    logs,
	})
	if err != nil {
		return err
	}

	currentSize, err := appendFailureLogLine(s.path, payload)
	if err != nil {
		return err
	}
	if currentSize <= maxFailureLogFileSize {
		return nil
	}

	return trimFailureLogFile(s.path, currentSize)
}

func appendFailureLogLine(path string, payload []byte) (int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if _, err := file.Write(payload); err != nil {
		return 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func trimFailureLogFile(path string, currentSize int64) error {
	if currentSize <= maxFailureLogFileSize {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	keepOffset, err := findFailureLogKeepOffset(file, currentSize-maxFailureLogFileSize, currentSize)
	if err != nil {
		return err
	}

	if _, err := file.Seek(keepOffset, io.SeekStart); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()

	if _, err := io.Copy(tempFile, file); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func findFailureLogKeepOffset(file *os.File, trimBytes int64, currentSize int64) (int64, error) {
	if trimBytes <= 0 {
		return 0, nil
	}
	if _, err := file.Seek(trimBytes, io.SeekStart); err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)
	discarded, err := reader.ReadBytes('\n')
	if err == nil {
		return trimBytes + int64(len(discarded)), nil
	}
	if errors.Is(err, io.EOF) {
		return currentSize, nil
	}
	return 0, err
}

func marshalFailureLogLine(entry failureLogEntry) ([]byte, error) {
	payload, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)+1) <= maxFailureLogFileSize {
		return append(payload, '\n'), nil
	}

	truncatedLogs, err := truncateFailureLogPayload(entry.Target, entry.Logs)
	if err != nil {
		return nil, err
	}
	entry.Logs = truncatedLogs
	payload, err = json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)+1) > maxFailureLogFileSize {
		return nil, fmt.Errorf("failure log for %s exceeds maximum file size", entry.Target)
	}

	return append(payload, '\n'), nil
}

func truncateFailureLogPayload(target string, logs string) (string, error) {
	best := ""
	low := 0
	high := len(logs)

	for low <= high {
		mid := low + (high-low)/2
		candidate := logs[:mid] + truncatedFailureLogSuffix
		payload, err := json.Marshal(failureLogEntry{
			Target:  target,
			Success: false,
			Logs:    candidate,
		})
		if err != nil {
			return "", err
		}

		if int64(len(payload)+1) <= maxFailureLogFileSize {
			best = candidate
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if best != "" {
		return best, nil
	}

	payload, err := json.Marshal(failureLogEntry{
		Target:  target,
		Success: false,
		Logs:    truncatedFailureLogSuffix,
	})
	if err != nil {
		return "", err
	}
	if int64(len(payload)+1) > maxFailureLogFileSize {
		return "", fmt.Errorf("failure log metadata for %s exceeds maximum file size", target)
	}

	return truncatedFailureLogSuffix, nil
}

func loadLatestFailureLogs(path string, targets map[string]struct{}) (map[string]string, error) {
	resolvedPath, err := resolveOptionalPath(path)
	if err != nil {
		return nil, err
	}
	if resolvedPath == "" {
		return map[string]string{}, nil
	}

	file, err := os.Open(resolvedPath)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	result := make(map[string]string)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var entry failureLogEntry
			if unmarshalErr := json.Unmarshal(bytes.TrimSpace(line), &entry); unmarshalErr != nil {
				return nil, unmarshalErr
			}
			if !entry.Success {
				if targets == nil {
					result[entry.Target] = entry.Logs
				} else if _, ok := targets[entry.Target]; ok {
					result[entry.Target] = entry.Logs
				}
			}
		}

		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		return nil, err
	}

	return result, nil
}
