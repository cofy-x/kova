package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

func newJobID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func defaultString(value string, def string) string {
	if strings.TrimSpace(value) == "" {
		return def
	}
	return value
}
