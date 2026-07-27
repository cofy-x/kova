package observability

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

const (
	AttrComponent      = "kova.component"
	AttrOperation      = "kova.operation"
	AttrResult         = "kova.result"
	AttrErrorClass     = "kova.error_class"
	AttrTarget         = "kova.target"
	AttrMode           = "kova.mode"
	AttrWorkerAddr     = "kova.worker_addr"
	AttrNodeIP         = "kova.node_ip"
	AttrAttempt        = "kova.attempt"
	AttrNamespace      = "kova.namespace"
	AttrPod            = "kova.pod"
	AttrHTTPStatusCode = "http.status_code"
)

var sensitiveKeyFragments = []string{
	"token",
	"secret",
	"password",
	"private_key",
	"public_key",
	"authorized_key",
	"auth",
	"stdin",
	"stdout",
	"stderr",
}

func StringAttr(key, value string) attribute.KeyValue {
	if SensitiveKey(key) {
		return attribute.String(key, "[redacted]")
	}
	return attribute.String(key, SanitizeValue(value))
}

func SensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	for _, fragment := range sensitiveKeyFragments {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

func SanitizeValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 256 {
		return value
	}
	return value[:256] + "...[truncated]"
}

func SanitizeLogBody(value string) string {
	value = strings.TrimSpace(value)
	normalized := strings.ToLower(strings.ReplaceAll(value, "-", "_"))
	for _, fragment := range sensitiveKeyFragments {
		if strings.Contains(normalized, fragment) {
			return "[redacted]"
		}
	}
	return SanitizeValue(value)
}

func SafeAttrs(attrs ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if SensitiveKey(string(attr.Key)) {
			out = append(out, attribute.String(string(attr.Key), "[redacted]"))
			continue
		}
		out = append(out, attr)
	}
	return out
}
