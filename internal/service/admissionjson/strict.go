// Package admissionjson checks persisted admission JSON before a typed decode.
// Unknown or duplicate fields must not be discarded by a later ledger write.
package admissionjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// Allowed reports whether an object key at path belongs to its exact schema.
// Paths include dynamic map keys; callers validate those keys after decoding.
type Allowed func(path []string, key string) bool

func Decode(raw []byte, result any, allowed Allowed) error {
	if !utf8.Valid(raw) || !validSurrogates(raw) {
		return fmt.Errorf("admission JSON has invalid Unicode")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := checkValue(decoder, nil, allowed, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("admission JSON has trailing data")
	}
	return json.Unmarshal(raw, result)
}

func checkValue(decoder *json.Decoder, path []string, allowed Allowed, depth int) error {
	if depth > 5 {
		return fmt.Errorf("admission JSON exceeds nesting limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("admission JSON has null field")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || !allowed(path, key) {
				return fmt.Errorf("admission JSON has unknown field")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("admission JSON has duplicate field")
			}
			seen[key] = struct{}{}
			if err := checkValue(decoder, append(path, key), allowed, depth+1); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := checkValue(decoder, path, allowed, depth+1); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	default:
		return fmt.Errorf("admission JSON has invalid container")
	}
}

// encoding/json replaces malformed surrogate escapes with U+FFFD. Reject
// those spellings so decoding and rewriting cannot silently change an ID.
func validSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		for i++; i < len(raw); i++ {
			if raw[i] == '"' {
				break
			}
			if raw[i] != '\\' {
				continue
			}
			i++
			if i >= len(raw) {
				return false
			}
			if raw[i] != 'u' {
				continue
			}
			value, ok := hex4(raw, i+1)
			if !ok {
				return false
			}
			i += 4
			if value >= 0xdc00 && value <= 0xdfff {
				return false
			}
			if value >= 0xd800 && value <= 0xdbff {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return false
				}
				low, ok := hex4(raw, i+3)
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			}
		}
		if i >= len(raw) {
			return false
		}
	}
	return true
}

func hex4(raw []byte, start int) (uint16, bool) {
	if start+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, b := range raw[start : start+4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value |= uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value |= uint16(b-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
