package admissionjson

import "testing"

func testAllowed(path []string, key string) bool {
	switch len(path) {
	case 0:
		return key == "active"
	case 1:
		return path[0] == "active" && key == "id"
	case 2:
		return path[0] == "active" && path[1] == "id" && key == "nonce"
	}
	return false
}

func TestDecodePreservesCompatibleJSONAndRejectsLossyFields(t *testing.T) {
	var result map[string]any
	for _, raw := range []string{
		` { "active" : { "id" : {"nonce":"a"} } } `,
		`{"active":{"id":{"nonce":"\u003c"}}}`,
		`{"active":{"id":{"nonce":"\ud83d\ude00"}}}`,
	} {
		if err := Decode([]byte(raw), &result, testAllowed); err != nil {
			t.Fatalf("legal spelling %s rejected: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"active":{},"active":{}}`,
		`{"active":{"id":{},"id":{}}}`,
		`{"active":{"id":{"nonce":"a","nonce":"b"}}}`,
		`{"active":{},"\u0061ctive":{}}`,
		`{"active":{"id":{"nonce":null}}}`,
		`{"active":{"id":{"nonce":"\ud800"}}}`,
		`{"active":{"id":{"nonce":"\udc00"}}}`,
		`{"active":{"id":{"nonce":"\ud800\u0041"}}}`,
		`{"active":{"id":{"unknown":"a"}}}`,
	} {
		if err := Decode([]byte(raw), &result, testAllowed); err == nil {
			t.Fatalf("lossy JSON %s accepted", raw)
		}
	}
}
