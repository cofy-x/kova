package buildobservation

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeFixedFiniteObservationAndDeepCopy(t *testing.T) {
	value := int64(17)
	o := &Observation{SchemaVersion: 1, Availability: "observed", VertexCount: 1, VertexIntervalCount: 1, VertexUnionNanoseconds: &value, ExportAvailability: "unavailable", PushAvailability: "unavailable", NydusAvailability: "unavailable", WorkerSessionsAvailability: "unavailable"}
	copy := Normalize(o)
	*copy.VertexUnionNanoseconds = 18
	if *o.VertexUnionNanoseconds != 17 || copy.Availability != "observed" {
		t.Fatal("copy aliases input or lost availability")
	}
	data, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	decoded := Decode(data)
	if decoded.Availability != "observed" || *decoded.VertexUnionNanoseconds != 17 {
		t.Fatalf("decoded=%#v", decoded)
	}
}

func TestInvalidOptionalObservationBecomesUnavailable(t *testing.T) {
	for _, raw := range []string{
		`true`, `[]`, `{"schemaVersion":2}`, `{"schemaVersion":1,"availability":"COMPLETE"}`,
		`{"schemaVersion":1,"availability":"unavailable","reason":"bearer secret"}`,
		`{"schemaVersion":1,"availability":"unavailable","reason":"missing_stream","exportAvailability":"unavailable","pushAvailability":"unavailable","nydusAvailability":"unavailable","workerSessionsAvailability":"observed"}`,
		`{"schemaVersion":1,"availability":"observed","vertexCount":1,"vertexIntervalCount":1,"vertexUnionNanoseconds":-1,"exportAvailability":"unavailable","pushAvailability":"unavailable","nydusAvailability":"unavailable","workerSessionsAvailability":"unavailable"}`,
		`{"schemaVersion":1,"availability":"observed","vertexCount":1,"vertexIntervalCount":1,"vertexUnionNanoseconds":86400000000001,"exportAvailability":"unavailable","pushAvailability":"unavailable","nydusAvailability":"unavailable","workerSessionsAvailability":"unavailable"}`,
		`{"schemaVersion":1,"availability":"observed","vertexCount":1,"vertexIntervalCount":1,"vertexUnionNanoseconds":NaN}`, strings.Repeat("x", 4097),
	} {
		o := Decode([]byte(raw))
		if o.Availability != "unavailable" || o.Reason != "invalid_observation" || o.VertexUnionNanoseconds != nil {
			t.Fatalf("input=%s observation=%#v", raw, o)
		}
	}
}

func TestOldEntryMayHaveNoObservation(t *testing.T) {
	if Decode(nil) != nil || Decode([]byte("null")) != nil {
		t.Fatal("legacy missing observation invented")
	}
}
