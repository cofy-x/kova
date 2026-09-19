package v1

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestOpenAPIContractMatchesPublicTypes(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate contract test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if document["openapi"] != "3.1.0" {
		t.Fatalf("openapi=%v", document["openapi"])
	}
	paths := object(t, document, "paths")
	for _, operation := range []struct{ path, method string }{
		{path: "/version", method: "get"}, {path: "/readyz", method: "get"},
		{path: "/v1/builds", method: "post"}, {path: "/v1/builds", method: "get"},
		{path: "/v1/builds/{id}", method: "get"}, {path: "/v1/builds/{id}/results", method: "get"},
		{path: "/v1/builds/{id}/logs", method: "get"}, {path: "/v1/builds/{id}/cancel", method: "post"},
	} {
		if _, ok := object(t, paths, operation.path)[operation.method]; !ok {
			t.Errorf("OpenAPI is missing %s %s", strings.ToUpper(operation.method), operation.path)
		}
	}
	schemas := object(t, object(t, document, "components"), "schemas")
	for name, model := range map[string]any{
		"CreateBuildRequest": CreateBuildRequest{}, "VersionInfo": VersionInfo{},
		"ReadyStatus": ReadyStatus{}, "BuildJob": BuildJob{}, "BuildOutput": BuildOutput{},
		"BuildResults": BuildResults{}, "JobList": JobList{}, "ErrorResponse": ErrorResponse{},
	} {
		want := jsonFieldNames(reflect.TypeOf(model))
		properties := object(t, object(t, schemas, name), "properties")
		got := make([]string, 0, len(properties))
		for property := range properties {
			got = append(got, property)
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s properties=%v, want %v", name, got, want)
		}
	}
	assertStringEnum(t, object(t, object(t, object(t, schemas, "VersionInfo"), "properties"), "api_version"), []string{APIVersion})
	assertStringEnum(t, object(t, object(t, object(t, schemas, "BuildJob"), "properties"), "status"), []string{
		string(JobStatusCancelled), string(JobStatusFailed), string(JobStatusQueued),
		string(JobStatusRunning), string(JobStatusStarting), string(JobStatusSucceeded),
	})
	assertStringEnum(t, object(t, object(t, object(t, schemas, "BuildJob"), "properties"), "failure_code"), []string{
		string(BuildFailureCancelled), string(BuildFailureExecutionFailed), string(BuildFailureInvalidSource),
		string(BuildFailureInvalidTargets), string(BuildFailureRunnerUnavailable), string(BuildFailureSubmissionFailed),
		string(BuildFailureVerificationFailed),
	})
	assertStringEnum(t, object(t, object(t, object(t, schemas, "ErrorResponse"), "properties"), "code"), []string{
		string(ErrorCodeConflict), string(ErrorCodeForbidden), string(ErrorCodeInternal), string(ErrorCodeInvalidRequest),
		string(ErrorCodeLogsUnavailable), string(ErrorCodeNotFound), string(ErrorCodeQueueCapacityExceeded), string(ErrorCodeUnauthenticated),
	})
	createProperties := object(t, object(t, schemas, "CreateBuildRequest"), "properties")
	assertInteger(t, object(t, createProperties, "source_uri")["maxLength"], MaxSourceURILength)
	assertInteger(t, object(t, createProperties, "targets")["maxItems"], MaxLogicalTargets)
	assertInteger(t, object(t, object(t, createProperties, "targets"), "items")["maxLength"], MaxTargetLength)
	assertInteger(t, object(t, createProperties, "concurrency")["maximum"], MaxBuildConcurrency)
	assertInteger(t, object(t, createProperties, "variables")["maxItems"], MaxBuildVariables)
	assertInteger(t, object(t, object(t, createProperties, "variables"), "items")["maxLength"], MaxBuildVariableLength)
	assertInteger(t, object(t, createProperties, "idempotency_key")["maxLength"], MaxIdempotencyKeyLength)
	assertInteger(t, object(t, object(t, object(t, schemas, "JobList"), "properties"), "jobs")["maxItems"], MaxListBuildsPageSize)
	assertInteger(t, object(t, object(t, object(t, schemas, "BuildResults"), "properties"), "outputs")["maxItems"], MaxConcreteOutputs)
	logsOperation := object(t, object(t, paths, "/v1/builds/{id}/logs"), "get")
	assertParameterMaximum(t, logsOperation["parameters"], "tail_lines", MaxLogTailLines)
	listOperation := object(t, object(t, paths, "/v1/builds"), "get")
	assertParameterMaximum(t, listOperation["parameters"], "limit", MaxListBuildsPageSize)
	for schema, code := range map[string]ErrorCode{
		"ConflictError": ErrorCodeConflict, "ForbiddenError": ErrorCodeForbidden,
		"InternalAPIError": ErrorCodeInternal, "InvalidRequestError": ErrorCodeInvalidRequest,
		"LogsUnavailableError": ErrorCodeLogsUnavailable, "NotFoundError": ErrorCodeNotFound,
		"QueueCapacityExceededError": ErrorCodeQueueCapacityExceeded, "UnauthenticatedError": ErrorCodeUnauthenticated,
	} {
		assertErrorSchemaCode(t, object(t, schemas, schema), code)
	}
}

func assertErrorSchemaCode(t *testing.T, schema map[string]any, want ErrorCode) {
	t.Helper()
	allOf, ok := schema["allOf"].([]any)
	if !ok || len(allOf) != 2 {
		t.Fatalf("allOf=%v", schema["allOf"])
	}
	constraint, ok := allOf[1].(map[string]any)
	if !ok {
		t.Fatalf("constraint=%T", allOf[1])
	}
	got := object(t, object(t, constraint, "properties"), "code")["const"]
	if got != string(want) {
		t.Fatalf("error code=%v, want %s", got, want)
	}
}

func assertParameterMaximum(t *testing.T, raw any, name string, want int) {
	t.Helper()
	parameters, ok := raw.([]any)
	if !ok {
		t.Fatalf("parameters=%T", raw)
	}
	for _, rawParameter := range parameters {
		parameter, ok := rawParameter.(map[string]any)
		if !ok || parameter["name"] != name {
			continue
		}
		assertInteger(t, object(t, parameter, "schema")["maximum"], want)
		return
	}
	t.Fatalf("parameter %q not found", name)
}

func assertInteger(t *testing.T, got any, want int) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("integer=%v, want %d", got, want)
	}
}

func object(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object", key)
	}
	return value
}

func jsonFieldNames(model reflect.Type) []string {
	fields := make([]string, 0, model.NumField())
	for index := 0; index < model.NumField(); index++ {
		name := strings.Split(model.Field(index).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	return fields
}

func assertStringEnum(t *testing.T, schema map[string]any, want []string) {
	t.Helper()
	raw, ok := schema["enum"].([]any)
	if !ok {
		t.Fatalf("enum=%T", schema["enum"])
	}
	got := make([]string, 0, len(raw))
	for _, value := range raw {
		got = append(got, value.(string))
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enum=%v, want %v", got, want)
	}
}
