package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/buildobservation"
	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
)

func progressTime(second int) *time.Time {
	value := time.Date(2026, 10, 9, 0, 0, second, 0, time.UTC)
	return &value
}
func feedProgress(t *testing.T, p *progressProjection, event progressEvent) {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	// Deliberately split JSON, base64 logs and delimiters across Write calls.
	for len(data) > 0 {
		count := min(7, len(data))
		if _, err := p.Write(data[:count]); err != nil {
			t.Fatal(err)
		}
		data = data[count:]
	}
}
func requireSeconds(t *testing.T, value *int64, want int64) {
	t.Helper()
	if value == nil || *value != want*int64(time.Second) {
		t.Fatalf("nanoseconds = %v, want %v seconds", value, want)
	}
}

func TestProgressProjectionActualIntervalsUnionOverlapAndCache(t *testing.T) {
	var output bytes.Buffer
	p := newProgressProjection(&output)
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{
		{Digest: "run-1", Name: "[stage 1/2] RUN ignored-secret", Started: progressTime(0), Completed: progressTime(8)},
		{Digest: "cache", Name: "[stage 2/2] COPY ignored-target", Started: progressTime(2), Completed: progressTime(2), Cached: true},
		{Digest: "export", Name: "exporting to image", Started: progressTime(6), Completed: progressTime(10)},
	}})
	// v0.31.2 finalize reuses the exporter digest but starts a new interval.
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: "export", Name: "exporting to image", Started: progressTime(12), Completed: progressTime(18)}}, Statuses: []*progressStatus{
		{ID: "pushing layers", Vertex: "export", Started: progressTime(8), Completed: progressTime(14)},
		{ID: "pushing manifest for registry.example/private@sha256:secret", Vertex: "export", Started: progressTime(14), Completed: progressTime(19)},
	}, Logs: []*progressLog{{Data: []byte("plain runner sentinel\n")}}, Warnings: []*progressWarning{{Short: []byte("ordinary warning")}}})
	o := p.finish(nil, false)
	if o.Availability != "observed" || o.VertexCount != 3 || o.VertexIntervalCount != 4 || o.CachedVertexIntervalCount != 1 || o.WorkerSessionsAvailability != "unavailable" {
		t.Fatalf("observation = %#v", o)
	}
	requireSeconds(t, o.VertexUnionNanoseconds, 16)
	requireSeconds(t, o.ExportUnionNanoseconds, 10)
	requireSeconds(t, o.PushUnionNanoseconds, 11)
	requireSeconds(t, o.ExportPushOverlapNanoseconds, 8)
	if output.String() != "plain runner sentinel\nordinary warning\n" {
		t.Fatalf("decoded logs = %q", output.String())
	}
	data, _ := json.Marshal(o)
	for _, secret := range []string{"ignored-secret", "ignored-target", "private", "sha256:secret", "run-1"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("raw identity retained in observation: %s", data)
		}
	}
}

func TestProgressProjectionRepeatedUpdatesDoNotDoubleCount(t *testing.T) {
	p := newProgressProjection(&bytes.Buffer{})
	vertex := &progressVertex{Digest: "v", Name: "RUN", Started: progressTime(1)}
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{vertex}})
	vertex.Completed = progressTime(5)
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{vertex}})
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{vertex}})
	o := p.finish(nil, false)
	if o.Availability != "observed" || o.VertexIntervalCount != 1 || o.ExportAvailability != "unavailable" || o.PushAvailability != "unavailable" || o.ExportPushOverlapNanoseconds != nil {
		t.Fatalf("observation=%#v", o)
	}
	requireSeconds(t, o.VertexUnionNanoseconds, 4)
}

func TestProgressProjectionPushRequiresExporterParentAndIDNotName(t *testing.T) {
	p := newProgressProjection(&bytes.Buffer{})
	feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: "run", Name: "RUN", Started: progressTime(0), Completed: progressTime(1)}}, Statuses: []*progressStatus{{ID: "pushing layers", Vertex: "run", Started: progressTime(0), Completed: progressTime(1)}}})
	o := p.finish(nil, false)
	if o.Availability != "incomplete" || o.PushUnionNanoseconds != nil || o.PushAvailability != "unavailable" {
		t.Fatalf("observation=%#v", o)
	}
}

func TestProgressProjectionRefusalsDoNotBecomeZeroOrParseErrors(t *testing.T) {
	for _, tc := range []struct{ name, data, reason string }{
		{"malformed JSON", "{broken}\n", "malformed_stream"},
		{"bad base64", "{\"logs\":[{\"data\":\"~\"}]}\n", "malformed_stream"},
		{"null vertex", "{\"vertexes\":[null]}\n", "malformed_stream"},
		{"unterminated final line", "{}", "partial_stream"},
		{"ordinary diagnostic", "connection refused\n", "malformed_stream"},
		{"no stream", "", "missing_stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			p := newProgressProjection(&output)
			if count, err := p.Write([]byte(tc.data)); count != len(tc.data) || err != nil {
				t.Fatalf("count=%d error=%v", count, err)
			}
			o := p.finish(nil, false)
			if o.Availability != "unavailable" || o.Reason != tc.reason || o.VertexUnionNanoseconds != nil || o.PushUnionNanoseconds != nil {
				t.Fatalf("observation=%#v", o)
			}
			if tc.name == "ordinary diagnostic" && !strings.Contains(output.String(), "connection refused") {
				t.Fatal("ordinary failure diagnostic lost")
			}
		})
	}
}

func TestProgressProjectionPartialAndCancelledStreams(t *testing.T) {
	for _, tc := range []struct {
		name      string
		complete  bool
		err       error
		cancelled bool
		reason    string
	}{
		{"unfinished vertex", false, nil, false, "partial_stream"},
		{"failed command", true, errors.New("failure"), false, "command_failed"},
		{"cancelled", true, context.Canceled, true, "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newProgressProjection(&bytes.Buffer{})
			vertex := &progressVertex{Digest: "v", Name: "RUN", Started: progressTime(1)}
			if tc.complete {
				vertex.Completed = progressTime(5)
			}
			feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{vertex}})
			o := p.finish(tc.err, tc.cancelled)
			if o.Availability != "incomplete" || o.Reason != tc.reason || (!tc.complete && o.VertexUnionNanoseconds != nil) {
				t.Fatalf("observation=%#v", o)
			}
		})
	}
}

func TestProgressProjectionRejectsBackwardsAndConflictingCompletion(t *testing.T) {
	for _, end := range []int{0, 5} {
		p := newProgressProjection(&bytes.Buffer{})
		feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: "v", Name: "RUN", Started: progressTime(1), Completed: progressTime(4)}}})
		feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: "v", Name: "RUN", Started: progressTime(1), Completed: progressTime(end)}}})
		if o := p.finish(nil, false); o.Availability != "unavailable" || o.Reason != "malformed_stream" {
			t.Fatalf("observation=%#v", o)
		}
	}
}

func TestProgressProjectionAllLimitsAndLogRetentionAreBounded(t *testing.T) {
	t.Run("line", func(t *testing.T) {
		p := newProgressProjection(&bytes.Buffer{})
		_, _ = p.Write(bytes.Repeat([]byte("x"), maxProgressLineBytes+1))
		if len(p.line) != 0 || !p.discardLine || p.finish(nil, false).Availability != "unavailable" {
			t.Fatal("unbounded line")
		}
	})
	t.Run("total bytes", func(t *testing.T) {
		p := newProgressProjection(&bytes.Buffer{})
		p.bytes = maxProgressBytes
		_, _ = p.Write([]byte("{}\n"))
		if o := p.finish(nil, false); o.Reason != "limit_exceeded" || o.VertexUnionNanoseconds != nil {
			t.Fatalf("observation=%#v", o)
		}
	})
	t.Run("events", func(t *testing.T) {
		p := newProgressProjection(&bytes.Buffer{})
		p.events = maxProgressEvents
		_, _ = p.Write([]byte("{}\n"))
		if o := p.finish(nil, false); o.Reason != "limit_exceeded" {
			t.Fatalf("observation=%#v", o)
		}
	})
	t.Run("vertices", func(t *testing.T) {
		p := newProgressProjection(&bytes.Buffer{})
		for i := 0; i <= buildobservation.MaxVertices; i++ {
			feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: fmt.Sprintf("v-%d", i), Name: "RUN", Started: progressTime(0), Completed: progressTime(1)}}})
		}
		if o := p.finish(nil, false); o.Reason != "limit_exceeded" || p.vertices != nil || p.intervals != nil {
			t.Fatalf("observation=%#v", o)
		}
	})
	t.Run("intervals", func(t *testing.T) {
		p := newProgressProjection(&bytes.Buffer{})
		for i := 0; i <= buildobservation.MaxIntervals; i++ {
			start := progressTime(0).Add(time.Duration(i) * time.Nanosecond)
			end := start.Add(time.Second)
			feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Digest: "v", Name: "RUN", Started: &start, Completed: &end}}})
		}
		if o := p.finish(nil, false); o.Reason != "limit_exceeded" {
			t.Fatalf("observation=%#v", o)
		}
	})
	t.Run("logs", func(t *testing.T) {
		var output boundedTailBuffer
		p := newProgressProjection(&output)
		for range 5 {
			feedProgress(t, p, progressEvent{Logs: []*progressLog{{Data: bytes.Repeat([]byte("x"), maxBuildOutputBytes/2)}}})
		}
		feedProgress(t, p, progressEvent{Logs: []*progressLog{{Data: []byte("final connection refused diagnostic\n")}}})
		if len(output.String()) > maxBuildOutputBytes+100 || !strings.Contains(output.String(), "earlier command output truncated") || !strings.Contains(output.String(), "final connection refused diagnostic") || !output.ContainsConnectionRefused() {
			t.Fatalf("tail diagnostics missing or unbounded: %d", len(output.String()))
		}
	})
}

func TestProgressProjectionLimitsDoNotSuppressFinalFailureDiagnostics(t *testing.T) {
	for _, budget := range []string{"bytes", "events"} {
		t.Run(budget, func(t *testing.T) {
			var output boundedTailBuffer
			p := newProgressProjection(&output)
			if budget == "bytes" {
				p.bytes = maxProgressBytes
			} else {
				p.events = maxProgressEvents
			}
			_, _ = p.Write([]byte("{}\n"))
			feedProgress(t, p, progressEvent{Logs: []*progressLog{{Data: []byte("decoded final connection refused\n")}}})
			feedProgress(t, p, progressEvent{Vertexes: []*progressVertex{{Error: "terminal failure error"}}})
			_, _ = p.Write([]byte("ordinary terminal connection refused"))
			o := p.finish(errors.New("failed"), false)
			if o.Availability != "unavailable" || o.Reason != "limit_exceeded" || p.vertices != nil || p.intervals != nil || !output.ContainsConnectionRefused() || !strings.Contains(output.String(), "decoded final connection refused") || !strings.Contains(output.String(), "terminal failure error") || !strings.Contains(output.String(), "ordinary terminal connection refused") {
				t.Fatalf("observation=%#v diagnostics=%q", o, output.String())
			}
		})
	}
}

func TestOptionalProgressCannotFailSuccessfulDigestAndPreservesPlainLogs(t *testing.T) {
	installFakeBuildCommands(t)
	want := "sha256:" + strings.Repeat("a", 64)
	t.Setenv("FAKE_BUILDKIT_DIGEST", want)
	t.Setenv("FAKE_BUILDKIT_PROGRESS", "{malformed}")
	var output bytes.Buffer
	digest, observation, err := runObservedBuildCommands(context.Background(), source.Spec{Target: "registry.example/app:dev", Format: source.BuildFormatOCI}, &scheduler.Addr{Addr: "tcp://buildkit:1234"}, Options{}, &output)
	if err != nil || digest != want || observation.Availability != "unavailable" || observation.Reason != "malformed_stream" {
		t.Fatalf("digest=%s observation=%#v error=%v", digest, observation, err)
	}
}

func TestBuildObservationTravelsThroughRealSubprocessAndEntry(t *testing.T) {
	dir := installFakeBuildCommands(t)
	want := "sha256:" + strings.Repeat("a", 64)
	t.Setenv("FAKE_BUILDKIT_DIGEST", want)
	argsPath := filepath.Join(dir, "buildctl-args")
	t.Setenv("FAKE_BUILDKIT_ARGS", argsPath)
	t.Setenv("FAKE_BUILDKIT_PROGRESS", `{"vertexes":[{"digest":"v","name":"RUN","started":"2026-10-09T00:00:00Z","completed":"2026-10-09T00:00:02Z"}],"logs":[{"data":"c2VudGluZWwK"}]}`)
	entry := executeBuild(context.Background(), source.Spec{Target: "registry.example/app:dev", Format: source.BuildFormatOCI}, &scheduler.Addr{Addr: "tcp://buildkit:1234"}, Options{})
	if !entry.Success || entry.ManifestDigest != want || entry.BuildObservation.Availability != "observed" {
		t.Fatalf("entry=%#v", entry)
	}
	requireSeconds(t, entry.BuildObservation.VertexUnionNanoseconds, 2)
	args, err := os.ReadFile(argsPath)
	if err != nil || !bytes.Contains(args, []byte("--progress=rawjson")) {
		t.Fatalf("args=%s error=%v", args, err)
	}
	data, err := json.Marshal(boundedSummaryEntry(entry))
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		BuildObservation *buildobservation.Observation `json:"build_observation"`
	}
	if err := json.Unmarshal(data, &summary); err != nil || summary.BuildObservation.Availability != "observed" {
		t.Fatalf("summary=%s error=%v", data, err)
	}
}

func TestNydusObservationIsCommandWallOnly(t *testing.T) {
	dir := installFakeBuildCommands(t)
	t.Setenv("FAKE_BUILDKIT_DIGEST", "sha256:"+strings.Repeat("a", 64))
	t.Setenv("FAKE_NYDUS_DIGEST", "sha256:"+strings.Repeat("b", 64))
	t.Setenv("FAKE_NYDUS_ARGS", filepath.Join(dir, "nydus-args"))
	var output boundedTailBuffer
	_, observation, err := runObservedBuildCommands(context.Background(), source.Spec{Target: "registry.example/app:dev_nydus_v3", Format: source.BuildFormatNydus}, &scheduler.Addr{Addr: "tcp://buildkit:1234"}, Options{}, &output)
	if err != nil || observation.Availability != "unavailable" || observation.Reason != "missing_stream" || observation.NydusAvailability != "observed" || observation.NydusWallNanoseconds == nil || observation.PushUnionNanoseconds != nil || observation.WorkerSessionsAvailability != "unavailable" {
		t.Fatalf("observation=%#v error=%v", observation, err)
	}
}

func TestNydusFailureRetainsOCIObservationButCannotClaimComplete(t *testing.T) {
	dir := installFakeBuildCommands(t)
	t.Setenv("FAKE_BUILDKIT_DIGEST", "sha256:"+strings.Repeat("a", 64))
	t.Setenv("FAKE_BUILDKIT_PROGRESS", `{"vertexes":[{"digest":"v","name":"RUN","started":"2026-10-09T00:00:00Z","completed":"2026-10-09T00:00:02Z"}]}`)
	t.Setenv("FAKE_NYDUS_DIGEST", "")
	t.Setenv("FAKE_NYDUS_ARGS", filepath.Join(dir, "nydus-args"))
	var output boundedTailBuffer
	_, observation, err := runObservedBuildCommands(context.Background(), source.Spec{Target: "registry.example/app:dev_nydus_v3", Format: source.BuildFormatNydus}, &scheduler.Addr{Addr: "tcp://buildkit:1234"}, Options{}, &output)
	if err == nil || observation.Availability != "incomplete" || observation.Reason != "command_failed" || observation.NydusAvailability != "incomplete" || observation.NydusWallNanoseconds == nil {
		t.Fatalf("observation=%#v error=%v", observation, err)
	}
	requireSeconds(t, observation.VertexUnionNanoseconds, 2)
}
