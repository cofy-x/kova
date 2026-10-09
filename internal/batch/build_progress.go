package batch

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cofy-x/kova/internal/buildobservation"
)

const (
	maxProgressBytes     = 64 << 20
	maxProgressLineBytes = 1 << 20
	maxProgressEvents    = 65536
)

// These structs project the pinned BuildKit v0.31.2 client/graph.go schema.
// RawJSON progress is SolveStatus JSON on stderr; []byte log data is base64.
type progressEvent struct {
	Vertexes []*progressVertex  `json:"vertexes"`
	Statuses []*progressStatus  `json:"statuses"`
	Logs     []*progressLog     `json:"logs"`
	Warnings []*progressWarning `json:"warnings"`
}
type progressVertex struct {
	Digest    string     `json:"digest"`
	Name      string     `json:"name"`
	Started   *time.Time `json:"started"`
	Completed *time.Time `json:"completed"`
	Cached    bool       `json:"cached"`
	Error     string     `json:"error"`
}
type progressStatus struct {
	ID        string     `json:"id"`
	Vertex    string     `json:"vertex"`
	Started   *time.Time `json:"started"`
	Completed *time.Time `json:"completed"`
}
type progressLog struct {
	Data []byte `json:"data"`
}
type progressWarning struct {
	Short  []byte   `json:"short"`
	Detail [][]byte `json:"detail"`
}
type diagnosticEvent struct {
	Logs     []*progressLog     `json:"logs"`
	Warnings []*progressWarning `json:"warnings"`
	Vertexes []*struct {
		Error string `json:"error"`
	} `json:"vertexes"`
}
type progressVertexState struct {
	export  bool
	started bool
}
type progressIntervalKey struct {
	vertex   [32]byte
	status   [32]byte
	start    time.Time
	isStatus bool
}
type progressInterval struct {
	start, end       time.Time
	complete, cached bool
}
type timeInterval struct{ start, end time.Time }

// progressProjection is an optional bounded streaming observation. Write always
// consumes output, even after refusal, so telemetry cannot fail a successful
// artifact build or backpressure a subprocess by returning a parse error.
type progressProjection struct {
	mu            sync.Mutex
	output        io.Writer
	line          []byte
	discardLine   bool
	bytes, events int
	reason        string
	vertices      map[[32]byte]progressVertexState
	intervals     map[progressIntervalKey]progressInterval
}

func newProgressProjection(output io.Writer) *progressProjection {
	return &progressProjection{output: output, vertices: make(map[[32]byte]progressVertexState), intervals: make(map[progressIntervalKey]progressInterval)}
}

func (p *progressProjection) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	written := len(data)
	if p.bytes <= maxProgressBytes && p.bytes > maxProgressBytes-len(data) {
		p.refuse("limit_exceeded")
		p.writeLog([]byte("[build observation byte limit reached; diagnostics continue]\n"))
		p.bytes = maxProgressBytes + 1
	} else if p.bytes <= maxProgressBytes {
		p.bytes += len(data)
	}
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		part := data
		if end >= 0 {
			part = data[:end]
		}
		if !p.discardLine {
			if len(p.line)+len(part) > maxProgressLineBytes {
				p.refuse("limit_exceeded")
				p.line = p.line[:0]
				p.discardLine = true
				p.writeLog([]byte("[oversize build progress line discarded]\n"))
			} else {
				p.line = append(p.line, part...)
			}
		}
		if end < 0 {
			break
		}
		if !p.discardLine {
			p.consumeLine(p.line)
		}
		p.line = p.line[:0]
		p.discardLine = false
		data = data[end+1:]
	}
	return written, nil
}

func (p *progressProjection) refuse(reason string) {
	if p.reason == "" {
		p.reason = reason
	}
	p.vertices = nil
	p.intervals = nil
}

func (p *progressProjection) writeLog(data []byte) {
	// The existing bounded tail sink keeps final diagnostics and detects a
	// connection-refused hint even when that text precedes the retained tail.
	_, _ = p.output.Write(data)
}

func (p *progressProjection) consumeLine(line []byte) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return
	}
	if p.events <= maxProgressEvents {
		p.events++
	}
	if p.events > maxProgressEvents && p.reason == "" {
		p.refuse("limit_exceeded")
		p.writeLog([]byte("[build observation event limit reached; diagnostics continue]\n"))
	}
	// buildctl may interleave ordinary CLI diagnostics. Preserve them, but never
	// silently call a mixed/non-JSON progress stream fully observed.
	if trimmed[0] != '{' {
		p.refuse("malformed_stream")
		p.writeLog(line)
		p.writeLog([]byte("\n"))
		return
	}
	var diagnostics diagnosticEvent
	if err := json.Unmarshal(trimmed, &diagnostics); err != nil {
		p.refuse("malformed_stream")
		p.writeLog([]byte("[malformed build progress record discarded]\n"))
		return
	}
	for _, log := range diagnostics.Logs {
		if log == nil {
			p.refuse("malformed_stream")
			continue
		}
		p.writeLog(log.Data)
	}
	for _, warning := range diagnostics.Warnings {
		if warning == nil {
			p.refuse("malformed_stream")
			continue
		}
		p.writeLog(warning.Short)
		p.writeLog([]byte("\n"))
		for _, detail := range warning.Detail {
			p.writeLog(detail)
			p.writeLog([]byte("\n"))
		}
	}
	for _, vertex := range diagnostics.Vertexes {
		if vertex != nil && vertex.Error != "" {
			p.writeLog([]byte(vertex.Error))
			p.writeLog([]byte("\n"))
		}
	}
	if p.reason != "" {
		return
	}
	var event progressEvent
	if err := json.Unmarshal(trimmed, &event); err != nil {
		p.refuse("malformed_stream")
		return
	}
	for _, vertex := range event.Vertexes {
		if vertex == nil || !validProgressIdentity(vertex.Digest) {
			p.refuse("malformed_stream")
			return
		}
		id := sha256.Sum256([]byte(vertex.Digest))
		state, exists := p.vertices[id]
		isExport := vertex.Name == "exporting to image"
		if exists && state.export != isExport {
			p.refuse("malformed_stream")
			return
		}
		if !exists && len(p.vertices) == buildobservation.MaxVertices {
			p.refuse("limit_exceeded")
			return
		}
		state.export = isExport
		state.started = state.started || vertex.Started != nil
		p.vertices[id] = state
		if !p.addInterval(progressIntervalKey{vertex: id}, vertex.Started, vertex.Completed, vertex.Cached) {
			return
		}
	}
	for _, status := range event.Statuses {
		if status == nil || !validProgressIdentity(status.Vertex) || status.ID == "" || len(status.ID) > 8192 {
			p.refuse("malformed_stream")
			return
		}
		// progress.OneOff uses the ID (not Action/Name) for these operations.
		push := status.ID == "pushing layers" || (strings.HasPrefix(status.ID, "pushing manifest for ") && len(status.ID) > len("pushing manifest for "))
		if !push {
			continue
		}
		key := progressIntervalKey{vertex: sha256.Sum256([]byte(status.Vertex)), status: sha256.Sum256([]byte(status.ID)), isStatus: true}
		if !p.addInterval(key, status.Started, status.Completed, false) {
			return
		}
	}
}

func validProgressIdentity(value string) bool { return value != "" && len(value) <= 256 }

func (p *progressProjection) addInterval(key progressIntervalKey, start, end *time.Time, cached bool) bool {
	if start == nil {
		if end != nil {
			p.refuse("malformed_stream")
			return false
		}
		return true
	}
	if start.IsZero() || start.Year() < 2000 || start.Year() > 2200 || (end != nil && (end.Before(*start) || end.Sub(*start).Seconds() > buildobservation.MaxSeconds)) {
		p.refuse("malformed_stream")
		return false
	}
	key.start = start.UTC()
	old, exists := p.intervals[key]
	if !exists && len(p.intervals) == buildobservation.MaxIntervals {
		p.refuse("limit_exceeded")
		return false
	}
	if exists && old.complete && (end == nil || !old.end.Equal(*end) || old.cached != cached) {
		p.refuse("malformed_stream")
		return false
	}
	interval := progressInterval{start: key.start, cached: cached}
	if end != nil {
		interval.end = end.UTC()
		interval.complete = true
	}
	p.intervals[key] = interval
	return true
}

func (p *progressProjection) finish(commandErr error, cancelled bool) *buildobservation.Observation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.line) > 0 || p.discardLine {
		p.refuse("partial_stream")
		if len(p.line) > 0 {
			p.consumeLine(p.line)
		}
	}
	if p.reason != "" {
		return buildobservation.Unavailable(p.reason)
	}
	if len(p.vertices) == 0 || len(p.intervals) == 0 {
		return buildobservation.Unavailable("missing_stream")
	}
	o := buildobservation.Unavailable("partial_stream")
	o.Availability = "observed"
	o.Reason = ""
	o.VertexCount = int32(len(p.vertices))
	var vertexIntervals, exportIntervals, pushIntervals []timeInterval
	partialVertex, partialExport, partialPush := false, false, false
	for _, state := range p.vertices {
		if !state.started {
			partialVertex = true
		}
	}
	for key, interval := range p.intervals {
		parent, known := p.vertices[key.vertex]
		if key.isStatus {
			if !known || !parent.export {
				partialPush = true
				continue
			}
			if !interval.complete {
				partialPush = true
				continue
			}
			pushIntervals = append(pushIntervals, timeInterval{interval.start, interval.end})
			continue
		}
		o.VertexIntervalCount++
		if !interval.complete {
			partialVertex = true
			if parent.export {
				partialExport = true
			}
			continue
		}
		if interval.cached {
			o.CachedVertexIntervalCount++
		}
		vertexIntervals = append(vertexIntervals, timeInterval{interval.start, interval.end})
		if parent.export {
			exportIntervals = append(exportIntervals, timeInterval{interval.start, interval.end})
		}
	}
	if partialVertex || partialExport || partialPush {
		o.Availability = "incomplete"
		o.Reason = "partial_stream"
	}
	if commandErr != nil {
		o.Availability = "incomplete"
		o.Reason = "command_failed"
	}
	if cancelled {
		o.Availability = "incomplete"
		o.Reason = "cancelled"
	}
	vertices := unionIntervals(vertexIntervals)
	exports := unionIntervals(exportIntervals)
	pushes := unionIntervals(pushIntervals)
	if !partialVertex && len(vertices) > 0 {
		o.VertexUnionNanoseconds = nanosecondsPointer(unionNanoseconds(vertices))
	}
	if !partialExport && len(exports) > 0 {
		o.ExportAvailability = "observed"
		o.ExportUnionNanoseconds = nanosecondsPointer(unionNanoseconds(exports))
	}
	if !partialPush && len(pushes) > 0 {
		o.PushAvailability = "observed"
		o.PushUnionNanoseconds = nanosecondsPointer(unionNanoseconds(pushes))
	}
	if o.ExportUnionNanoseconds != nil && o.PushUnionNanoseconds != nil {
		o.ExportPushOverlapNanoseconds = nanosecondsPointer(overlapNanoseconds(exports, pushes))
	}
	return buildobservation.Normalize(o)
}

func nanosecondsPointer(value int64) *int64 { return &value }

func unionIntervals(intervals []timeInterval) []timeInterval {
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
	var merged []timeInterval
	for _, interval := range intervals {
		if len(merged) == 0 || interval.start.After(merged[len(merged)-1].end) {
			merged = append(merged, interval)
			continue
		}
		if interval.end.After(merged[len(merged)-1].end) {
			merged[len(merged)-1].end = interval.end
		}
	}
	return merged
}

func unionNanoseconds(intervals []timeInterval) int64 {
	var duration int64
	for _, interval := range intervals {
		duration += int64(interval.end.Sub(interval.start))
	}
	return duration
}

func overlapNanoseconds(a, b []timeInterval) int64 {
	var overlap int64
	for i, j := 0, 0; i < len(a) && j < len(b); {
		start, end := a[i].start, a[i].end
		if b[j].start.After(start) {
			start = b[j].start
		}
		if b[j].end.Before(end) {
			end = b[j].end
		}
		if end.After(start) {
			overlap += int64(end.Sub(start))
		}
		if a[i].end.Before(b[j].end) {
			i++
		} else {
			j++
		}
	}
	return overlap
}
