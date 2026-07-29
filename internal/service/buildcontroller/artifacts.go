package buildcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
)

func (r *KovaBuildReconciler) persistLogs(ctx context.Context, build *kovav1.KovaBuild) {
	if r.Store == nil || r.Kube == nil || build.Status.RunnerPodName == "" || build.Status.LogArtifactURI != "" {
		return
	}
	logs := newTailBuffer(r.Cfg.MaxLogBytes)
	if err := r.Kube.WritePodLogsTail(ctx, build.Namespace, build.Status.RunnerPodName, -1, logs); err != nil || logs.Len() == 0 {
		return
	}
	raw := logs.Bytes()
	uri, err := r.Store.Put(ctx, "builds/"+build.Name+"/runner.log", bytes.NewReader(raw), int64(len(raw)), "text/plain; charset=utf-8")
	if err != nil {
		return
	}
	digest := sha256.Sum256(raw)
	build.Status.LogArtifactURI = uri
	build.Status.LogArtifactDigest = fmt.Sprintf("sha256:%x", digest)
}

type tailBuffer struct {
	limit int
	data  []byte
}

func newTailBuffer(limit int64) *tailBuffer {
	if limit <= 0 {
		limit = 16 << 20
	}
	return &tailBuffer{limit: int(limit)}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return written, nil
	}
	overflow := len(b.data) + len(p) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return written, nil
}

func (b *tailBuffer) Len() int { return len(b.data) }

func (b *tailBuffer) Bytes() []byte { return append([]byte(nil), b.data...) }

func (r *KovaBuildReconciler) persistResults(ctx context.Context, build *kovav1.KovaBuild) error {
	build.Status.ResultSummary = summarize(build.Status.Results)
	if r.Store == nil || len(build.Status.Results) == 0 {
		return nil
	}
	raw, err := json.Marshal(build.Status.Results)
	if err != nil {
		return fmt.Errorf("encode build results: %w", err)
	}
	uri, err := r.Store.Put(ctx, "builds/"+build.Name+"/results.json", bytes.NewReader(raw), int64(len(raw)), "application/json")
	if err != nil {
		return fmt.Errorf("persist build results: %w", err)
	}
	build.Status.ResultArtifactURI = uri
	digest := sha256.Sum256(raw)
	build.Status.ResultArtifactDigest = fmt.Sprintf("sha256:%x", digest)
	if len(build.Status.Results) > 100 {
		build.Status.Results = build.Status.Results[:100]
	}
	return nil
}
