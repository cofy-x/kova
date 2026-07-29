package artifactgc

import (
	"bytes"
	"context"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCollectorDeletesOnlyUnreferencedBuildDirectories(t *testing.T) {
	ctx := context.Background()
	store, err := artifactstore.NewFilesystem(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retainedURI, err := store.Put(ctx, "builds/retained/source.zip", bytes.NewReader([]byte("keep")), 4, "application/zip")
	if err != nil {
		t.Fatal(err)
	}
	orphanURI, err := store.Put(ctx, "builds/orphan/source.zip", bytes.NewReader([]byte("delete")), 6, "application/zip")
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(&kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "jobs"}}).Build()
	collector := Collector{Reader: reader, Store: store, Namespace: "jobs", OrphanTTL: time.Hour, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	if err := collector.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if reader, err := store.Open(ctx, retainedURI); err != nil {
		t.Fatalf("retained artifact was deleted: %v", err)
	} else {
		_ = reader.Close()
	}
	if reader, err := store.Open(ctx, orphanURI); err == nil {
		_ = reader.Close()
		t.Fatal("orphan artifact was not deleted")
	}
}
