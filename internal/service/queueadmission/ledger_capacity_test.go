package queueadmission

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestFullQueueTableHasEncodedHeadroomForRelease(t *testing.T) {
	current := state{Version: 1, GlobalLimit: 1000, RequesterLimit: 1000, Intents: make(map[string]Intent, 1000)}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%04d", i) + strings.Repeat("a", 249)
		current.Intents[id] = Intent{RequesterHash: strings.Repeat("a", 64), RequestDigest: strings.Repeat("b", 64),
			Nonce: strings.Repeat("c", 32), CreatedAtUnix: math.MaxInt64}
	}
	encoded, err := encodeState(current)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxHeaderBytes+1000*maxIntentBytes {
		t.Fatalf("full queue encoded to %d bytes above proven bound", len(encoded))
	}
	t.Logf("full queue JSON = %d bytes; 768 KiB headroom = %d bytes", len(encoded), maxLedgerBytes-len(encoded))
	delete(current.Intents, strings.Repeat("0", 4)+strings.Repeat("a", 249))
	if _, err := encodeState(current); err != nil {
		t.Fatalf("full queue could not release one intent: %v", err)
	}
}

func TestQueueStateRejectsInvalidIdentityAndLossyJSON(t *testing.T) {
	valid := state{Version: 1, GlobalLimit: 1, RequesterLimit: 1, Intents: map[string]Intent{
		"build": {RequesterHash: strings.Repeat("a", 64), RequestDigest: strings.Repeat("b", 64), Nonce: strings.Repeat("c", 32), CreatedAtUnix: 1},
	}}
	for name, change := range map[string]func(*state){
		"long build id":    func(s *state) { s.Intents[strings.Repeat("a", 254)] = s.Intents["build"]; delete(s.Intents, "build") },
		"invalid build id": func(s *state) { s.Intents["not valid"] = s.Intents["build"]; delete(s.Intents, "build") },
		"hash escaping": func(s *state) {
			entry := s.Intents["build"]
			entry.RequesterHash = strings.Repeat("<", 64)
			s.Intents["build"] = entry
		},
		"digest uppercase": func(s *state) {
			entry := s.Intents["build"]
			entry.RequestDigest = strings.Repeat("B", 64)
			s.Intents["build"] = entry
		},
		"nonce uppercase": func(s *state) {
			entry := s.Intents["build"]
			entry.Nonce = strings.Repeat("C", 32)
			s.Intents["build"] = entry
		},
		"too many intents": func(s *state) { s.Intents["another"] = s.Intents["build"] },
	} {
		t.Run(name, func(t *testing.T) {
			changed := valid
			changed.Intents = map[string]Intent{"build": valid.Intents["build"]}
			change(&changed)
			if err := validateState(changed); err == nil {
				t.Fatal("invalid queue state accepted")
			}
		})
	}
	encoded, err := encodeState(valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		strings.Replace(string(encoded), `"intents":`, `"unknown":0,"intents":`, 1),
		strings.Replace(string(encoded), `"intents":`, `"intents":{},"intents":`, 1),
		strings.Replace(string(encoded), `"intents":`, `"\u0069ntents":{},"intents":`, 1),
		strings.Replace(string(encoded), `"createdAtUnix":1`, `"createdAtUnix":null`, 1),
	} {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: ConfigMapName}, Data: map[string]string{dataKey: raw}}
		if _, _, err := (Store{Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1, Reader: staticReader{cm}}).read(t.Context()); err == nil {
			t.Fatalf("lossy queue JSON accepted: %s", raw)
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: ConfigMapName}, Data: map[string]string{dataKey: string(encoded), "other": "evidence"}}
	if _, _, err := (Store{Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1, Reader: staticReader{cm}}).read(t.Context()); err == nil {
		t.Fatal("extra ConfigMap data would be silently removed")
	}
}

type staticReader struct{ cm *corev1.ConfigMap }

func (r staticReader) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	r.cm.DeepCopyInto(obj.(*corev1.ConfigMap))
	return nil
}

func (r staticReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return fmt.Errorf("not used")
}
