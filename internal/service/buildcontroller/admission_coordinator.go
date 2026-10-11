package buildcontroller

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// AdmissionCoordinator bounds this process's fresh Genesis grant contention.
// It is only a scheduling hint: grants, fairness, and uncertain effects remain
// governed by the original durable ledgers, CAS, and one-shot receipt protocol.
// Existing grants and every Pod, runner, cancellation, and cleanup path bypass it.
type AdmissionCoordinator struct {
	mu         sync.Mutex
	namespace  string
	holder     *freshGrantPermit
	pending    bool
	generation uint64
	release    chan event.GenericEvent
}

type freshGrantPermit struct{ owner *AdmissionCoordinator }

func NewAdmissionCoordinator(namespace string) *AdmissionCoordinator {
	return &AdmissionCoordinator{namespace: namespace, release: make(chan event.GenericEvent, 1)}
}

func (g *AdmissionCoordinator) validate(namespace string) error {
	if g == nil || namespace == "" || g.namespace != namespace || g.release == nil {
		return fmt.Errorf("local admission coordinator differs from configured Namespace")
	}
	return nil
}

// tryBegin never waits for another grant. A busy caller leaves only one
// coalesced demand bit, not a per-build waiter, timer, or permission to retry.
func (g *AdmissionCoordinator) tryBegin(ctx context.Context, namespace string) (*freshGrantPermit, error) {
	if err := g.validate(namespace); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.holder != nil {
		g.pending = true
		return nil, nil
	}
	permit := &freshGrantPermit{owner: g}
	g.holder = permit
	return permit, nil
}

// deferIfBusy makes Pump's busy observation and demand registration atomic
// with release. A release cannot fall between them and lose the only wakeup.
func (g *AdmissionCoordinator) deferIfBusy(namespace string) (bool, uint64, error) {
	if err := g.validate(namespace); err != nil {
		return false, 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.holder != nil {
		g.pending = true
		return true, g.generation, nil
	}
	return false, g.generation, nil
}

// release is idempotent and tied to the original holder pointer. A duplicate
// release cannot open a successor's interval. Neither cancellation nor an
// unknown API outcome releases a durable charge: only this local hint ends.
func (p *freshGrantPermit) release() {
	if p == nil || p.owner == nil {
		return
	}
	g := p.owner
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.holder != p {
		return
	}
	g.holder = nil
	g.generation++
	if !g.pending {
		return
	}
	g.pending = false
	e := event.GenericEvent{Object: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: g.namespace, Name: admissionReleaseRequestName,
	}}}
	// A pending release is sufficient. Pump reads the latest generation;
	// its unchanged 30s audit also covers a dropped event or process loss.
	select {
	case g.release <- e:
	default:
	}
}

func (g *AdmissionCoordinator) ReleaseEvents() <-chan event.GenericEvent { return g.release }
