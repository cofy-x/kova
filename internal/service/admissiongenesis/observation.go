package admissiongenesis

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Observation is a short, synchronous read-only interval belonging to exactly
// one original Guard. It is not an authority lease: fresh API reads and their
// UID/schema checks still happen on every call, and a writer must use the
// ordinary full Guard.Check and CAS after this interval ends.
//
// The type exposes no mutation method and must not be retained by a Store or
// Reconciler. Its only purpose is to avoid re-observing the entire original
// Genesis pair between adjacent, non-effecting reads.
type Observation struct {
	state *observationState
}

type observationState struct {
	mu     sync.RWMutex
	guard  *Guard
	ctx    context.Context
	closed bool
}

// Observe brackets one synchronous read-only chain with full original-pair
// checks. The closing check runs even when the callback reports an error.
// Callers must place every Create, Patch, Update, Delete, Pod operation, and
// runner operation outside the callback.
func (g *Guard) Observe(ctx context.Context, read func(*Observation) error) error {
	if g == nil || read == nil {
		return fmt.Errorf("read-only Genesis observation has no guard or callback")
	}
	if err := g.Check(ctx); err != nil {
		return err
	}
	observation := &Observation{state: &observationState{guard: g, ctx: ctx}}
	defer func() {
		observation.state.mu.Lock()
		observation.state.closed = true
		observation.state.mu.Unlock()
	}()
	readErr := read(observation)
	observation.state.mu.Lock()
	observation.state.closed = true
	observation.state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		readErr = errors.Join(readErr, err)
	}
	return errors.Join(readErr, g.Check(ctx))
}

// Assert only identifies the live observation and its original Guard. It may
// guard local computations; direct API reads must use Read so Close waits for
// the entire in-flight read. It is never an effect-edge authority check.
func (o *Observation) Assert(g *Guard) error {
	if o == nil || o.state == nil || g == nil {
		return fmt.Errorf("read-only Genesis observation is closed or belongs to another original pair")
	}
	o.state.mu.RLock()
	defer o.state.mu.RUnlock()
	if o.state.closed || o.state.guard != g {
		return fmt.Errorf("read-only Genesis observation is closed or belongs to another original pair")
	}
	return nil
}

// AssertNoOrdinaryContext prevents observed helper implementations from
// silently dropping a caller's separately derived deadline. Their public
// Observed entrypoints take only this token; ordinary calls keep their own
// context and full Guard checks.
func (o *Observation) AssertNoOrdinaryContext(g *Guard, ordinaryCtx context.Context) error {
	if ordinaryCtx != nil {
		return fmt.Errorf("read-only Genesis observation cannot ignore a separate context")
	}
	return o.Assert(g)
}

// Read runs exactly one read-only operation using the original context. The
// shared token lock prevents a callback's leaked asynchronous read from
// continuing past scope closure; copied tokens share the same state.
func (o *Observation) Read(g *Guard, read func(context.Context) error) error {
	if o == nil || o.state == nil || g == nil || read == nil {
		return fmt.Errorf("read-only Genesis observation is closed or belongs to another original pair")
	}
	o.state.mu.RLock()
	defer o.state.mu.RUnlock()
	if o.state.closed || o.state.guard != g {
		return fmt.Errorf("read-only Genesis observation is closed or belongs to another original pair")
	}
	if err := o.state.ctx.Err(); err != nil {
		return err
	}
	return read(o.state.ctx)
}
