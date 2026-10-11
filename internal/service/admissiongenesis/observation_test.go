package admissiongenesis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"k8s.io/apimachinery/pkg/types"
)

func TestObservationTokenIsOriginalScopedAndClosesCopies(t *testing.T) {
	_, _, guard, _ := committedGuardFixture(t)
	var zero Observation
	if err := zero.Assert(guard); err == nil {
		t.Fatal("zero-value observation was accepted")
	}
	var retained Observation
	if err := guard.Observe(t.Context(), func(o *Observation) error {
		if err := o.Assert(guard); err != nil {
			return err
		}
		derived, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := o.AssertNoOrdinaryContext(guard, derived); err == nil {
			t.Fatal("observed helper silently discarded a derived cancellation context")
		}
		if err := o.Assert(&Guard{}); err == nil {
			t.Fatal("foreign Guard was accepted")
		}
		retained = *o
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := retained.Assert(guard); err == nil {
		t.Fatal("copied token remained live after scope")
	}
	if err := retained.Read(guard, func(context.Context) error {
		t.Fatal("closed token issued an API read")
		return nil
	}); err == nil {
		t.Fatal("closed token accepted a read")
	}
}

func TestObservationEntryAndExitRefuseOriginalDrift(t *testing.T) {
	for _, mode := range []string{"receipt", "runner-namespace", "receipt-namespace", "genesis", "active", "queue", "invalid-active-data"} {
		t.Run(mode, func(t *testing.T) {
			for _, at := range []string{"entry", "exit"} {
				t.Run(at, func(t *testing.T) {
					fake, bootstrap, guard, _ := committedGuardFixture(t)
					mutate := func() {
						switch mode {
						case "receipt":
							guard.ReceiptCheck = func(context.Context) error { return ErrChanged }
						case "runner-namespace":
							fake.namespace.UID = types.UID("replacement")
						case "receipt-namespace":
							fake.receiptNamespace.UID = types.UID("replacement")
						case "genesis":
							fake.objects[bootstrap.Receipt.GenesisName].UID = types.UID("replacement")
						case "active":
							fake.objects[admissioncontract.ActiveLedgerName].UID = types.UID("replacement")
						case "queue":
							fake.objects[admissioncontract.QueueLedgerName].UID = types.UID("replacement")
						case "invalid-active-data":
							fake.objects[admissioncontract.ActiveLedgerName].Data[admissioncontract.ActiveLedgerDataKey] = "invalid"
						}
					}
					if at == "entry" {
						mutate()
					}
					entered := false
					err := guard.Observe(t.Context(), func(*Observation) error {
						entered = true
						if at == "exit" {
							mutate()
						}
						return nil
					})
					if err == nil || entered != (at == "exit") {
						t.Fatalf("drift was accepted or callback entered at wrong edge: entered=%t err=%v", entered, err)
					}
					if fake.patchCount[admissioncontract.ActiveLedgerName] != 0 || fake.patchCount[admissioncontract.QueueLedgerName] != 0 {
						t.Fatal("read-only scope caused a ledger Patch")
					}
				})
			}
		})
	}
}

func TestObservationExitPreservesCallbackAndCancellationErrors(t *testing.T) {
	_, _, guard, _ := committedGuardFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	callbackErr := errors.New("read refused")
	err := guard.Observe(ctx, func(*Observation) error {
		cancel()
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("callback or canceled exit was lost: %v", err)
	}
}

func TestObservationCloseWaitsForInFlightReadAndRejectsLeakedRead(t *testing.T) {
	_, _, guard, _ := committedGuardFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan error, 1)
	scopeDone := make(chan error, 1)
	var leaked Observation
	var once sync.Once
	go func() {
		scopeDone <- guard.Observe(t.Context(), func(o *Observation) error {
			leaked = *o
			go func() {
				readDone <- o.Read(guard, func(context.Context) error {
					once.Do(func() { close(started) })
					<-release
					return nil
				})
			}()
			<-started
			return nil
		})
	}()
	select {
	case <-scopeDone:
		t.Fatal("scope closed before in-flight read completed")
	case <-time.After(5 * time.Millisecond):
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-scopeDone; err != nil {
		t.Fatal(err)
	}
	if err := leaked.Read(guard, func(context.Context) error { t.Fatal("leaked read ran"); return nil }); err == nil {
		t.Fatal("leaked token remained usable")
	}
}

func TestObservationPanicStillClosesCopiedToken(t *testing.T) {
	_, _, guard, _ := committedGuardFixture(t)
	var leaked Observation
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("synthetic callback panic was swallowed")
			}
		}()
		_ = guard.Observe(t.Context(), func(o *Observation) error {
			leaked = *o
			panic("synthetic read panic")
		})
	}()
	if err := leaked.Assert(guard); err == nil {
		t.Fatal("panic leaked a live read-only token")
	}
}
