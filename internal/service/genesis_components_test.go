package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cofy-x/kova/internal/service/admissiongenesis"
)

type admissionCheckFunc func(context.Context) error

func (f admissionCheckFunc) Check(ctx context.Context) error { return f(ctx) }

var _ admissiongenesis.Checker = admissionCheckFunc(nil)

func TestGenesisBarrierPreventsManagerAndListenerStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var managerCalls, httpCalls atomic.Int32
	veto := errors.New("committed ledger is missing")
	err := startServiceComponents(ctx, cancel, admissionCheckFunc(func(context.Context) error { return veto }),
		func(context.Context) error { managerCalls.Add(1); return nil },
		func(context.Context) error { httpCalls.Add(1); return nil })
	if !errors.Is(err, veto) || managerCalls.Load() != 0 || httpCalls.Load() != 0 {
		t.Fatalf("failed Genesis check started a component: err=%v manager=%d http=%d", err, managerCalls.Load(), httpCalls.Load())
	}
}

func TestGenesisBarrierStartsBothAfterQualification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpStarted := make(chan struct{})
	checks := 0
	err := startServiceComponents(ctx, cancel, admissionCheckFunc(func(context.Context) error {
		checks++
		return nil
	}), func(context.Context) error {
		<-httpStarted
		return nil
	}, func(context.Context) error {
		close(httpStarted)
		return nil
	})
	if err != nil || checks != 1 {
		t.Fatalf("qualified Genesis startup did not start components: err=%v checks=%d", err, checks)
	}
}
