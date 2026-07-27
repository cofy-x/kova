package scheduler

import "testing"

func TestPickAvailableSlotFallsBackWhenPrimaryBusy(t *testing.T) {
	pool := NewPool([]*Addr{
		{Addr: "tcp://10.0.0.1:9094"},
		{Addr: "tcp://10.0.0.2:9094"},
		{Addr: "tcp://10.0.0.3:9094"},
	}, 1)
	snapshot := pool.Snapshot()
	key := "target-a"

	primary := PickSlot(snapshot, key)
	if primary == nil {
		t.Fatal("expected a primary slot")
	}

	if !TryAcquireSlot(primary.Sem) {
		t.Fatal("expected to occupy the primary slot")
	}
	defer func() { <-primary.Sem }()

	fallback := PickAvailableSlot(snapshot, key)
	if fallback == nil {
		t.Fatal("expected a fallback slot")
	}
	defer func() { <-fallback.Sem }()

	if fallback == primary {
		t.Fatal("expected scheduler to skip the busy primary slot")
	}
}

func TestPickAvailableSlotReturnsNilWhenAllSlotsBusy(t *testing.T) {
	pool := NewPool([]*Addr{
		{Addr: "tcp://10.0.0.1:9094"},
		{Addr: "tcp://10.0.0.2:9094"},
	}, 1)
	snapshot := pool.Snapshot()

	for _, slot := range snapshot.Slots {
		if !TryAcquireSlot(slot.Sem) {
			t.Fatal("expected to occupy slot")
		}
		defer func(slot *Slot) { <-slot.Sem }(slot)
	}

	if slot := PickAvailableSlot(snapshot, "target-b"); slot != nil {
		t.Fatal("expected no available slot when every endpoint is busy")
	}
}
