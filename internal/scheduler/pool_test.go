package scheduler

import "testing"

func TestPoolReplaceSortsAddressesAndReusesExistingSlots(t *testing.T) {
	pool := NewPool([]*Addr{
		{Addr: "tcp://10.0.0.2:9094"},
		{Addr: "tcp://10.0.0.1:9094"},
	}, 1)

	initial := pool.Snapshot()
	var reused *Slot
	for _, slot := range initial.Slots {
		if slot.Addr.Addr == "tcp://10.0.0.2:9094" {
			reused = slot
			break
		}
	}
	if reused == nil {
		t.Fatal("expected to find reusable slot")
	}
	if !TryAcquireSlot(reused.Sem) {
		t.Fatal("expected to occupy reusable slot")
	}
	defer func() { <-reused.Sem }()

	pool.Replace([]*Addr{
		{Addr: "tcp://10.0.0.3:9094"},
		{Addr: "tcp://10.0.0.2:9094"},
	})

	addresses := pool.Addresses()
	want := []string{"tcp://10.0.0.2:9094", "tcp://10.0.0.3:9094"}
	if len(addresses) != len(want) {
		t.Fatalf("addresses = %v, want %v", addresses, want)
	}
	for i := range want {
		if addresses[i] != want[i] {
			t.Fatalf("addresses = %v, want %v", addresses, want)
		}
	}

	var after *Slot
	for _, slot := range pool.Snapshot().Slots {
		if slot.Addr.Addr == "tcp://10.0.0.2:9094" {
			after = slot
			break
		}
	}
	if after != reused {
		t.Fatal("expected Replace to reuse the existing slot for unchanged address")
	}
	if TryAcquireSlot(after.Sem) {
		defer func() { <-after.Sem }()
		t.Fatal("expected reused slot semaphore to preserve occupancy")
	}
}

func TestPoolReplaceWithEmptyAddrsClearsHash(t *testing.T) {
	pool := NewPool([]*Addr{{Addr: "tcp://10.0.0.1:9094"}}, 1)
	pool.Replace(nil)

	snapshot := pool.Snapshot()
	if len(snapshot.Slots) != 0 {
		t.Fatalf("len(snapshot.Slots) = %d, want 0", len(snapshot.Slots))
	}
	if snapshot.hash != nil {
		t.Fatal("expected hash to be cleared")
	}
	if slot := PickSlot(snapshot, "target-a"); slot != nil {
		t.Fatal("expected no slot from empty pool")
	}
}
