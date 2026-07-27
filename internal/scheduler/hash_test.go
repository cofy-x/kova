package scheduler

import "testing"

func TestConsistentHashSlotOrderCoversEachNodeOnce(t *testing.T) {
	hash := newConsistentHash(4, 25)
	order := hash.getSlotOrder("localhost:5001/kova-examples/simple:dev")

	if len(order) != 4 {
		t.Fatalf("len(order) = %d, want 4", len(order))
	}

	seen := make(map[int]bool, len(order))
	for _, idx := range order {
		if idx < 0 || idx >= 4 {
			t.Fatalf("slot index %d outside node range", idx)
		}
		if seen[idx] {
			t.Fatalf("slot index %d appeared more than once in %v", idx, order)
		}
		seen[idx] = true
	}
}

func TestConsistentHashSlotOrderIsDeterministic(t *testing.T) {
	hash := newConsistentHash(3, 10)
	left := hash.getSlotOrder("target-a")
	right := hash.getSlotOrder("target-a")

	if len(left) != len(right) {
		t.Fatalf("slot order lengths differ: %v vs %v", left, right)
	}
	for i := range left {
		if left[i] != right[i] {
			t.Fatalf("slot order is not deterministic: %v vs %v", left, right)
		}
	}
}

func TestConsistentHashEmptyRingReturnsNilOrder(t *testing.T) {
	if order := (&consistentHash{}).getSlotOrder("target-a"); order != nil {
		t.Fatalf("order = %v, want nil", order)
	}
}
