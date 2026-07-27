package scheduler

import (
	"sort"
	"sync"
)

type Slot struct {
	Addr *Addr
	Sem  chan struct{}
}

type PoolSnapshot struct {
	Slots []*Slot
	hash  *consistentHash
}

type Pool struct {
	mu          sync.RWMutex
	concurrency int
	slots       []*Slot
	hash        *consistentHash
}

func NewPool(addrs []*Addr, concurrency int) *Pool {
	p := &Pool{concurrency: concurrency}
	p.Replace(addrs)
	return p
}

func (p *Pool) Snapshot() PoolSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	slots := append([]*Slot(nil), p.slots...)
	return PoolSnapshot{Slots: slots, hash: p.hash}
}

func (p *Pool) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slotAddressKeys(p.slots)
}

func (p *Pool) Replace(addrs []*Addr) {
	p.mu.Lock()
	defer p.mu.Unlock()

	existing := make(map[string]*Slot, len(p.slots))
	for _, slot := range p.slots {
		if slot != nil && slot.Addr != nil {
			existing[slot.Addr.Addr] = slot
		}
	}

	newSlots := make([]*Slot, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		if slot, ok := existing[addr.Addr]; ok {
			newSlots = append(newSlots, slot)
			continue
		}
		newSlots = append(newSlots, &Slot{
			Addr: addr,
			Sem:  make(chan struct{}, p.concurrency),
		})
	}

	sort.Slice(newSlots, func(i, j int) bool {
		return newSlots[i].Addr.Addr < newSlots[j].Addr.Addr
	})

	p.slots = newSlots
	if len(newSlots) == 0 {
		p.hash = nil
		return
	}
	p.hash = newConsistentHash(len(newSlots), 150)
}

func PickSlot(snapshot PoolSnapshot, key string) *Slot {
	if len(snapshot.Slots) == 0 || snapshot.hash == nil {
		return nil
	}

	order := snapshot.hash.getSlotOrder(key)
	if len(order) == 0 {
		return nil
	}

	var fallback *Slot
	for _, idx := range order {
		if idx < 0 || idx >= len(snapshot.Slots) {
			continue
		}
		slot := snapshot.Slots[idx]
		if slot == nil || slot.Addr == nil {
			continue
		}
		if fallback == nil {
			fallback = slot
		}
		if !slot.Addr.IsInCooldown() {
			return slot
		}
	}

	return fallback
}

func PickAvailableSlot(snapshot PoolSnapshot, key string) *Slot {
	if len(snapshot.Slots) == 0 || snapshot.hash == nil {
		return nil
	}

	order := snapshot.hash.getSlotOrder(key)
	if len(order) == 0 {
		return nil
	}

	for _, requireReady := range []bool{true, false} {
		for _, idx := range order {
			if idx < 0 || idx >= len(snapshot.Slots) {
				continue
			}
			slot := snapshot.Slots[idx]
			if slot == nil || slot.Addr == nil {
				continue
			}
			if requireReady && slot.Addr.IsInCooldown() {
				continue
			}
			if !TryAcquireSlot(slot.Sem) {
				continue
			}
			return slot
		}
	}

	return nil
}

func TryAcquireSlot(sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func slotAddressKeys(slots []*Slot) []string {
	keys := make([]string, 0, len(slots))
	for _, slot := range slots {
		if slot == nil || slot.Addr == nil {
			continue
		}
		keys = append(keys, slot.Addr.Addr)
	}
	return keys
}
