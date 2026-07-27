package scheduler

import (
	"fmt"
	"slices"
	"sort"
)

// consistentHash maps keys to node indices using a hash ring with virtual nodes,
// ensuring the same target is consistently scheduled to the same buildkitd address.
type consistentHash struct {
	ring     []uint32
	nodeMap  map[uint32]int
	numNodes int
}

func newConsistentHash(numNodes, replicas int) *consistentHash {
	ch := &consistentHash{
		nodeMap:  make(map[uint32]int),
		numNodes: numNodes,
	}
	for i := range numNodes {
		for r := range replicas {
			h := fnv1a(fmt.Sprintf("node-%d-replica-%d", i, r))
			ch.ring = append(ch.ring, h)
			ch.nodeMap[h] = i
		}
	}
	slices.Sort(ch.ring)
	return ch
}

// getSlotOrder returns node indices in preference order for the given key.
// The first element is the primary; subsequent elements are fallbacks.
func (ch *consistentHash) getSlotOrder(key string) []int {
	if len(ch.ring) == 0 {
		return nil
	}
	h := fnv1a(key)
	idx := sort.Search(len(ch.ring), func(i int) bool { return ch.ring[i] >= h })
	if idx >= len(ch.ring) {
		idx = 0
	}

	seen := make(map[int]bool)
	order := make([]int, 0, ch.numNodes)
	for len(order) < ch.numNodes {
		nodeIdx := ch.nodeMap[ch.ring[idx%len(ch.ring)]]
		if !seen[nodeIdx] {
			seen[nodeIdx] = true
			order = append(order, nodeIdx)
		}
		idx++
	}
	return order
}

func fnv1a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
