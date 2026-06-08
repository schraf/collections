package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ╭──────────────────────────╮
// │ [ 0 ]--->[ 1 ]<-->[ 2 ]  │
// │    \        ^            │
// │     \        \-------    │
// │      \               \   │
// │       --->[ 3 ]--->[ 4 ] │
// ╰──────────────────────────╯

func TestGraph(t *testing.T) {
	const t0 uint8 = 0
	const t1 uint8 = 1
	const t2 uint8 = 2

	b := NewGraphBuilder[uint64, uint8](5, true)

	b.AddEdgeWithLabel(0, 1, t0)
	b.AddEdgeWithLabel(0, 3, t1)
	b.AddEdgeWithLabel(1, 2, t2)
	b.AddEdgeWithLabel(2, 1, t0)
	b.AddEdgeWithLabel(3, 4, t1)
	b.AddEdgeWithLabel(4, 1, t2)

	graph := MutableBidirectedGraph[uint64, uint8]{
		Inbound:  NewMutableDirectedGraph(*b.BuildInboundGraph()),
		Outbound: NewMutableDirectedGraph(*b.BuildOutboundGraph()),
	}

	// ╭────────────────────────────────────────────────────────────────────╮
	// │ Helpers                                                            │
	// ╰────────────────────────────────────────────────────────────────────╯

	assertNeighbors := func(foundNeighbors []uint32, expectedNeighbors []uint32) {
		t.Helper()
		if expectedNeighbors == nil {
			assert.Empty(t, foundNeighbors)
		} else {
			assert.ElementsMatch(t, expectedNeighbors, foundNeighbors)
		}
	}

	assertLabels := func(foundLabels []uint8, expectedLabels []uint8) {
		t.Helper()
		if expectedLabels == nil {
			assert.Empty(t, foundLabels)
		} else {
			assert.ElementsMatch(t, expectedLabels, foundLabels)
		}
	}

	assertOutboundNeighbors := func(node uint32, expectedNeighbors []uint32, expectedLabels []uint8) {
		t.Helper()
		foundNeighbors, foundLabels := graph.Outbound.Neighbors(node)
		assertNeighbors(foundNeighbors, expectedNeighbors)
		assertLabels(foundLabels, expectedLabels)
	}

	assertInboundNeighbors := func(node uint32, expectedNeighbors []uint32, expectedLabels []uint8) {
		t.Helper()
		foundNeighbors, foundLabels := graph.Inbound.Neighbors(node)
		assertNeighbors(foundNeighbors, expectedNeighbors)
		assertLabels(foundLabels, expectedLabels)
	}

	// ╭────────────────────────────────────────────────────────────────────╮
	// │ Check Edge Connections                                             │
	// ╰────────────────────────────────────────────────────────────────────╯

	// node 0
	assertOutboundNeighbors(0, []uint32{1, 3}, []uint8{t0, t1})
	assertInboundNeighbors(0, nil, nil)

	// node 1
	assertOutboundNeighbors(1, []uint32{2}, []uint8{t2})
	assertInboundNeighbors(1, []uint32{0, 2, 4}, []uint8{t0, t0, t2})

	// node 2
	assertOutboundNeighbors(2, []uint32{1}, []uint8{t0})
	assertInboundNeighbors(2, []uint32{1}, []uint8{t2})

	// node 3
	assertOutboundNeighbors(3, []uint32{4}, []uint8{t1})
	assertInboundNeighbors(3, []uint32{0}, []uint8{t1})

	// node 4
	assertOutboundNeighbors(4, []uint32{1}, []uint8{t2})
	assertInboundNeighbors(4, []uint32{3}, []uint8{t1})

	// ╭────────────────────────────────────────────────────────────────────╮
	// │ MutateGraph                                                        │
	// ╰────────────────────────────────────────────────────────────────────╯

	graph.AddEdgeWithLabel(1, 3, t1)
	graph.RemoveEdge(0, 3)

	// node 0
	assertOutboundNeighbors(0, []uint32{1}, []uint8{t0})
	assertInboundNeighbors(0, nil, nil)

	// node 1
	assertOutboundNeighbors(1, []uint32{2, 3}, []uint8{t2, t1})
	assertInboundNeighbors(1, []uint32{0, 2, 4}, []uint8{t0, t0, t2})

	// node 3
	assertOutboundNeighbors(3, []uint32{4}, []uint8{t1})
	assertInboundNeighbors(3, []uint32{1}, []uint8{t1})
}
