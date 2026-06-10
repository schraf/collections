package collections

import (
	"sync"
)

type DenseId = uint32

// ╭────────────────────────────────────────────────────────────────────╮
// │ Directed Graph                                                     │
// ╰────────────────────────────────────────────────────────────────────╯

type DirectedGraph[OffsetType Unsigned, EdgeType any] struct {
	offsets []OffsetType
	edges   []DenseId
	labels  []EdgeType
}

func NewDirectedGraph[OffsetType Unsigned, EdgeType any](offsets []OffsetType, edges []DenseId, labels []EdgeType) *DirectedGraph[OffsetType, EdgeType] {
	return &DirectedGraph[OffsetType, EdgeType]{
		offsets: offsets,
		edges:   edges,
		labels:  labels,
	}
}

func (g *DirectedGraph[OffsetType, EdgeType]) Neighbors(denseId DenseId) ([]DenseId, []EdgeType) {
	if int(denseId) >= len(g.offsets)-1 {
		return nil, nil
	}

	start := g.offsets[denseId]
	end := g.offsets[denseId+1]

	neighbors := g.edges[start:end]

	var labels []EdgeType

	if g.labels != nil {
		labels = g.labels[start:end]
	}

	return neighbors, labels
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Mutable Directed Graph                                             │
// ╰────────────────────────────────────────────────────────────────────╯

type MutableDirectedGraph[OffsetType Unsigned, EdgeType any] struct {
	DirectedGraph[OffsetType, EdgeType]

	lock         sync.RWMutex
	addedEdges   map[DenseId][]DenseId
	addedLabels  map[DenseId][]EdgeType
	removedEdges map[DenseId]map[DenseId]struct{}
}

func NewMutableDirectedGraph[OffsetType Unsigned, EdgeType any](base DirectedGraph[OffsetType, EdgeType]) *MutableDirectedGraph[OffsetType, EdgeType] {
	return &MutableDirectedGraph[OffsetType, EdgeType]{
		DirectedGraph: base,
		addedEdges:    make(map[DenseId][]DenseId),
		addedLabels:   make(map[DenseId][]EdgeType),
		removedEdges:  make(map[DenseId]map[DenseId]struct{}),
	}
}

func (g *MutableDirectedGraph[OffsetType, EdgeType]) AddEdge(a DenseId, b DenseId) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.addedEdges[a] = append(g.addedEdges[a], b)

	if g.DirectedGraph.labels != nil {
		var label EdgeType
		g.addedLabels[a] = append(g.addedLabels[a], label)
	}
}

func (g *MutableDirectedGraph[OffsetType, EdgeType]) AddEdgeWithLabel(a DenseId, b DenseId, label EdgeType) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.addedEdges[a] = append(g.addedEdges[a], b)

	if g.DirectedGraph.labels != nil {
		g.addedLabels[a] = append(g.addedLabels[a], label)
	}
}

func (g *MutableDirectedGraph[OffsetType, EdgeType]) RemoveEdge(a DenseId, b DenseId) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.removedEdges[a] == nil {
		g.removedEdges[a] = make(map[DenseId]struct{})
	}

	g.removedEdges[a][b] = struct{}{}
}

func (g *MutableDirectedGraph[OffsetType, EdgeType]) Neighbors(denseId DenseId) ([]DenseId, []EdgeType) {
	var neighbors []DenseId
	var labels []EdgeType

	baseNeighbors, baseLabels := g.DirectedGraph.Neighbors(denseId)

	g.lock.RLock()
	defer g.lock.RUnlock()

	tombstones, hasDeletes := g.removedEdges[denseId]

	if hasDeletes {
		for i := 0; i < len(baseNeighbors); i++ {
			neighbor := baseNeighbors[i]

			if _, deleted := tombstones[neighbor]; !deleted {
				neighbors = append(neighbors, neighbor)

				if baseLabels != nil {
					labels = append(labels, baseLabels[i])
				}
			}
		}
	} else if len(baseNeighbors) > 0 {
		neighbors = append(neighbors, baseNeighbors...)

		if baseLabels != nil {
			labels = append(labels, baseLabels...)
		}
	}

	added, hasAdded := g.addedEdges[denseId]
	if hasAdded {
		neighbors = append(neighbors, added...)

		if g.DirectedGraph.labels != nil {
			addedLabels := g.addedLabels[denseId]
			labels = append(labels, addedLabels...)
		}
	}

	return neighbors, labels
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Bidirected Graph                                                   │
// ╰────────────────────────────────────────────────────────────────────╯

type BidirectedGraph[OffsetType Unsigned, EdgeType any] struct {
	Outbound *DirectedGraph[OffsetType, EdgeType]
	Inbound  *DirectedGraph[OffsetType, EdgeType]
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ MutableBidirected Graph                                            │
// ╰────────────────────────────────────────────────────────────────────╯

type MutableBidirectedGraph[OffsetType Unsigned, EdgeType any] struct {
	Outbound *MutableDirectedGraph[OffsetType, EdgeType]
	Inbound  *MutableDirectedGraph[OffsetType, EdgeType]
}

func (m *MutableBidirectedGraph[OffsetType, EdgeType]) AddEdge(a DenseId, b DenseId) {
	m.Outbound.AddEdge(a, b)
	m.Inbound.AddEdge(b, a)
}

func (m *MutableBidirectedGraph[OffsetType, EdgeType]) AddEdgeWithLabel(a DenseId, b DenseId, label EdgeType) {
	m.Outbound.AddEdgeWithLabel(a, b, label)
	m.Inbound.AddEdgeWithLabel(b, a, label)
}

func (m *MutableBidirectedGraph[OffsetType, EdgeType]) RemoveEdge(a DenseId, b DenseId) {
	m.Outbound.RemoveEdge(a, b)
	m.Inbound.RemoveEdge(b, a)
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Graph Builder                                                      │
// ╰────────────────────────────────────────────────────────────────────╯

type GraphBuilder[OffsetType Unsigned, EdgeType any] struct {
	nodeCount  DenseId
	withLabels bool

	froms  []DenseId
	tos    []DenseId
	labels []EdgeType
}

func NewGraphBuilder[OffsetType Unsigned, EdgeType any](nodeCount DenseId, withLabels bool) *GraphBuilder[OffsetType, EdgeType] {
	return &GraphBuilder[OffsetType, EdgeType]{
		nodeCount:  nodeCount,
		withLabels: withLabels,
	}
}

func (b *GraphBuilder[OffsetType, EdgeType]) AddEdgeWithLabel(from DenseId, to DenseId, label EdgeType) {
	b.froms = append(b.froms, from)
	b.tos = append(b.tos, to)

	if b.withLabels {
		b.labels = append(b.labels, label)
	}
}

func (b *GraphBuilder[OffsetType, EdgeType]) AddEdge(from DenseId, to DenseId) {
	b.froms = append(b.froms, from)
	b.tos = append(b.tos, to)

	if b.withLabels {
		var label EdgeType
		b.labels = append(b.labels, label)
	}
}

func (b *GraphBuilder[OffsetType, EdgeType]) BuildInboundGraph() *DirectedGraph[OffsetType, EdgeType] {
	// Inbound is keyed on the edge target; the neighbor is the edge source.
	return b.buildCSR(b.tos, b.froms)
}

func (b *GraphBuilder[OffsetType, EdgeType]) BuildOutboundGraph() *DirectedGraph[OffsetType, EdgeType] {
	// Outbound is keyed on the edge source; the neighbor is the edge target.
	return b.buildCSR(b.froms, b.tos)
}

func (b *GraphBuilder[OffsetType, EdgeType]) buildCSR(keys, neighbors []DenseId) *DirectedGraph[OffsetType, EdgeType] {
	edgeCount := len(keys)
	offsets := make([]OffsetType, b.nodeCount+1)

	// Count degrees: offsets[key+1] accumulates the number of edges keyed on k.
	for _, key := range keys {
		offsets[key+1]++
	}

	// Prefix-sum into start offsets. offsets[i] becomes node i's start
	// position and offsets[nodeCount] == edgeCount.
	for i := DenseId(0); i < b.nodeCount; i++ {
		offsets[i+1] += offsets[i]
	}

	edges := make([]DenseId, edgeCount)

	var labels []EdgeType
	if b.withLabels {
		labels = make([]EdgeType, edgeCount)
	}

	// Cursor tracks the next free slot per node, seeded from the start offsets.
	cursor := make([]OffsetType, b.nodeCount)
	copy(cursor, offsets[:b.nodeCount])

	for i := 0; i < edgeCount; i++ {
		key := keys[i]
		pos := cursor[key]
		edges[pos] = neighbors[i]

		if b.withLabels {
			labels[pos] = b.labels[i]
		}

		cursor[key]++
	}

	return NewDirectedGraph(offsets, edges, labels)
}
