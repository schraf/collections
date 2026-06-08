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
	nodeCount      DenseId
	inbound        map[DenseId][]DenseId
	outbound       map[DenseId][]DenseId
	inboundLabels  map[DenseId][]EdgeType
	outboundLabels map[DenseId][]EdgeType
}

func NewGraphBuilder[OffsetType Unsigned, EdgeType any](nodeCount DenseId, withLabels bool) *GraphBuilder[OffsetType, EdgeType] {
	builder := &GraphBuilder[OffsetType, EdgeType]{
		nodeCount: nodeCount,
		inbound:   make(map[DenseId][]DenseId, nodeCount),
		outbound:  make(map[DenseId][]DenseId, nodeCount),
	}

	if withLabels {
		builder.inboundLabels = make(map[DenseId][]EdgeType, nodeCount)
		builder.outboundLabels = make(map[DenseId][]EdgeType, nodeCount)
	}

	return builder
}

func (b *GraphBuilder[OffsetType, EdgeType]) AddEdgeWithLabel(from DenseId, to DenseId, label EdgeType) {
	if b.outbound[from] == nil {
		b.outbound[from] = []DenseId{}
		b.outboundLabels[from] = []EdgeType{}
	}

	b.outbound[from] = append(b.outbound[from], to)
	b.outboundLabels[from] = append(b.outboundLabels[from], label)

	if b.inbound[to] == nil {
		b.inbound[to] = []DenseId{}
		b.inboundLabels[to] = []EdgeType{}
	}

	b.inbound[to] = append(b.inbound[to], from)
	b.inboundLabels[to] = append(b.inboundLabels[to], label)
}

func (b *GraphBuilder[OffsetType, EdgeType]) AddEdge(from DenseId, to DenseId) {
	var label EdgeType

	if b.outbound[from] == nil {
		b.outbound[from] = []DenseId{}

		if b.outboundLabels != nil {
			b.outboundLabels[from] = []EdgeType{}
		}
	}

	b.outbound[from] = append(b.outbound[from], to)

	if b.outboundLabels != nil {
		b.outboundLabels[from] = append(b.outboundLabels[from], label)
	}

	if b.inbound[to] == nil {
		b.inbound[to] = []DenseId{}

		if b.inboundLabels != nil {
			b.inboundLabels[to] = []EdgeType{}
		}
	}

	b.inbound[to] = append(b.inbound[to], from)

	if b.inboundLabels != nil {
		b.inboundLabels[to] = append(b.inboundLabels[to], label)
	}
}

func (b *GraphBuilder[OffsetType, EdgeType]) BuildInboundGraph() *DirectedGraph[OffsetType, EdgeType] {
	offsets := make([]OffsetType, b.nodeCount+1)

	var edges []DenseId
	var labels []EdgeType
	var index DenseId

	for index = DenseId(0); index < b.nodeCount; index++ {
		offsets[index] = OffsetType(len(edges))

		if neighbors, ok := b.inbound[index]; ok {
			edges = append(edges, neighbors...)

			if b.inboundLabels != nil {
				neighborLabels := b.inboundLabels[index]
				labels = append(labels, neighborLabels...)
			}
		}
	}

	offsets[b.nodeCount] = OffsetType(len(edges))

	return NewDirectedGraph(offsets, edges, labels)
}

func (b *GraphBuilder[OffsetType, EdgeType]) BuildOutboundGraph() *DirectedGraph[OffsetType, EdgeType] {
	offsets := make([]OffsetType, b.nodeCount+1)

	var edges []DenseId
	var labels []EdgeType
	var index DenseId

	for index = DenseId(0); index < b.nodeCount; index++ {
		offsets[index] = OffsetType(len(edges))

		if neighbors, ok := b.outbound[index]; ok {
			edges = append(edges, neighbors...)

			if b.outboundLabels != nil {
				neighborLabels := b.outboundLabels[index]
				labels = append(labels, neighborLabels...)
			}
		}
	}

	offsets[b.nodeCount] = OffsetType(len(edges))

	return NewDirectedGraph(offsets, edges, labels)
}
