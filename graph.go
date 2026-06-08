package collections

import (
	"sync"
)

// ╭────────────────────────────────────────────────────────────────────╮
// │ Directed Graph                                                     │
// ╰────────────────────────────────────────────────────────────────────╯

type DirectedGraph struct {
	offsets []uint32
	edges   []uint32
	labels  []uint8
}

func NewDirectedGraph(offsets []uint32, edges []uint32, labels []uint8) *DirectedGraph {
	return &DirectedGraph{
		offsets: offsets,
		edges:   edges,
		labels:  labels,
	}
}

func (g *DirectedGraph) Neighbors(nodeId uint32) ([]uint32, []uint8) {
	if int(nodeId) >= len(g.offsets)-1 {
		return nil, nil
	}

	start := g.offsets[nodeId]
	end := g.offsets[nodeId+1]

	return g.edges[start:end], g.labels[start:end]
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Mutable Directed Graph                                             │
// ╰────────────────────────────────────────────────────────────────────╯

type MutableDirectedGraph struct {
	DirectedGraph

	lock         sync.RWMutex
	addedEdges   map[uint32][]uint32
	addedLabels  map[uint32][]uint8
	removedEdges map[uint32]map[uint32]struct{}
}

func NewMutableDirectedGraph(base DirectedGraph) *MutableDirectedGraph {
	return &MutableDirectedGraph{
		DirectedGraph: base,
		addedEdges:    make(map[uint32][]uint32),
		addedLabels:   make(map[uint32][]uint8),
		removedEdges:  make(map[uint32]map[uint32]struct{}),
	}
}

func (g *MutableDirectedGraph) AddEdge(a uint32, b uint32, label uint8) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.addedEdges[a] = append(g.addedEdges[a], b)
	g.addedLabels[a] = append(g.addedLabels[a], label)
}

func (g *MutableDirectedGraph) RemoveEdge(a uint32, b uint32) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.removedEdges[a] == nil {
		g.removedEdges[a] = make(map[uint32]struct{})
	}

	g.removedEdges[a][b] = struct{}{}
}

func (g *MutableDirectedGraph) Neighbors(nodeId uint32) ([]uint32, []uint8) {
	var neighbors []uint32
	var labels []uint8

	baseNeighbors, baseLabels := g.DirectedGraph.Neighbors(nodeId)

	g.lock.RLock()
	defer g.lock.RUnlock()

	tombstones, hasDeletes := g.removedEdges[nodeId]

	if hasDeletes {
		for i := 0; i < len(baseNeighbors); i++ {
			neighbor := baseNeighbors[i]

			if _, deleted := tombstones[neighbor]; !deleted {
				neighbors = append(neighbors, neighbor)
				labels = append(labels, baseLabels[i])
			}
		}
	} else if len(baseNeighbors) > 0 {
		neighbors = append(neighbors, baseNeighbors...)
		labels = append(labels, baseLabels...)
	}

	added, hasAdded := g.addedEdges[nodeId]
	if hasAdded {
		addedLabels := g.addedLabels[nodeId]

		neighbors = append(neighbors, added...)
		labels = append(labels, addedLabels...)
	}

	return neighbors, labels
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Bidirected Graph                                                   │
// ╰────────────────────────────────────────────────────────────────────╯

type BidirectedGraph struct {
	Outbound *DirectedGraph
	Inbound  *DirectedGraph
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ MutableBidirected Graph                                            │
// ╰────────────────────────────────────────────────────────────────────╯

type MutableBidirectedGraph struct {
	Outbound *MutableDirectedGraph
	Inbound  *MutableDirectedGraph
}

func (m *MutableBidirectedGraph) AddEdge(a uint32, b uint32, label uint8) {
	m.Outbound.AddEdge(a, b, label)
	m.Inbound.AddEdge(b, a, label)
}

func (m *MutableBidirectedGraph) RemoveEdge(a uint32, b uint32) {
	m.Outbound.RemoveEdge(a, b)
	m.Inbound.RemoveEdge(b, a)
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Graph Builder                                                      │
// ╰────────────────────────────────────────────────────────────────────╯

type GraphBuilder struct {
	nodeCount      uint32
	inbound        map[uint32][]uint32
	outbound       map[uint32][]uint32
	inboundLabels  map[uint32][]uint8
	outboundLabels map[uint32][]uint8
}

func NewGraphBuilder(nodeCount uint32) *GraphBuilder {
	return &GraphBuilder{
		nodeCount:      nodeCount,
		inbound:        make(map[uint32][]uint32, nodeCount),
		outbound:       make(map[uint32][]uint32, nodeCount),
		inboundLabels:  make(map[uint32][]uint8, nodeCount),
		outboundLabels: make(map[uint32][]uint8, nodeCount),
	}
}

func (b *GraphBuilder) AddEdge(from uint32, to uint32, label uint8) {
	if b.outbound[from] == nil {
		b.outbound[from] = []uint32{}
		b.outboundLabels[from] = []uint8{}
	}

	b.outbound[from] = append(b.outbound[from], to)
	b.outboundLabels[from] = append(b.outboundLabels[from], label)

	if b.inbound[to] == nil {
		b.inbound[to] = []uint32{}
		b.inboundLabels[to] = []uint8{}
	}

	b.inbound[to] = append(b.inbound[to], from)
	b.inboundLabels[to] = append(b.inboundLabels[to], label)
}

func (b *GraphBuilder) BuildInboundGraph() *DirectedGraph {
	offsets := make([]uint32, b.nodeCount+1)

	var edges []uint32
	var labels []uint8
	var index uint32

	for index = 0; index < b.nodeCount; index++ {
		offsets[index] = uint32(len(edges))

		if neighbors, ok := b.inbound[index]; ok {
			neighborLabels := b.inboundLabels[index]

			edges = append(edges, neighbors...)
			labels = append(labels, neighborLabels...)
		}
	}

	offsets[b.nodeCount] = uint32(len(edges))

	return NewDirectedGraph(offsets, edges, labels)
}

func (b *GraphBuilder) BuildOutboundGraph() *DirectedGraph {
	offsets := make([]uint32, b.nodeCount+1)

	var edges []uint32
	var labels []uint8
	var index uint32

	for index = 0; index < b.nodeCount; index++ {
		offsets[index] = uint32(len(edges))

		if neighbors, ok := b.outbound[index]; ok {
			neighborLabels := b.outboundLabels[index]

			edges = append(edges, neighbors...)
			labels = append(labels, neighborLabels...)
		}
	}

	offsets[b.nodeCount] = uint32(len(edges))

	return NewDirectedGraph(offsets, edges, labels)
}
