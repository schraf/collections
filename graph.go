package collections

import (
	"sync"
)

type DenseId = uint32

// ╭────────────────────────────────────────────────────────────────────╮
// │ Directed Graph                                                     │
// ╰────────────────────────────────────────────────────────────────────╯

type DirectedGraph[OffsetType Unsigned, EdgeType any] struct {
	offsets Array[OffsetType]
	edges   Array[DenseId]
	labels  Array[EdgeType]
}

// NewDirectedGraph builds a directed graph from in-memory CSR slices. The
// slices are wrapped in SliceArrays and retained by reference. A nil labels
// slice indicates an unlabeled graph.
func NewDirectedGraph[OffsetType Unsigned, EdgeType any](offsets []OffsetType, edges []DenseId, labels []EdgeType) *DirectedGraph[OffsetType, EdgeType] {
	var labelArray Array[EdgeType]
	if labels != nil {
		labelArray = NewSliceArray(labels)
	}

	return &DirectedGraph[OffsetType, EdgeType]{
		offsets: NewSliceArray(offsets),
		edges:   NewSliceArray(edges),
		labels:  labelArray,
	}
}

// NewDirectedGraphFromArrays builds a directed graph from arbitrary Array
// backends. A nil labels array indicates an unlabeled graph.
func NewDirectedGraphFromArrays[OffsetType Unsigned, EdgeType any](offsets Array[OffsetType], edges Array[DenseId], labels Array[EdgeType]) *DirectedGraph[OffsetType, EdgeType] {
	return &DirectedGraph[OffsetType, EdgeType]{
		offsets: offsets,
		edges:   edges,
		labels:  labels,
	}
}

// Neighbors returns the destination node IDs and corresponding labels for the
// given node. When the graph is backed by in-memory slices the returned slices
// are zero-copy views into internal storage and must not be modified; when
// backed by a paged array the data is copied into freshly allocated slices.
//
// For allocation-free traversal that works identically across backends, prefer
// NeighborsInto.
func (g *DirectedGraph[OffsetType, EdgeType]) Neighbors(denseId DenseId) ([]DenseId, []EdgeType, error) {
	if int(denseId) >= g.offsets.Len()-1 {
		return nil, nil, nil
	}

	startVal, err := g.offsets.At(int(denseId))
	if err != nil {
		return nil, nil, err
	}
	start := int(startVal)

	endVal, err := g.offsets.At(int(denseId) + 1)
	if err != nil {
		return nil, nil, err
	}
	end := int(endVal)

	neighbors, err := g.edges.Slice(start, end, nil)
	if err != nil {
		return nil, nil, err
	}

	var labels []EdgeType

	if g.labels != nil {
		labels, err = g.labels.Slice(start, end, nil)
		if err != nil {
			return nil, nil, err
		}
	}

	return neighbors, labels, nil
}

// NeighborsInto copies the neighbors (and labels, when present) of the given
// node into the supplied buffers, growing them as needed, and returns the
// filled sub-slices. This is allocation-free across repeated calls when the
// buffers are reused and is safe against page eviction for disk-backed graphs.
// Pass a nil labelBuf for unlabeled graphs.
func (g *DirectedGraph[OffsetType, EdgeType]) NeighborsInto(denseId DenseId, edgeBuf []DenseId, labelBuf []EdgeType) ([]DenseId, []EdgeType, error) {
	if int(denseId) >= g.offsets.Len()-1 {
		return edgeBuf[:0], labelBuf[:0], nil
	}

	startVal, err := g.offsets.At(int(denseId))
	if err != nil {
		return nil, nil, err
	}
	start := int(startVal)

	endVal, err := g.offsets.At(int(denseId) + 1)
	if err != nil {
		return nil, nil, err
	}
	end := int(endVal)

	neighbors, err := g.edges.Slice(start, end, edgeBuf)
	if err != nil {
		return nil, nil, err
	}

	var labels []EdgeType
	if g.labels != nil {
		labels, err = g.labels.Slice(start, end, labelBuf)
		if err != nil {
			return nil, nil, err
		}
	}

	return neighbors, labels, nil
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

func (g *MutableDirectedGraph[OffsetType, EdgeType]) Neighbors(denseId DenseId) ([]DenseId, []EdgeType, error) {
	var neighbors []DenseId
	var labels []EdgeType

	baseNeighbors, baseLabels, err := g.DirectedGraph.Neighbors(denseId)
	if err != nil {
		return nil, nil, err
	}

	g.lock.RLock()
	defer g.lock.RUnlock()

	tombstones, hasDeletes := g.removedEdges[denseId]
	_, hasAdded := g.addedEdges[denseId]

	// early out if no modifications have been done to this node
	if !hasDeletes && !hasAdded {
		return baseNeighbors, baseLabels, nil
	}

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

	if added, ok := g.addedEdges[denseId]; ok {
		neighbors = append(neighbors, added...)

		if g.DirectedGraph.labels != nil {
			addedLabels := g.addedLabels[denseId]
			labels = append(labels, addedLabels...)
		}
	}

	return neighbors, labels, nil
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
