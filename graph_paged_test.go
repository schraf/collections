package collections

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildInMemoryGraph constructs the same small labeled graph used across these
// tests and returns its outbound directed graph.
func buildInMemoryOutbound(t *testing.T) *DirectedGraph[uint64, uint8] {
	t.Helper()
	b := NewGraphBuilder[uint64, uint8](5, true)
	b.AddEdgeWithLabel(0, 1, 10)
	b.AddEdgeWithLabel(0, 3, 11)
	b.AddEdgeWithLabel(1, 2, 12)
	b.AddEdgeWithLabel(2, 1, 13)
	b.AddEdgeWithLabel(3, 4, 14)
	b.AddEdgeWithLabel(4, 1, 15)
	return b.BuildOutboundGraph()
}

// sortedRow returns a node's neighbors and labels sorted by neighbor id so that
// graphs built via different mechanisms can be compared regardless of row order.
func sortedRow(neighbors []uint32, labels []uint8) ([]uint32, []uint8) {
	type pair struct {
		n uint32
		l uint8
	}
	pairs := make([]pair, len(neighbors))
	for i := range neighbors {
		pairs[i].n = neighbors[i]
		if labels != nil {
			pairs[i].l = labels[i]
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].n < pairs[j].n })

	outN := make([]uint32, len(pairs))
	outL := make([]uint8, len(pairs))
	for i, p := range pairs {
		outN[i] = p.n
		outL[i] = p.l
	}
	return outN, outL
}

// pagedCopyOf writes a DirectedGraph's CSR arrays out to paged files and reopens
// them with a tiny RAM budget, returning a paged-backed equivalent graph.
func pagedCopyOf(t *testing.T, g *DirectedGraph[uint64, uint8]) *DirectedGraph[uint64, uint8] {
	t.Helper()
	dir := t.TempDir()

	// Extract the CSR arrays from the in-memory graph.
	n := g.offsets.Len()
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		offsets[i] = g.offsets.At(i)
	}
	edgeCount := g.edges.Len()
	edges := make([]uint32, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges[i] = g.edges.At(i)
	}
	labels := make([]uint8, edgeCount)
	for i := 0; i < edgeCount; i++ {
		labels[i] = g.labels.At(i)
	}

	offPath := filepath.Join(dir, "offsets")
	edgePath := filepath.Join(dir, "edges")
	labelPath := filepath.Join(dir, "labels")

	// Tiny page size to force multi-page spans and eviction.
	require.NoError(t, CreatePagedArrayFile(offPath, offsets, 16))
	require.NoError(t, CreatePagedArrayFile(edgePath, edges, 16))
	require.NoError(t, CreatePagedArrayFile(labelPath, labels, 16))

	offsetsPA, err := OpenPagedArray[uint64](offPath, 32)
	require.NoError(t, err)
	edgesPA, err := OpenPagedArray[uint32](edgePath, 32)
	require.NoError(t, err)
	labelsPA, err := OpenPagedArray[uint8](labelPath, 32)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = offsetsPA.Close()
		_ = edgesPA.Close()
		_ = labelsPA.Close()
	})

	return NewDirectedGraphFromArrays[uint64, uint8](offsetsPA, edgesPA, labelsPA)
}

func TestDirectedGraph_PagedBackendEquivalence(t *testing.T) {
	mem := buildInMemoryOutbound(t)
	paged := pagedCopyOf(t, mem)

	for node := uint32(0); node < 5; node++ {
		mn, ml := mem.Neighbors(node)
		pn, pl := paged.Neighbors(node)

		msn, msl := sortedRow(mn, ml)
		psn, psl := sortedRow(pn, pl)

		assert.Equal(t, msn, psn, "neighbors of node %d", node)
		assert.Equal(t, msl, psl, "labels of node %d", node)
	}
}

func TestDirectedGraph_NeighborsInto(t *testing.T) {
	paged := pagedCopyOf(t, buildInMemoryOutbound(t))

	// Reuse buffers across calls; results must be correct and stable.
	edgeBuf := make([]uint32, 0)
	labelBuf := make([]uint8, 0)

	n0, l0 := paged.NeighborsInto(0, edgeBuf, labelBuf)
	s0n, s0l := sortedRow(n0, l0)
	assert.Equal(t, []uint32{1, 3}, s0n)
	assert.Equal(t, []uint8{10, 11}, s0l)

	// Detach by copying before the next call reuses the buffer.
	saved := append([]uint32(nil), n0...)

	n3, _ := paged.NeighborsInto(3, edgeBuf, labelBuf)
	sn3, _ := sortedRow(n3, nil)
	assert.Equal(t, []uint32{4}, sn3)

	// The detached copy of node 0's neighbors is unaffected.
	ss, _ := sortedRow(saved, nil)
	assert.Equal(t, []uint32{1, 3}, ss)
}

func TestStreamingGraphBuilder_MatchesInMemory(t *testing.T) {
	mem := buildInMemoryOutbound(t)

	dir := t.TempDir()
	sb, err := NewStreamingGraphBuilder[uint64, uint8](dir, 5, true, 64, 16)
	require.NoError(t, err)
	defer sb.Close()

	require.NoError(t, sb.AddEdgeWithLabel(0, 1, 10))
	require.NoError(t, sb.AddEdgeWithLabel(0, 3, 11))
	require.NoError(t, sb.AddEdgeWithLabel(1, 2, 12))
	require.NoError(t, sb.AddEdgeWithLabel(2, 1, 13))
	require.NoError(t, sb.AddEdgeWithLabel(3, 4, 14))
	require.NoError(t, sb.AddEdgeWithLabel(4, 1, 15))

	out, err := sb.BuildOutboundGraph()
	require.NoError(t, err)

	for node := uint32(0); node < 5; node++ {
		mn, ml := mem.Neighbors(node)
		sn, sl := out.Neighbors(node)

		msn, msl := sortedRow(mn, ml)
		ssn, ssl := sortedRow(sn, sl)

		assert.Equal(t, msn, ssn, "neighbors of node %d", node)
		assert.Equal(t, msl, ssl, "labels of node %d", node)
	}
}

func TestStreamingGraphBuilder_InboundMatchesInMemory(t *testing.T) {
	b := NewGraphBuilder[uint64, uint8](5, true)
	b.AddEdgeWithLabel(0, 1, 10)
	b.AddEdgeWithLabel(0, 3, 11)
	b.AddEdgeWithLabel(1, 2, 12)
	b.AddEdgeWithLabel(2, 1, 13)
	b.AddEdgeWithLabel(3, 4, 14)
	b.AddEdgeWithLabel(4, 1, 15)
	memIn := b.BuildInboundGraph()

	dir := t.TempDir()
	sb, err := NewStreamingGraphBuilder[uint64, uint8](dir, 5, true, 64, 16)
	require.NoError(t, err)
	defer sb.Close()

	require.NoError(t, sb.AddEdgeWithLabel(0, 1, 10))
	require.NoError(t, sb.AddEdgeWithLabel(0, 3, 11))
	require.NoError(t, sb.AddEdgeWithLabel(1, 2, 12))
	require.NoError(t, sb.AddEdgeWithLabel(2, 1, 13))
	require.NoError(t, sb.AddEdgeWithLabel(3, 4, 14))
	require.NoError(t, sb.AddEdgeWithLabel(4, 1, 15))

	in, err := sb.BuildInboundGraph()
	require.NoError(t, err)

	for node := uint32(0); node < 5; node++ {
		mn, ml := memIn.Neighbors(node)
		sn, sl := in.Neighbors(node)

		msn, msl := sortedRow(mn, ml)
		ssn, ssl := sortedRow(sn, sl)

		assert.Equal(t, msn, ssn, "inbound neighbors of node %d", node)
		assert.Equal(t, msl, ssl, "inbound labels of node %d", node)
	}
}

func TestStreamingGraphBuilder_Unlabeled(t *testing.T) {
	dir := t.TempDir()
	sb, err := NewStreamingGraphBuilder[uint32, uint8](dir, 4, false, 64, 16)
	require.NoError(t, err)
	defer sb.Close()

	require.NoError(t, sb.AddEdge(0, 1))
	require.NoError(t, sb.AddEdge(0, 2))
	require.NoError(t, sb.AddEdge(3, 1))

	out, err := sb.BuildOutboundGraph()
	require.NoError(t, err)

	n0, l0 := out.Neighbors(0)
	sn, _ := sortedRow(n0, nil)
	assert.Equal(t, []uint32{1, 2}, sn)
	assert.Nil(t, l0)
}
