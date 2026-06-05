package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSREdgeRangeAndEdges(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "a")
	b.AddEdge(1, 3, "b")
	b.AddEdge(2, 3, "a")
	c, err := b.Build()
	require.NoError(t, err)

	d1, _ := c.Dense(1)
	start, end := c.EdgeRange(d1)
	assert.Equal(t, int32(2), end-start, "node 1 has 2 outgoing edges")

	// Row()[k] corresponds to flat position start+k.
	nbrs, labels := c.Row(d1)
	require.Len(t, nbrs, 2)
	for k := range nbrs {
		pos := start + int32(k)
		assert.GreaterOrEqual(t, pos, start)
		assert.Less(t, pos, end)
		assert.Equal(t, nbrs[k], c.neighbors[pos])
		assert.Equal(t, labels[k], c.labels[pos])
	}
}

func TestBuildBiCSRWithData(t *testing.T) {
	b := NewBiCSRBuilder[int64, string](0, 0)
	// Edge payload = a synthetic "edge id" per AddEdge, in accumulation order.
	b.AddEdge(1, 2, "depends_on") // id 100
	b.AddEdge(2, 3, "depends_on") // id 101
	b.AddEdge(1, 3, "built_from") // id 102
	ids := []int64{100, 101, 102}

	bi, outIds, inIds, err := BuildBiCSRWithData(b, ids)
	require.NoError(t, err)
	require.Len(t, outIds, 3)
	require.Len(t, inIds, 3)

	// Outbound: walking node 1's edges, the payload at each flat position must
	// be the id of that specific edge.
	out := bi.Out()
	d1, _ := out.Dense(1)
	start, _ := out.EdgeRange(d1)
	nbrs, _ := out.Row(d1)
	got := map[int64]int64{} // neighborId -> edgeId
	for k, neighbor := range nbrs {
		got[out.NodeId(neighbor)] = outIds[start+int32(k)]
	}
	assert.Equal(t, map[int64]int64{2: 100, 3: 102}, got)

	// Inbound: walking node 3's inbound edges, payload must match the same
	// logical edges (101 from 2->3, 102 from 1->3).
	in := bi.In()
	d3, _ := in.Dense(3)
	inStart, _ := in.EdgeRange(d3)
	inNbrs, _ := in.Row(d3)
	gotIn := map[int64]int64{} // sourceId -> edgeId
	for k, neighbor := range inNbrs {
		gotIn[in.NodeId(neighbor)] = inIds[inStart+int32(k)]
	}
	assert.Equal(t, map[int64]int64{2: 101, 1: 102}, gotIn)
}

func TestBuildBiCSRWithData_LengthMismatch(t *testing.T) {
	b := NewBiCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "a")
	b.AddEdge(2, 3, "a")

	_, _, _, err := BuildBiCSRWithData(b, []int64{1}) // wrong length
	require.Error(t, err)
}

func TestPermuteCSRData(t *testing.T) {
	// perm[i] = position of accumulation-index i.
	data := []string{"a", "b", "c"}
	perm := []int32{2, 0, 1}
	out := PermuteCSRData(data, perm)
	assert.Equal(t, []string{"b", "c", "a"}, out)
}
