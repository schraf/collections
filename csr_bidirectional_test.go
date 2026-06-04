package collections

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func biIDs(c *CSR[int64, string], dense []int32) []int64 {
	out := make([]int64, 0, len(dense))
	for _, d := range dense {
		out = append(out, c.ID(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func biOut(t *testing.T, bi *BiCSR[int64, string], id int64) []int64 {
	t.Helper()
	d, ok := bi.Out().Dense(id)
	require.True(t, ok, "node %d missing", id)
	nbrs, _ := bi.Out().Row(d)
	return biIDs(bi.Out(), nbrs)
}

func biIn(t *testing.T, bi *BiCSR[int64, string], id int64) []int64 {
	t.Helper()
	d, ok := bi.In().Dense(id)
	require.True(t, ok, "node %d missing", id)
	nbrs, _ := bi.In().Row(d)
	return biIDs(bi.In(), nbrs)
}

func TestBiCSR(t *testing.T) {
	b := NewBiCSRBuilder[int64, string](0, 0)
	// 1 -> 2 -> 3, plus 1 -> 3
	b.AddEdge(1, 2, "depends_on")
	b.AddEdge(2, 3, "depends_on")
	b.AddEdge(1, 3, "built_from")

	bi, err := b.Build()
	require.NoError(t, err)

	assert.Equal(t, 3, bi.NumNodes())
	assert.Equal(t, 3, bi.NumEdges())

	assert.Equal(t, []int64{2, 3}, biOut(t, bi, 1))
	assert.Equal(t, []int64{1, 2}, biIn(t, bi, 3))
	assert.Empty(t, biIn(t, bi, 1), "nothing points at 1")
}

func TestBiCSRSharedDenseIndex(t *testing.T) {
	b := NewBiCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "a")
	b.AddEdge(2, 3, "a")
	bi, err := b.Build()
	require.NoError(t, err)

	// The same external id must map to the same dense index in both
	// directions so a traversal can carry dense indices across them.
	for _, id := range []int64{1, 2, 3} {
		dOut, okOut := bi.Out().Dense(id)
		dIn, okIn := bi.In().Dense(id)
		require.True(t, okOut)
		require.True(t, okIn)
		assert.Equal(t, dOut, dIn, "dense index mismatch for node %d", id)
	}
}

func TestBiCSRTooManyLabels(t *testing.T) {
	b := NewBiCSRBuilder[int64, int](0, 0)
	for i := 0; i <= MaxCSRLabels; i++ {
		b.AddEdge(1, int64(i+2), i)
	}
	_, err := b.Build()
	require.Error(t, err)
}

func TestBiCSREmpty(t *testing.T) {
	b := NewBiCSRBuilder[int64, string](0, 0)
	bi, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, 0, bi.NumNodes())
	assert.Equal(t, 0, bi.NumEdges())
}
