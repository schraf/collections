package collections

import (
	"math/rand"
	"testing"
)

// buildRandomBiCSR builds a bidirectional CSR with the given node/edge counts
// and a small label space, using a fixed seed for reproducibility.
func buildRandomBiCSR(b *testing.B, nodes, edges int) *BiCSR[int64, int] {
	b.Helper()

	rng := rand.New(rand.NewSource(1))
	builder := NewBiCSRBuilder[int64, int](nodes, edges)
	for i := 0; i < edges; i++ {
		src := int64(rng.Intn(nodes))
		dst := int64(rng.Intn(nodes))
		builder.AddEdge(src, dst, rng.Intn(12))
	}

	bi, err := builder.Build()
	if err != nil {
		b.Fatalf("build: %v", err)
	}
	return bi
}

func BenchmarkCSRBuild(b *testing.B) {
	const nodes, edges = 1_000_000, 4_000_000
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buildRandomBiCSR(b, nodes, edges)
	}
}

// BenchmarkCSRBFS measures an in-memory BFS over the bidirectional CSR
// following both directions, which is the traversal hot path.
func BenchmarkCSRBFS(b *testing.B) {
	const nodes, edges = 1_000_000, 4_000_000
	bi := buildRandomBiCSR(b, nodes, edges)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		visited := make(map[int32]struct{}, 4096)
		start, _ := bi.Dense(0)
		visited[start] = struct{}{}
		frontier := []int32{start}

		for depth := 0; depth < 4 && len(frontier) > 0; depth++ {
			var next []int32
			for _, node := range frontier {
				for nb := range bi.Out().Neighbors(node) {
					if _, seen := visited[nb]; !seen {
						visited[nb] = struct{}{}
						next = append(next, nb)
					}
				}
				for nb := range bi.In().Neighbors(node) {
					if _, seen := visited[nb]; !seen {
						visited[nb] = struct{}{}
						next = append(next, nb)
					}
				}
			}
			frontier = next
		}
	}
}
