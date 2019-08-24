package graph

// Does not error, just like any other in-memory graph using adjacency list.
type inMemoryPolicy struct {
	m     map[string]Edges
	graph string
}

func NewInMemoryPolicy(graph string) inMemoryPolicy {
	return inMemoryPolicy{
		m:     make(map[string]Edges),
		graph: graph,
	}
}

func (p inMemoryPolicy) Name() string {
	return p.graph
}

func (p inMemoryPolicy) Vertices() ([]string, error) {
	vs := make([]string, 0, len(p.m))
	for v := range p.m {
		vs = append(vs, v)
	}

	return vs, nil
}

func (p inMemoryPolicy) Edges(vertex string) (Edges, bool, error) {
	edges, ok := p.m[vertex]
	return edges.Copy(), ok, nil
}

func (p inMemoryPolicy) InsertEdges(edges ...InsertEdge) error {
	for _, edge := range edges {
		dst, _ := p.m[edge.Dst]
		dst.InitIfEmpty()
		dst.InEdges[edge.Src] = edge.Val
		p.m[edge.Dst] = dst

		src, _ := p.m[edge.Src]
		src.InitIfEmpty()
		src.OutEdges[edge.Dst] = edge.Val
		p.m[edge.Src] = src
	}
	return nil
}

func (p inMemoryPolicy) RemoveEdges(edges ...RemoveEdge) error {
	for _, edge := range edges {
		// No need to write back if map is empty, there is nothing to
		// delete.
		dst, _ := p.m[edge.Dst]
		delete(dst.InEdges, edge.Src)

		src, _ := p.m[edge.Src]
		delete(src.OutEdges, edge.Dst)
	}
	return nil
}

func (p inMemoryPolicy) InsertVertices(vertices ...string) error {
	for _, v := range vertices {
		edges := p.m[v]
		edges.InitIfEmpty()
		p.m[v] = edges
	}

	return nil
}

func (p inMemoryPolicy) RemoveVertices(vertices ...string) error {
	for _, v := range vertices {
		delete(p.m, v)
	}

	return nil
}

func (p inMemoryPolicy) Commit() error {
	return nil
}
