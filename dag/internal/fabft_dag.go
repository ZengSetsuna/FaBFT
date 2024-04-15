package internal

import (
	"fmt"
	"sort"

	"github.com/tuannh982/dag-bft/dag/commons"
)

type FabftDAG interface {
	//VertexExist(v *commons.BaseVertex) bool
	//AllEdgesExist(v *commons.Vertex) bool
	NewRoundIfNotExists(r commons.Round)
	GetRound(r commons.Round) VertexRoundSet
	//SetDelivered(v *commons.Vertex, delivered bool)
	String() string
}

type FabftVertexRoundSet interface {
	Entries() []commons.Vertex
	AddVertex(v commons.Vertex) bool
	//SourceExists(a commons.Address) bool
	//GetBySource(a commons.Address) commons.Vertex
	//SetDelivered(a commons.Address, delivered bool)
	Size() int
	VertexMap() map[commons.Address]commons.Vertex
}

type fabftVertexRoundSet struct {
	internal map[commons.VHash]commons.Vertex
}

type fabftdag struct {
	internal map[commons.Round]FabftVertexRoundSet
}

func NewFabftDAG() FabftDAG {
	return &fabftdag{
		internal: make(map[commons.Round]VertexRoundSet),
	}
}

/*func (dag *fabftdag) VertexExist(v *commons.BaseVertex) bool {
	if v == nil {
		return false
	}
	if roundSet, ok := dag.internal[v.Round]; ok {
		return roundSet.SourceExists(v.Source)
	}
	return false
}*/

/*func (dag *fabftdag) AllEdgesExist(v *commons.Vertex) bool {
	for _, u := range v.StrongEdges {
		if !dag.VertexExist(&u) {
			return false
		}
	}
	for _, u := range v.WeakEdges {
		if !dag.VertexExist(&u) {
			return false
		}
	}
	return true
}*/

/*func (dag *fabftdag) Path(v, u *commons.Vertex) bool {
	if v == nil || u == nil {
		return false
	}
	if !dag.VertexExist(&v.BaseVertex) || !dag.VertexExist(&u.BaseVertex) {
		return false
	}
	if v.Round == u.Round {
		if commons.Equals(v, u) {
			return true
		} else {
			return false
		}
	}
	root := v
	arr := collections.NewQueue[*commons.Vertex]()
	visited := make(map[string]bool)
	arr.Push(root)
	visited[root.Hash()] = true
	for arr.Size() > 0 {
		top := arr.Pop()
		for _, x := range top.StrongEdgesValues() {
			if _, found := visited[x.Hash()]; !found {
				xx := dag.GetRound(x.Round).GetBySource(x.Source)
				if commons.Equals(u, &xx) {
					return true
				}
				arr.Push(&xx)
				visited[x.Hash()] = true
			}
		}
		for _, x := range top.WeakEdgesValues() {
			if _, found := visited[x.Hash()]; !found {
				xx := dag.GetRound(x.Round).GetBySource(x.Source)
				if commons.Equals(u, &xx) {
					return true
				}
				arr.Push(&xx)
				visited[x.Hash()] = true
			}
		}
	}
	return false
}

func (dag *dag) StrongPath(v, u *commons.Vertex) bool {
	if v == nil || u == nil {
		return false
	}
	if !dag.VertexExist(&v.BaseVertex) || !dag.VertexExist(&u.BaseVertex) {
		return false
	}
	if v.Round == u.Round {
		if commons.Equals(v, u) {
			return true
		} else {
			return false
		}
	}
	root := v
	arr := collections.NewQueue[*commons.Vertex]()
	visited := make(map[string]bool)
	arr.Push(root)
	visited[root.Hash()] = true
	for arr.Size() > 0 {
		top := arr.Pop()
		for _, x := range top.StrongEdgesValues() {
			if _, found := visited[x.Hash()]; !found {
				xx := dag.GetRound(x.Round).GetBySource(x.Source)
				if commons.Equals(u, &xx) {
					return true
				}
				arr.Push(&xx)
				visited[x.Hash()] = true
			}
		}
	}
	return false
}*/

func (dag *fabftdag) NewRoundIfNotExists(r commons.Round) {
	if _, ok := dag.internal[r]; !ok {
		dag.internal[r] = &fabftVertexRoundSet{
			internal: make(map[commons.VHash]commons.Vertex),
		}
	}
}

func (dag *fabftdag) GetRound(r commons.Round) FabftVertexRoundSet {
	return dag.internal[r]
}

/*
	func (dag *dag) SetDelivered(v *commons.Vertex, delivered bool) {
		if dag.VertexExist(&v.BaseVertex) {
			dag.GetRound(v.Round).SetDelivered(v.Source, delivered)
		}
	}
*/
func (dag *fabftdag) String() string {
	m := make(map[commons.Round][]string)
	roundSet := make([]commons.Round, 0, len(dag.internal))
	for round, vertices := range dag.internal {
		s := make([]string, 0)
		for _, vp := range vertices.Entries() {
			s = append(s, vp.String())
		}
		m[round] = s
		roundSet = append(roundSet, round)
	}
	sort.Slice(roundSet, func(i, j int) bool {
		return roundSet[i] < roundSet[j]
	})
	ret := ""
	for _, round := range roundSet {
		ret += fmt.Sprintf("%d:%s\n", round, m[round])
	}
	return ret
}

func (s *fabftVertexRoundSet) Entries() []commons.Vertex {
	arr := make([]commons.Vertex, 0, len(s.internal))
	for _, v := range s.internal {
		arr = append(arr, v)
	}
	return arr
}

func (s *fabftVertexRoundSet) AddVertex(v commons.Vertex) bool {
	s.internal[v.VertexHash] = v
	return true
}

/*
	func (s *vertexRoundSet) SourceExists(a commons.Address) bool {
		if _, found := s.internal[a]; found {
			return true
		}
		return false
	}
*/
func (s *fabftVertexRoundSet) GetByHash(vh commons.VHash) commons.Vertex {
	return s.internal[vh]
}

/*
	func (s *vertexRoundSet) SetDelivered(a commons.Address, delivered bool) {
		if s.SourceExists(a) {
			v := s.internal[a]
			v.Delivered = delivered
			s.internal[a] = v
		}
	}
*/
func (s *fabftVertexRoundSet) Size() int {
	return len(s.internal)
}

// FindTips returns the tips of the DAG.
func (dag *fabftdag) FindTips() []commons.Vertex {
	tips := make([]commons.Vertex, 0)

	// Iterate through all vertices in the DAG
	for round, roundSet := range dag.internal {
		for _, vertex := range roundSet.Entries() {
			// Check if the vertex has no descendants
			if dag.isTip(round, vertex) {
				tips = append(tips, vertex)
			}
		}
	}

	return tips
}

// isTip checks if the vertex is a tip (has no descendants).
func (dag *fabftdag) isTip(round commons.Round, vertex commons.Vertex) bool {
	// Check if there are any edges pointing to descendants of the vertex
	for _, roundSet := range dag.internal {
		for _, otherVertex := range roundSet.Entries() {
			if dag.Path(&vertex, &otherVertex) || dag.StrongPath(&vertex, &otherVertex) {
				return false
			}
		}
	}
	return true
}

func (s *fabftVertexRoundSet) VertexMap() map[commons.VHash]commons.Vertex {
	return s.internal
}
