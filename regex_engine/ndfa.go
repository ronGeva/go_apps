package regex_engine

type NondeterministicNode struct {
	next            map[byte][]int
	is_accept_state bool
}

func newNondeterministicNode() *NondeterministicNode {
	node := NondeterministicNode{}
	node.next = make(map[byte][]int)
	node.is_accept_state = false
	return &node
}

type NondeterministicAutomata struct {
	nodes []*NondeterministicNode
}

type DeterministicAutomata = CompiledPattern

type NodeShift struct {
	src_node []int
	dst_node []int
	value    byte
}

func getUnique64BitKey(numbers []int) uint64 {
	var res uint64 = 0
	for _, num := range numbers {
		// TODO: assert all numbers are unique
		// TODO: assert all numbers are smaller than 63
		res += (1 << num)
	}
	return res
}

func buildShiftMapAndJokerDestinations(ndfa *NondeterministicAutomata, node_id []int) (map[byte]map[int]struct{}, map[int]struct{}) {
	shifts_map := make(map[byte]map[int]struct{})
	joker_dst_nodes := make(map[int]struct{})
	for _, node_index := range node_id {
		ndfa_node := ndfa.nodes[node_index]
		for key, val := range ndfa_node.next {
			if key == '.' {
				for _, dst_index := range val {
					joker_dst_nodes[dst_index] = struct{}{}
				}
				continue
			}
			if _, exists := shifts_map[key]; !exists {
				shifts_map[key] = make(map[int]struct{})
			}
			for _, dst_index := range val {
				shifts_map[key][dst_index] = struct{}{}
			}
		}
	}
	return shifts_map, joker_dst_nodes
}

func buildNewNodeShifts(shifts_map map[byte]map[int]struct{}, joker_dst_nodes map[int]struct{}, node_id []int) []NodeShift {
	new_node_shifts := make([]NodeShift, 0)
	for key, shift := range shifts_map {
		dst_indexes := make([]int, 0)
		for dst_index := range shift {
			dst_indexes = append(dst_indexes, dst_index)
		}
		new_node_shifts = append(new_node_shifts,
			NodeShift{src_node: node_id, dst_node: dst_indexes, value: key})
	}

	joker_node_shift_dst_nodes := make([]int, 0)
	for dst_index := range joker_dst_nodes {
		joker_node_shift_dst_nodes = append(joker_node_shift_dst_nodes, dst_index)
		for i := 0; i < len(new_node_shifts); i++ {
			new_node_shifts[i].dst_node = append(new_node_shifts[i].dst_node, dst_index)
		}
	}
	new_node_shifts = append(new_node_shifts, NodeShift{src_node: node_id,
		dst_node: joker_node_shift_dst_nodes, value: '.'})
	return new_node_shifts
}

func addNodeShifts(ndfa *NondeterministicAutomata, node_id []int, node_shifts *[]NodeShift) {
	shifts_map, joker_dst_nodes := buildShiftMapAndJokerDestinations(ndfa, node_id)
	new_node_shifts := buildNewNodeShifts(shifts_map, joker_dst_nodes, node_id)
	*node_shifts = append(*node_shifts, new_node_shifts...)
}

func getAcceptState(node_id []int, ndfa *NondeterministicAutomata) bool {
	for _, index := range node_id {
		if ndfa.nodes[index].is_accept_state {
			return true
		}
	}
	return false
}

func (ndfa *NondeterministicAutomata) convertToDeterministic() *DeterministicAutomata {
	if len(ndfa.nodes) > 64 {
		// We use 64 bits to represent a unique set of ndfa nodes, which means we currently
		// do not support conversion of NDFA with more than 64 nodes
		return nil
	}

	dfa := newCompiledPattern()

	// Scan the ndfa graph
	node_shifts := make([]NodeShift, 0)
	visited_nodes := make(map[uint64]int)
	addNodeShifts(ndfa, []int{0}, &node_shifts)

	dfa.nodes[0] = newAutomataNode()
	visited_nodes[getUnique64BitKey([]int{0})] = 0

	for len(node_shifts) > 0 {
		curr_ns := node_shifts[len(node_shifts)-1]
		node_shifts = node_shifts[:len(node_shifts)-1]

		src_key := getUnique64BitKey(curr_ns.src_node)
		dst_key := getUnique64BitKey(curr_ns.dst_node)
		var dst_node_index int
		exists := false
		if dst_node_index, exists = visited_nodes[dst_key]; !exists {
			dst_node := newAutomataNode()
			dst_node.is_accept_state = getAcceptState(curr_ns.dst_node, ndfa)
			dfa.nodes = append(dfa.nodes, dst_node)
			addNodeShifts(ndfa, curr_ns.dst_node, &node_shifts)
			visited_nodes[dst_key] = len(dfa.nodes) - 1
			dst_node_index = len(dfa.nodes) - 1
		}
		// TODO: validate the source node is the map
		src_node := dfa.nodes[visited_nodes[src_key]]
		src_node.next[curr_ns.value] = dst_node_index
	}

	return dfa
}

func CreateNDFA(pattern string) NondeterministicAutomata {
	nodes := make([]*NondeterministicNode, 0)
	nodes = append(nodes, newNondeterministicNode())
	current_node := nodes[0]
	for i := 0; i < len(pattern); {
		current_val := pattern[i]
		next_node := newNondeterministicNode()
		nodes = append(nodes, next_node)

		if (i < len(pattern)-1) && (pattern[i+1] == '+') {
			current_node.next[current_val] = []int{len(nodes) - 1, len(nodes) - 2}
			i += 2
		} else {
			current_node.next[current_val] = []int{len(nodes) - 1}
			i++
		}
		current_node = next_node
	}
	nodes[len(nodes)-1].is_accept_state = true
	return NondeterministicAutomata{nodes: nodes}
}
