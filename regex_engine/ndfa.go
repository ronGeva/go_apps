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

type EpsilonShift struct {
	src_node int
	dst_node int
}

type NondeterministicAutomata struct {
	nodes          []*NondeterministicNode
	epsilon_shifts []EpsilonShift
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

/*
Creates the shifts map and the joker destination nodes.
The shifts map is simply a map of byte to a set of integers representing the destination
nodes the current node arrives at when encountering the key byte.
The joker destination nodes are all the nodes to which we can reach from the current node
when encountering a joker ('.') value.
This extra structure is necessary since we need to treat these differently than other "specific"
values.
*/
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

/*
Given shifts_map (the mapping of each byte into a set of nodes) and the joker destination nodes,
creates all the node shifts whose src node is node_id.

This is done by simply creating a node shift for every (key, val) pair in shifts_map,
and then adding the joker destination nodes to the val of each pair, as well as adding
the necessary node shifts for the joker value.
*/
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

/*
Adds the node shifts whose source node is node_id, according to the NDFA.
*/
func addNodeShifts(ndfa *NondeterministicAutomata, node_id []int, node_shifts *[]NodeShift) {
	shifts_map, joker_dst_nodes := buildShiftMapAndJokerDestinations(ndfa, node_id)
	new_node_shifts := buildNewNodeShifts(shifts_map, joker_dst_nodes, node_id)
	*node_shifts = append(*node_shifts, new_node_shifts...)
}

/*
Calculate the accept state of a node int the new determinstic automata.
A node is in accept state only if node_id contains the id of a node that is:
1. In accept state in the NDFA.
or
2. Has an epsilon shift to a node that is in accept state in the NDFA.
*/
func getAcceptState(node_id []int, ndfa *NondeterministicAutomata) bool {
	for _, index := range node_id {
		if ndfa.nodes[index].is_accept_state {
			return true
		}
	}
	return false
}

// TODO: move to utils or use third party
func joinSlices(first []int, second []int) []int {
	items := make(map[int]struct{}, 0)
	for _, item := range first {
		items[item] = struct{}{}
	}
	for _, item := range second {
		items[item] = struct{}{}
	}
	items_slice := make([]int, 0)
	for item := range items {
		items_slice = append(items_slice, item)
	}
	return items_slice
}

/*
For each epsilon shift {a, b} add the outgoing edges of b to a.
Also, if b is in accept state, then a is as well (since we can get to b from a with 0 new characters).
*/
func resolveEpsilonShifts(epsilon_shifts []EpsilonShift, nodes []*NondeterministicNode) {
	// reversed order
	for i := len(epsilon_shifts) - 1; i >= 0; i-- {
		curr_shift := epsilon_shifts[i]
		dst_node := nodes[curr_shift.dst_node]
		src_node := nodes[curr_shift.src_node]
		for key, destination_nodes := range dst_node.next {
			if src_destination_nodes, exists := src_node.next[key]; !exists {
				src_node.next[key] = destination_nodes
			} else {
				src_node.next[key] = joinSlices(src_destination_nodes, destination_nodes)
			}
		}
		if nodes[curr_shift.dst_node].is_accept_state {
			nodes[curr_shift.src_node].is_accept_state = true
		}
	}
}

/*
Converts the NDFA into a DFA.
Resolves epsilon shifts as well.
*/
func (ndfa *NondeterministicAutomata) convertToDeterministic() *DeterministicAutomata {
	if len(ndfa.nodes) > 64 {
		// We use 64 bits to represent a unique set of ndfa nodes, which means we currently
		// do not support conversion of NDFA with more than 64 nodes
		return nil
	}
	resolveEpsilonShifts(ndfa.epsilon_shifts, ndfa.nodes)
	dfa := newCompiledPattern()

	// Scan the ndfa graph
	node_shifts := make([]NodeShift, 0)
	visited_nodes := make(map[uint64]int)
	addNodeShifts(ndfa, []int{0}, &node_shifts)

	dfa.nodes[0] = newAutomataNode()
	dfa.nodes[0].is_accept_state = getAcceptState([]int{0}, ndfa)
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

/*
Creates a Non Deterministic Automata with epsilon shifts from a pattern
(a string representing a regular expression).
*/
func CreateNDFA(pattern string) NondeterministicAutomata {
	nodes := make([]*NondeterministicNode, 0)
	nodes = append(nodes, newNondeterministicNode())
	epsilon_shifts := make([]EpsilonShift, 0)
	current_node := nodes[0]
	for i := 0; i < len(pattern); {
		current_val := pattern[i]
		next_node := newNondeterministicNode()
		nodes = append(nodes, next_node)

		if (i < len(pattern)-1) && (pattern[i+1] == '+' || pattern[i+1] == '*') {
			current_node.next[current_val] = []int{len(nodes) - 1, len(nodes) - 2}
			if pattern[i+1] == '*' {
				epsilon_shifts = append(epsilon_shifts,
					EpsilonShift{src_node: len(nodes) - 2, dst_node: len(nodes) - 1})
			}
			i += 2
		} else {
			current_node.next[current_val] = []int{len(nodes) - 1}
			i++
		}
		current_node = next_node
	}
	nodes[len(nodes)-1].is_accept_state = true
	return NondeterministicAutomata{nodes: nodes, epsilon_shifts: epsilon_shifts}
}
