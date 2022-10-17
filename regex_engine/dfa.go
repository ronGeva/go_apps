// This module represnts a determinstic finite automata
package regex_engine

type AutomataNode struct {
	next            map[byte]*AutomataNode
	is_accept_state bool
}

func newAutomataNode() *AutomataNode {
	node := AutomataNode{}
	node.next = make(map[byte]*AutomataNode)
	node.is_accept_state = false
	return &node
}

type CompiledPattern struct {
	start *AutomataNode
}

func CompilePattern(pattern string) CompiledPattern {
	compiled_pattern := CompiledPattern{newAutomataNode()}
	current_node := compiled_pattern.start

	for i := 0; i < len(pattern); i++ {
		tmp := newAutomataNode()
		current_node.next[pattern[i]] = tmp
		current_node = tmp
	}
	current_node.is_accept_state = true // mark the final state as an accepting state

	return compiled_pattern
}

func (pattern *CompiledPattern) Match(data string) bool {
	currentNode := pattern.start
	for i := 0; i < len(data); i++ {
		currentNode = currentNode.next[data[i]]
		if currentNode == nil {
			return false
		}
	}
	return currentNode.is_accept_state
}
