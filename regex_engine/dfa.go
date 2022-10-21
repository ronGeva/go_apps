// This module represnts a determinstic finite automata
package regex_engine

type AutomataNode struct {
	next            map[byte]int
	is_accept_state bool
}

func newAutomataNode() *AutomataNode {
	node := AutomataNode{}
	node.next = make(map[byte]int)
	node.is_accept_state = false
	return &node
}

type CompiledPattern struct {
	nodes []*AutomataNode
}

func newCompiledPattern() *CompiledPattern {
	nodes := make([]*AutomataNode, 1)
	nodes[0] = newAutomataNode()
	return &CompiledPattern{nodes: nodes}
}

/*
Currently supports:
{<plain characters>, '.', '+', '*'}
*/
func CompilePattern(pattern string) *CompiledPattern {
	ndfa := CreateNDFA(pattern)
	return ndfa.convertToDeterministic()
}

func (pattern *CompiledPattern) Match(data string) bool {
	currentNode := pattern.nodes[0]
	for i := 0; i < len(data); i++ {
		if nextNodeIndex, exists := currentNode.next[data[i]]; exists {
			currentNode = pattern.nodes[nextNodeIndex]
		} else {
			if nextNodeIndex, exists := currentNode.next['.']; exists {
				currentNode = pattern.nodes[nextNodeIndex]
			} else {
				return false
			}
		}
	}
	return currentNode.is_accept_state
}
