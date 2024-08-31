package b_tree

type BTreeIterator struct {
	tree         *BTree
	currentNode  *bTreeNode
	offsetInNode int
}

func (iterator *BTreeIterator) advanceIfNeeded() bool {
	if iterator.offsetInNode < len(iterator.currentNode.nodePointers) {
		return true
	}

	pointer := iterator.currentNode.nextNode
	if pointer == InvalidBTreePointer {
		// no more nodes
		return false
	}

	iterator.currentNode = iterator.currentNode.persistency.LoadNode(pointer)
	iterator.offsetInNode = 0
	return true
}

func (iterator *BTreeIterator) Next() *BTreeKeyPointerPair {
	if !iterator.advanceIfNeeded() {
		return nil // no more entries
	}

	pair := &iterator.currentNode.nodePointers[iterator.offsetInNode]

	// advance offset
	iterator.offsetInNode += 1

	return pair
}
