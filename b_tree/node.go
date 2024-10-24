package b_tree

import "errors"

const InvalidBTreePointer BTreePointer = -1

var BTreeErrorKeyAlreadyExists error = errors.New("item's key is already in the tree")

// A pair of key/pointer values.
// The tree will index each pair according to the value of the key.
type BTreeKeyPointerPair struct {
	Pointer BTreePointer
	Key     BTreeKeyType
}

type bTreeNode struct {
	isInternal bool // internal don't point to user data but to other nodes, external nodes contain user data
	// A list of key-pointer pairs.
	// Each pointer points to the next node/user value
	// Each key represents the smallest key found underneath this child node (note this is different from a
	// standard B+ tree which contains the smallest value of the next node instead)
	nodePointers  []BTreeKeyPointerPair
	maximumDegree int
	persistency   persistencyApi
	selfPointer   BTreePointer
	nextNode      BTreePointer // Points to the next brother node
}

func initializeBTreeNodeFromBrother(node *bTreeNode) *bTreeNode {
	return initializeBTreeNode(node.maximumDegree, node.isInternal, node.persistency)
}

func (node *bTreeNode) persist() {
	pointer := node.persistency.PersistNode(node, node.selfPointer)
	if node.selfPointer != InvalidBTreePointer && node.selfPointer != pointer {
		// TODO: what if the persistency API changes the pointer? Should we support this flow?
		panic(errors.New("got different pointer during persist"))
	}

	node.selfPointer = pointer
}

func (node *bTreeNode) full() bool {
	return len(node.nodePointers) == node.maximumDegree
}

func (node *bTreeNode) halfEmpty() bool {
	return len(node.nodePointers) <= node.maximumDegree/2
}

// Splits child #childIndex of the current node.
// A new node will be created for the bigger half of the child node items.
// Returns the new pointer of the current node (it is guranteed to be changed and therefore persisted during this
// function).
func (node *bTreeNode) splitChild(childIndex int) BTreePointer {
	leftNode := node.persistency.LoadNode(node.nodePointers[childIndex].Pointer)
	rightNode := initializeBTreeNodeFromBrother(leftNode)

	// Split the leftNode in the middle, put the bigger values into the right node
	middleIndex := leftNode.maximumDegree / 2
	rightNode.nodePointers = append(rightNode.nodePointers, leftNode.nodePointers[middleIndex:]...)
	leftNode.nodePointers = leftNode.nodePointers[:middleIndex]
	rightNode.nextNode = leftNode.nextNode

	// Save the new node
	rightPointer := node.persistency.PersistNode(rightNode, InvalidBTreePointer)
	leftNode.nextNode = rightPointer

	// Add a pointer to the right child
	newValue := BTreeKeyPointerPair{Key: rightNode.nodePointers[0].Key, Pointer: rightPointer}
	// Treat this node as if it was a leaf and just insert the value "as is" into it
	node.insertNonFullLeaf(newValue)

	// Left node was changed, persist it
	leftNode.persist()
	// The node itself was changed, persist it
	node.persist()
	return node.selfPointer
}

func (node *bTreeNode) stealFromBrother(brother *bTreeNode, isPrev bool) {
	if isPrev {
		// steal the biggest item
		item := brother.nodePointers[len(brother.nodePointers)-1]
		brother.nodePointers = brother.nodePointers[:len(brother.nodePointers)-1]
		node.nodePointers = append([]BTreeKeyPointerPair{item}, node.nodePointers...)
	} else {
		// steal the smallest item
		item := brother.nodePointers[0]
		brother.nodePointers = brother.nodePointers[1:]
		node.nodePointers = append(node.nodePointers, item)
	}

	node.persist()
	brother.persist()
}

// Increases the amount of nodes in a child node by 1, either by an item from his brother, or by merging it
// with one of his brother/this node
func (node *bTreeNode) fill(childIndex int) {
	innerNodePointer := node.nodePointers[childIndex]
	innerNode := node.persistency.LoadNode(innerNodePointer.Pointer)

	var leftNode *bTreeNode = nil
	var rightNode *bTreeNode = nil

	if childIndex > 0 {
		leftNode = node.persistency.LoadNode(node.nodePointers[childIndex-1].Pointer)
	}
	if childIndex < len(node.nodePointers)-1 {
		rightNode = node.persistency.LoadNode(node.nodePointers[childIndex+1].Pointer)
	}

	if leftNode != nil && !leftNode.halfEmpty() {
		innerNode.stealFromBrother(leftNode, true)
		node.nodePointers[childIndex].Key = innerNode.nodePointers[0].Key
		return
	}

	if rightNode != nil && !rightNode.halfEmpty() {
		innerNode.stealFromBrother(rightNode, false)
		node.nodePointers[childIndex+1].Key = rightNode.nodePointers[0].Key
		return
	}

	// failed to steal item from brothers, merge instead
	var mergedNode *bTreeNode = nil
	var nodeToDelete *bTreeNode = nil
	var deletionPointer BTreeKeyPointerPair
	if leftNode != nil {
		mergedNode = leftNode
		nodeToDelete = innerNode
		deletionPointer = innerNodePointer
	} else {
		// we know that rightNode != nil, since each node!=root has at least one brother
		mergedNode = innerNode
		nodeToDelete = rightNode
		deletionPointer = node.nodePointers[childIndex+1]
	}

	mergedNode.nodePointers = append(mergedNode.nodePointers, nodeToDelete.nodePointers...)
	mergedNode.nextNode = nodeToDelete.nextNode
	mergedNode.persist()
	node.removeFromLeaf(deletionPointer.Key)
	node.persistency.RemoveNode(nodeToDelete.selfPointer)
	if len(node.nodePointers) == 1 {
		// notice this can only occur at the root, as we guaranteed all nodes except for the root
		// are not half empty.

		// make the pointer of this node point to the merged node instead
		mergedNode.selfPointer = node.selfPointer
		// delete self from persistence
		node.persistency.RemoveNode(node.selfPointer)
		*node = *mergedNode
	}
}

func (node *bTreeNode) findMatchingNodeIndex(key BTreeKeyType) int {
	// If the key is smaller than everything in this node, return the smallest child node
	if key <= node.nodePointers[0].Key {
		return 0
	}

	for i := 1; i < len(node.nodePointers); i++ {
		innerNodePointer := node.nodePointers[i]
		if key < innerNodePointer.Key {
			// everthing in the current node is bigger than key, return the previous node
			return i - 1
		}
	}

	// key is bigger than the values in all node but the rightside one
	return len(node.nodePointers) - 1
}

func (node *bTreeNode) findMatchingNode(key BTreeKeyType) (*bTreeNode, int) {
	innerNodeIndex := node.findMatchingNodeIndex(key)
	innerNodePointer := node.nodePointers[innerNodeIndex]
	return node.persistency.LoadNode(innerNodePointer.Pointer), innerNodeIndex
}

func (node *bTreeNode) findMatchingNodeInsert(item BTreeKeyPointerPair) *bTreeNode {
	innerNode, innerNodeIndex := node.findMatchingNode(item.Key)
	if innerNode.full() {
		node.splitChild(innerNodeIndex)

		// the inner node shouldn't be full this time
		innerNode, _ = node.findMatchingNode(item.Key)
	}

	return innerNode
}

func (node *bTreeNode) findMatchingNodeDelete(key BTreeKeyType) (*bTreeNode, int) {
	innerNode, innerNodeIndex := node.findMatchingNode(key)
	if innerNode.halfEmpty() {
		node.fill(innerNodeIndex)
		node.persist()

		// the inner node shouldn't be half-empty this time
		innerNode, innerNodeIndex = node.findMatchingNode(key)
	}

	return innerNode, innerNodeIndex
}

func (node *bTreeNode) insertNonFullInternal(item BTreeKeyPointerPair) error {
	innerNode := node.findMatchingNodeInsert(item)
	err := innerNode.insertNonFull(item)
	if err != nil {
		return err
	}

	// New item changes the minimal value of this nodes' children, reflect that in node pointers
	if item.Key < node.nodePointers[0].Key {
		node.nodePointers[0].Key = item.Key
		node.persist()
	}

	return nil
}

func (node *bTreeNode) insertNonFullLeaf(item BTreeKeyPointerPair) error {
	// allocate space for one additional element
	node.nodePointers = append(node.nodePointers, node.nodePointers[len(node.nodePointers)-1])

	i := len(node.nodePointers) - 1
	// move all keys bigger than the new item one location forward
	for ; i > 0; i-- {
		if item.Key == node.nodePointers[i-1].Key {
			return BTreeErrorKeyAlreadyExists
		}

		if item.Key >= node.nodePointers[i-1].Key {
			break // found the location for the new item
		}

		// move item one cell forward
		node.nodePointers[i] = node.nodePointers[i-1]
	}

	node.nodePointers[i] = item
	// node was changed, persist it
	node.persist()

	return nil
}

func (node *bTreeNode) insertNonFull(item BTreeKeyPointerPair) error {
	if node.isInternal {
		return node.insertNonFullInternal(item)
	} else {
		return node.insertNonFullLeaf(item)
	}
}

// returns whether or not the item was removed
func (node *bTreeNode) removeFromLeaf(key BTreeKeyType) bool {
	index := -1
	for i := 0; i < len(node.nodePointers); i++ {
		if node.nodePointers[i].Key == key {
			index = i
			break
		}
	}

	if index == -1 {
		return false
	}

	node.nodePointers = append(node.nodePointers[:index], node.nodePointers[index+1:]...)
	return true
}

func (node *bTreeNode) remove(key BTreeKeyType) bool {
	removed := false
	if node.isInternal {
		innerNode, index := node.findMatchingNodeDelete(key)
		removed = innerNode.remove(key)

		// if the inner node's smallest key has changed, update it in the parent
		if removed && node.nodePointers[index].Key == key {
			node.nodePointers[index].Key = innerNode.nodePointers[0].Key
			node.persist()
		}
	} else {
		removed = node.removeFromLeaf(key)
		if removed && len(node.nodePointers) == 0 {
			// This can only happen for the root node, update accordingly
			node.persistency.RemoveNode(node.selfPointer)
			node.selfPointer = InvalidBTreePointer
		} else {
			node.persist()
		}
	}

	return removed
}
