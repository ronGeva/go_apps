package b_tree

import "sync/atomic"

type bTreeValueType int
type bTreePointer int64
type bTreeKeyType int
type nodeRetrievalFunc func(bTreePointer) *bTreeNode
type userValueRetrievalFunc func(bTreePointer) bTreeValueType
type allocateNodeFunc func() (bTreePointer, *bTreeNode)

const invalidBTreePointer bTreePointer = -1

type bTreeKeyPointerPair struct {
	pointer bTreePointer
	key     bTreeKeyType
}

type bTreeNode struct {
	isInternal    bool // internal don't point to user data but to other nodes, external nodes contain user data
	nodePointers  []bTreeKeyPointerPair
	maximumDegree int
	getNode       nodeRetrievalFunc
	allocateNode  allocateNodeFunc
}

// Assume the underlying value is bTreeKeyType
type BTree struct {
	root *bTreeNode
	// The following members represent pointers to structures in "memory"
	getNode       nodeRetrievalFunc
	getUserValue  userValueRetrievalFunc
	minimumDegree int
}

type BTreeIterator struct {
	tree         *BTree
	currentNode  *bTreeNode
	offsetInNode int
}

var globalCounter int32 = 0

func InitializeBTree() (*BTree, error) {
	nodesInMemory := make(map[bTreePointer]*bTreeNode)
	userValuesInMemory := make(map[bTreePointer]bTreeValueType)
	getNode := func(pointer bTreePointer) *bTreeNode {
		return nodesInMemory[pointer]
	}

	getUserValue := func(pointer bTreePointer) bTreeValueType {
		return userValuesInMemory[pointer]
	}

	allocateNode := func() (bTreePointer, *bTreeNode) {
		pointer := bTreePointer(atomic.AddInt32(&globalCounter, 1))
		node := &bTreeNode{}
		nodesInMemory[pointer] = node

		return pointer, node
	}

	root := &bTreeNode{isInternal: false, nodePointers: make([]bTreeKeyPointerPair, 0), allocateNode: allocateNode, maximumDegree: 3}
	return &BTree{root: root, getNode: getNode, getUserValue: getUserValue, minimumDegree: 3}, nil
}

func initializeBTreeNodeFromBrother(node *bTreeNode) (bTreePointer, *bTreeNode) {
	pointer, newNode := node.allocateNode()
	newNode.isInternal = node.isInternal
	newNode.nodePointers = make([]bTreeKeyPointerPair, node.maximumDegree)
	newNode.maximumDegree = node.maximumDegree
	newNode.allocateNode = node.allocateNode

	return pointer, newNode
}

func (node *bTreeNode) full() bool {
	return len(node.nodePointers) == node.maximumDegree
}

// Splits child #childIndex of the current node.
// A new node will be created for the bigger half of the child node items.
func (node *bTreeNode) splitChild(childIndex int) {
	leftNode := node.getNode(node.nodePointers[childIndex].pointer)
	rightPointer, rightNode := initializeBTreeNodeFromBrother(leftNode)

	// Split the leftNode in the middle, put the bigger values into the right node
	middleIndex := leftNode.maximumDegree
	rightNode.nodePointers = append(rightNode.nodePointers, leftNode.nodePointers[middleIndex:]...)
	leftNode.nodePointers = leftNode.nodePointers[:middleIndex]

	// Add a pointer to the right child
	newValue := bTreeKeyPointerPair{key: rightNode.nodePointers[0].key, pointer: rightPointer}
	// Treat this node as if it was a leaf and just insert the value "as is" into it
	node.insertNonFullLeaf(newValue)
}

func (node *bTreeNode) insertNonFullInternal(item bTreeKeyPointerPair) {
	// This is an internal node, continue traveling to the appropriate leaf node
	for i := 0; i < len(node.nodePointers); i++ {
		innerNodePointer := node.nodePointers[i]
		if item.key >= innerNodePointer.key {
			continue
		}

		innerNode := node.getNode(innerNodePointer.pointer)
		if innerNode.full() {
			node.splitChild(i)
			// Check if the newly created node should contain the new item
			if item.key >= innerNodePointer.key {
				innerNode = node.getNode(node.nodePointers[i+1].pointer)
			}
		}
		innerNode.insertNonFull(item)
	}
}

func (node *bTreeNode) insertNonFullLeaf(item bTreeKeyPointerPair) {
	if len(node.nodePointers) == 0 {
		// handle the edge case of an empty node
		node.nodePointers = []bTreeKeyPointerPair{item}
		return
	}

	// allocate space for one additional element
	node.nodePointers = append(node.nodePointers, node.nodePointers[len(node.nodePointers)-1])

	// move all keys bigger than the new item one location forward
	for i := len(node.nodePointers) - 1; i > 1; i-- {
		if item.key < node.nodePointers[i].key {
			// move item one cell forward
			node.nodePointers[i] = node.nodePointers[i-1]
		} else {
			// insert new item, then leave
			node.nodePointers[i] = item
			break
		}
	}
}

func (node *bTreeNode) insertNonFull(item bTreeKeyPointerPair) {
	if node.isInternal {
		node.insertNonFullInternal(item)
	} else {
		node.insertNonFullLeaf(item)
	}
}

func (tree *BTree) Insert(item bTreeKeyPointerPair) error {
	if tree.root.full() {
		// root has reached maximum size, we need to split the root, thus adding another level
		pointer, newRoot := initializeBTreeNodeFromBrother(tree.root)
		oldRootPointer := bTreeKeyPointerPair{key: tree.root.nodePointers[0].key, pointer: pointer}
		newRoot.nodePointers = append(newRoot.nodePointers, oldRootPointer)

		// newRoot now points to root, set newRoot as the root of the tree
		tree.root = newRoot

		// now split the old root from the perspective of the new one
		newRoot.splitChild(0)
	}

	// root is not full, insert the new item
	tree.root.insertNonFull(item)
	return nil
}

func (tree *BTree) Delete(item bTreeKeyPointerPair) error {

	return nil
}

func (tree *BTree) Iterator() *BTreeIterator {
	currentNode := tree.root
	for !currentNode.isInternal {
		currentNode = tree.getNode(currentNode.nodePointers[0].pointer)
	}
	// reached an internal node
	return &BTreeIterator{currentNode: currentNode, offsetInNode: 0, tree: tree}
}

func (iterator *BTreeIterator) advanceIfNeeded() bool {
	if iterator.offsetInNode < len(iterator.currentNode.nodePointers) {
		return true
	}

	pointer := iterator.currentNode.nodePointers[iterator.offsetInNode-1].pointer
	if pointer == invalidBTreePointer {
		return false // no more entries
	}

	iterator.currentNode = iterator.tree.getNode(pointer)
	iterator.offsetInNode = 0
	return true
}

func (iterator *BTreeIterator) Next() *bTreeKeyPointerPair {
	if !iterator.advanceIfNeeded() {
		return nil // no more entries
	}

	pair := &iterator.currentNode.nodePointers[iterator.offsetInNode]

	// advance offset
	iterator.offsetInNode += 1

	return pair
}
