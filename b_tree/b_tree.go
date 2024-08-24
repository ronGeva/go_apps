package b_tree

import (
	"errors"
	"sync/atomic"
)

type bTreeValueType int
type bTreePointer int64
type bTreeKeyType int
type nodeRetrievalFunc func(bTreePointer) *bTreeNode
type userValueRetrievalFunc func(bTreePointer) bTreeValueType
type allocateNodeFunc func() (bTreePointer, *bTreeNode)

var globalCounter int32 = 0

type PersistencyApi interface {
	// loads a node from a bTreePointer that was previously received from PersistNode
	LoadNode(bTreePointer) *bTreeNode

	// Persists a node into the persistent storage
	// This function receives the optional parameter of the original pointer to the node,
	// and returns the new points to it (that can be used to load it again in the future)
	PersistNode(*bTreeNode, bTreePointer) bTreePointer
}

type InMemoryPersistency struct {
	nodesInMemory map[bTreePointer]*bTreeNode
	counter       int32
}

func (api *InMemoryPersistency) LoadNode(pointer bTreePointer) *bTreeNode {
	if pointer == invalidBTreePointer {
		return nil
	}

	return api.nodesInMemory[pointer]
}

func (api *InMemoryPersistency) PersistNode(node *bTreeNode, pointer bTreePointer) bTreePointer {
	if pointer != invalidBTreePointer {
		api.nodesInMemory[pointer] = node
		return pointer
	}

	newPointer := bTreePointer(atomic.AddInt32(&api.counter, 1))

	api.nodesInMemory[newPointer] = node
	return newPointer
}

const invalidBTreePointer bTreePointer = -1

// Each pointer-value pair indicate a node with a values starting with that value
type BTreeKeyPointerPair struct {
	pointer bTreePointer
	key     bTreeKeyType
}

type bTreeNode struct {
	isInternal    bool // internal don't point to user data but to other nodes, external nodes contain user data
	nodePointers  []BTreeKeyPointerPair
	maximumDegree int
	persistency   PersistencyApi
	pointer       bTreePointer
}

// Assume the underlying value is bTreeKeyType
type BTree struct {
	rootPointer bTreePointer
	// The following members represent pointers to structures in "memory"
	persistency   PersistencyApi
	getUserValue  userValueRetrievalFunc
	minimumDegree int
}

type BTreeIterator struct {
	tree         *BTree
	currentNode  *bTreeNode
	offsetInNode int
}

func InitializeBTree() (*BTree, error) {
	inMemoryPersistency := InMemoryPersistency{nodesInMemory: map[bTreePointer]*bTreeNode{}, counter: 0}

	//root := &bTreeNode{isInternal: false, nodePointers: make([]bTreeKeyPointerPair, 0), persistency: &inMemoryPersistency, maximumDegree: 3}
	return &BTree{rootPointer: invalidBTreePointer, persistency: &inMemoryPersistency, minimumDegree: 3}, nil
}

func initializeBTreeNodeFromBrother(node *bTreeNode) *bTreeNode {
	newNode := &bTreeNode{}
	newNode.isInternal = node.isInternal
	newNode.nodePointers = make([]BTreeKeyPointerPair, 0)
	newNode.maximumDegree = node.maximumDegree
	newNode.persistency = node.persistency
	newNode.pointer = invalidBTreePointer

	return newNode
}

func (node *bTreeNode) persist() {
	pointer := node.persistency.PersistNode(node, node.pointer)
	if node.pointer != invalidBTreePointer && node.pointer != pointer {
		// TODO: what if the persistency API changes the pointer? Should we support this flow?
		panic(errors.New("got different pointer during persist"))
	}

	node.pointer = pointer
}

func (node *bTreeNode) full() bool {
	return len(node.nodePointers) == node.maximumDegree
}

// Splits child #childIndex of the current node.
// A new node will be created for the bigger half of the child node items.
// Returns the new pointer of the current node (it is guranteed to be changed and therefore persisted during this
// function).
func (node *bTreeNode) splitChild(childIndex int) bTreePointer {
	leftNode := node.persistency.LoadNode(node.nodePointers[childIndex].pointer)
	rightNode := initializeBTreeNodeFromBrother(leftNode)

	// Split the leftNode in the middle, put the bigger values into the right node
	middleIndex := leftNode.maximumDegree / 2
	rightNode.nodePointers = append(rightNode.nodePointers, leftNode.nodePointers[middleIndex:]...)
	leftNode.nodePointers = leftNode.nodePointers[:middleIndex]

	// Save the new node
	rightPointer := node.persistency.PersistNode(rightNode, invalidBTreePointer)

	// Add a pointer to the right child
	newValue := BTreeKeyPointerPair{key: rightNode.nodePointers[0].key, pointer: rightPointer}
	// Treat this node as if it was a leaf and just insert the value "as is" into it
	node.insertNonFullLeaf(newValue)

	// Left node was changed, persist it
	leftNode.persist()
	// The node itself was changed, persist it
	node.persist()
	return node.pointer
}

func (node *bTreeNode) findMatchingNode(item BTreeKeyPointerPair) int {
	// If the key is smaller than everything in this node, return the smallest child node
	if item.key <= node.nodePointers[0].key {
		return 0
	}

	for i := 1; i < len(node.nodePointers); i++ {
		innerNodePointer := node.nodePointers[i]
		if item.key < innerNodePointer.key {
			// everthing in the current node is bigger than key, return the previous node
			return i - 1
		}
	}

	// key is bigger than the values in all node but the rightside one
	return len(node.nodePointers) - 1
}

func (node *bTreeNode) insertNonFullInternal(item BTreeKeyPointerPair) {
	innerNodeIndex := node.findMatchingNode(item)
	innerNodePointer := node.nodePointers[innerNodeIndex]
	innerNode := node.persistency.LoadNode(innerNodePointer.pointer)
	if innerNode.full() {
		node.splitChild(innerNodeIndex)
		// Check if the newly created node should contain the new item
		if item.key >= innerNodePointer.key {
			innerNode = node.persistency.LoadNode(node.nodePointers[innerNodeIndex+1].pointer)
		}
	}
	innerNode.insertNonFull(item)
}

func (node *bTreeNode) insertNonFullLeaf(item BTreeKeyPointerPair) {
	// allocate space for one additional element
	node.nodePointers = append(node.nodePointers, node.nodePointers[len(node.nodePointers)-1])

	i := len(node.nodePointers) - 1
	// move all keys bigger than the new item one location forward
	for ; i > 0; i-- {
		if item.key >= node.nodePointers[i].key {
			break // found the location for the new item
		}

		// move item one cell forward
		node.nodePointers[i] = node.nodePointers[i-1]
	}

	node.nodePointers[i] = item
	// node was changed, persist it
	node.persist()
}

func (node *bTreeNode) insertNonFull(item BTreeKeyPointerPair) {
	if node.isInternal {
		node.insertNonFullInternal(item)
	} else {
		node.insertNonFullLeaf(item)
	}
}

func (tree *BTree) Insert(item BTreeKeyPointerPair) error {
	if tree.rootPointer == invalidBTreePointer {
		// tree is empty, allocate root
		root := &bTreeNode{isInternal: false, nodePointers: make([]BTreeKeyPointerPair, 0), persistency: tree.persistency, maximumDegree: 5}
		root.nodePointers = append(root.nodePointers, item)
		tree.rootPointer = tree.persistency.PersistNode(root, invalidBTreePointer)
		return nil
	}

	root := tree.persistency.LoadNode(tree.rootPointer)
	if root.full() {
		// root has reached maximum size, we need to split the root, thus adding another level
		newRoot := initializeBTreeNodeFromBrother(root)
		// new root isn't a leaf anymore, it is internal
		newRoot.isInternal = true
		oldRootPointer := BTreeKeyPointerPair{key: root.nodePointers[0].key, pointer: tree.rootPointer}
		newRoot.nodePointers = append(newRoot.nodePointers, oldRootPointer)

		// now split the old root from the perspective of the new one
		newRootPointer := newRoot.splitChild(0)
		// save the new root pointer in the tree
		tree.rootPointer = newRootPointer
		root = newRoot
	}

	// root is not full, insert the new item
	root.insertNonFull(item)
	return nil
}

func (tree *BTree) Delete(item BTreeKeyPointerPair) error {

	return nil
}

func (tree *BTree) Iterator() *BTreeIterator {
	currentNode := tree.persistency.LoadNode(tree.rootPointer)

	// traverse the leftside of the tree until we've reached a leaf node
	for !currentNode.isInternal {
		currentNode = tree.persistency.LoadNode(currentNode.nodePointers[0].pointer)
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

	iterator.currentNode = iterator.tree.persistency.LoadNode(pointer)
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