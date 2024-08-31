package b_tree

import (
	"errors"
)

type bTreeValueType int
type bTreePointer int64
type bTreeKeyType int
type nodeRetrievalFunc func(bTreePointer) *bTreeNode
type userValueRetrievalFunc func(bTreePointer) bTreeValueType
type allocateNodeFunc func() (bTreePointer, *bTreeNode)

var BTreeErrorNotFound error = errors.New("item not found")

const invalidBTreePointer bTreePointer = -1

// Each pointer-value pair indicate a node with a values starting with that value
type BTreeKeyPointerPair struct {
	pointer bTreePointer
	key     bTreeKeyType
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
	selfPointer   bTreePointer
	nextNode      bTreePointer // Points to the next brother node
}

// Assume the underlying value is bTreeKeyType
type BTree struct {
	rootPointer bTreePointer
	// The following members represent pointers to structures in "memory"
	persistency   persistencyApi
	getUserValue  userValueRetrievalFunc
	minimumDegree int
}

type BTreeIterator struct {
	tree         *BTree
	currentNode  *bTreeNode
	offsetInNode int
}

func InitializeBTree() (*BTree, error) {
	inMemoryPersistency := InitializeInMemoryPersistency()
	persistency := persistencyApi{store: inMemoryPersistency}

	return &BTree{rootPointer: invalidBTreePointer, persistency: persistency, minimumDegree: 3}, nil
}

func initializeBTreeNode(maximumDegree int, isInternal bool, persistency persistencyApi) *bTreeNode {
	newNode := &bTreeNode{}
	newNode.isInternal = isInternal
	newNode.nodePointers = make([]BTreeKeyPointerPair, 0)
	newNode.maximumDegree = maximumDegree
	newNode.persistency = persistency
	newNode.selfPointer = invalidBTreePointer
	newNode.nextNode = invalidBTreePointer

	return newNode
}

func initializeBTreeNodeFromBrother(node *bTreeNode) *bTreeNode {
	return initializeBTreeNode(node.maximumDegree, node.isInternal, node.persistency)
}

func (node *bTreeNode) persist() {
	pointer := node.persistency.PersistNode(node, node.selfPointer)
	if node.selfPointer != invalidBTreePointer && node.selfPointer != pointer {
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
func (node *bTreeNode) splitChild(childIndex int) bTreePointer {
	leftNode := node.persistency.LoadNode(node.nodePointers[childIndex].pointer)
	rightNode := initializeBTreeNodeFromBrother(leftNode)

	// Split the leftNode in the middle, put the bigger values into the right node
	middleIndex := leftNode.maximumDegree / 2
	rightNode.nodePointers = append(rightNode.nodePointers, leftNode.nodePointers[middleIndex:]...)
	leftNode.nodePointers = leftNode.nodePointers[:middleIndex]
	rightNode.nextNode = leftNode.nextNode

	// Save the new node
	rightPointer := node.persistency.PersistNode(rightNode, invalidBTreePointer)
	leftNode.nextNode = rightPointer

	// Add a pointer to the right child
	newValue := BTreeKeyPointerPair{key: rightNode.nodePointers[0].key, pointer: rightPointer}
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
	innerNode := node.persistency.LoadNode(innerNodePointer.pointer)

	var leftNode *bTreeNode = nil
	var rightNode *bTreeNode = nil

	if childIndex > 0 {
		leftNode = node.persistency.LoadNode(node.nodePointers[childIndex-1].pointer)
	}
	if childIndex < len(node.nodePointers)-1 {
		rightNode = node.persistency.LoadNode(node.nodePointers[childIndex+1].pointer)
	}

	if leftNode != nil && !leftNode.halfEmpty() {
		innerNode.stealFromBrother(leftNode, true)
		node.nodePointers[childIndex].key = innerNode.nodePointers[0].key
		return
	}

	if rightNode != nil && !rightNode.halfEmpty() {
		innerNode.stealFromBrother(rightNode, false)
		node.nodePointers[childIndex+1].key = rightNode.nodePointers[0].key
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
	node.removeFromLeaf(deletionPointer)
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

func (node *bTreeNode) findMatchingNodeIndex(item BTreeKeyPointerPair) int {
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

func (node *bTreeNode) findMatchingNode(item BTreeKeyPointerPair) (*bTreeNode, int) {
	innerNodeIndex := node.findMatchingNodeIndex(item)
	innerNodePointer := node.nodePointers[innerNodeIndex]
	return node.persistency.LoadNode(innerNodePointer.pointer), innerNodeIndex
}

func (node *bTreeNode) findMatchingNodeInsert(item BTreeKeyPointerPair) *bTreeNode {
	innerNode, innerNodeIndex := node.findMatchingNode(item)
	if innerNode.full() {
		node.splitChild(innerNodeIndex)

		// the inner node shouldn't be full this time
		innerNode, _ = node.findMatchingNode(item)
	}

	return innerNode
}

func (node *bTreeNode) findMatchingNodeDelete(item BTreeKeyPointerPair) (*bTreeNode, int) {
	innerNode, innerNodeIndex := node.findMatchingNode(item)
	if innerNode.halfEmpty() {
		node.fill(innerNodeIndex)
		node.persist()

		// the inner node shouldn't be half-empty this time
		innerNode, innerNodeIndex = node.findMatchingNode(item)
	}

	return innerNode, innerNodeIndex
}

func (node *bTreeNode) insertNonFullInternal(item BTreeKeyPointerPair) {
	innerNode := node.findMatchingNodeInsert(item)
	innerNode.insertNonFull(item)

	// New item changes the minimal value of this nodes' children, reflect that in node pointers
	if item.key < node.nodePointers[0].key {
		node.nodePointers[0].key = item.key
		node.persist()
	}
}

func (node *bTreeNode) insertNonFullLeaf(item BTreeKeyPointerPair) {
	// allocate space for one additional element
	node.nodePointers = append(node.nodePointers, node.nodePointers[len(node.nodePointers)-1])

	i := len(node.nodePointers) - 1
	// move all keys bigger than the new item one location forward
	for ; i > 0; i-- {
		if item.key >= node.nodePointers[i-1].key {
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

// returns whether or not the item was removed
func (node *bTreeNode) removeFromLeaf(item BTreeKeyPointerPair) bool {
	index := -1
	for i := 0; i < len(node.nodePointers); i++ {
		if node.nodePointers[i] == item {
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

func (node *bTreeNode) remove(item BTreeKeyPointerPair) bool {
	removed := false
	if node.isInternal {
		innerNode, index := node.findMatchingNodeDelete(item)
		removed = innerNode.remove(item)

		// if the inner node's smallest key has changed, update it in the parent
		if removed && node.nodePointers[index].key == item.key {
			node.nodePointers[index].key = innerNode.nodePointers[0].key
			node.persist()
		}
	} else {
		removed = node.removeFromLeaf(item)
		if removed && len(node.nodePointers) == 0 {
			// This can only happen for the root node, update accordingly
			node.persistency.RemoveNode(node.selfPointer)
			node.selfPointer = invalidBTreePointer
		} else {
			node.persist()
		}
	}

	return removed
}

func (tree *BTree) Insert(item BTreeKeyPointerPair) error {
	if tree.rootPointer == invalidBTreePointer {
		// tree is empty, allocate root
		root := initializeBTreeNode(5, false, tree.persistency)
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
	// Algorithm:
	// Traverse the tree to find the leaf node in which the item resides.
	// At every node on the way (except for the root), if the if len(children)< maxDegree/2 then fill the node.
	// If the item isn't found, return an error.
	// If the item is found, remove it from the leaf, then check if len(children) < maxDegree/2, and if so, fill the
	// node.

	// Filling a node is done using this algorithm:
	// If the prev brother node or the next brother node has more children than maxDegree/2, borrow one value from them.
	// Otherwise, merge the node with on of its brothers.
	// If a merge was done and the new node has 0 brothers, replace its father node with it and remove it.

	if tree.rootPointer == invalidBTreePointer {
		return BTreeErrorNotFound
	}

	root := tree.persistency.LoadNode(tree.rootPointer)

	removed := root.remove(item)
	if !removed {
		return BTreeErrorNotFound
	}
	// update the root pointer, just in case something has changed
	tree.rootPointer = root.selfPointer
	return nil
}

func (tree *BTree) Iterator() *BTreeIterator {
	currentNode := tree.persistency.LoadNode(tree.rootPointer)

	// traverse the leftside of the tree until we've reached a leaf node
	for currentNode.isInternal {
		currentNode = tree.persistency.LoadNode(currentNode.nodePointers[0].pointer)
	}
	// reached an internal node
	return &BTreeIterator{currentNode: currentNode, offsetInNode: 0, tree: tree}
}

func (iterator *BTreeIterator) advanceIfNeeded() bool {
	if iterator.offsetInNode < len(iterator.currentNode.nodePointers) {
		return true
	}

	pointer := iterator.currentNode.nextNode
	if pointer == invalidBTreePointer {
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
