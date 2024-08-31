package b_tree

import (
	"errors"
)

type bTreePointer int64
type bTreeKeyType int

var BTreeErrorNotFound error = errors.New("item not found")

// Assume the underlying value is bTreeKeyType
type BTree struct {
	rootPointer bTreePointer
	// The following members represent pointers to structures in "memory"
	persistency   persistencyApi
	minimumDegree int
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
