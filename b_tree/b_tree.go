package b_tree

import (
	"encoding/binary"
	"errors"
)

// Externally supplied BTreePointer values should always be positive.
// Negative BTreePointer values are used internally by the b_tree module and should
// never be used by external code (such as the persistency interface)
type BTreePointer int64
type BTreeKeyType int

var BTreeErrorNotFound error = errors.New("item not found")

type BTree struct {
	rootPointer   BTreePointer
	persistency   persistencyApi
	minimumDegree int
}

func deserializeBTree(data []byte, persistency persistencyApi) *BTree {
	root := binary.LittleEndian.Uint64(data[:8])
	return &BTree{rootPointer: BTreePointer(root), persistency: persistency}
}

func serializeBTree(tree *BTree) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data[:8], uint64(tree.rootPointer))
	return data
}

// Initializes a new empty B+ Tree.
// @param store: an object allowing the tree to save/load data into persistent storage.
func InitializeBTree(store PersistencyInterface) (*BTree, error) {
	persistency := persistencyApi{store: store}
	rootData, err := store.Load(store.RootPointer())
	if err == BTreeNotInitialized {
		tree := BTree{rootPointer: InvalidBTreePointer, persistency: persistency, minimumDegree: 3}
		persistency.PersistTree(&tree)
		return &tree, nil
	}
	if err != nil {
		return nil, err
	}

	return deserializeBTree(rootData, persistency), nil
}

func initializeBTreeNode(maximumDegree int, isInternal bool, persistency persistencyApi) *bTreeNode {
	newNode := &bTreeNode{}
	newNode.isInternal = isInternal
	newNode.nodePointers = make([]BTreeKeyPointerPair, 0)
	newNode.maximumDegree = maximumDegree
	newNode.persistency = persistency
	newNode.selfPointer = InvalidBTreePointer
	newNode.nextNode = InvalidBTreePointer

	return newNode
}

func (tree *BTree) updateRootPointer(pointer BTreePointer) {
	tree.rootPointer = pointer
	tree.persistency.PersistTree(tree)
}

// Inserts a key-value pair into the tree.
// This new item will be indexes according to its key.
// Entering a pair with a key that is alreadyd in the tree will fail and return BTreeErrorKeyAlreadyExists.
func (tree *BTree) Insert(item BTreeKeyPointerPair) error {
	if tree.rootPointer == InvalidBTreePointer {
		// tree is empty, allocate root.
		// this flow cannot fail - the tree is empty so there's no chance of duplication
		root := initializeBTreeNode(5, false, tree.persistency)
		root.nodePointers = append(root.nodePointers, item)
		root.persist()
		tree.updateRootPointer(root.selfPointer)
		return nil
	}

	root := tree.persistency.LoadNode(tree.rootPointer)
	if root.full() {
		// root has reached maximum size, we need to split the root, thus adding another level
		newRoot := initializeBTreeNodeFromBrother(root)
		// new root isn't a leaf anymore, it is internal
		newRoot.isInternal = true
		oldRootPointer := BTreeKeyPointerPair{Key: root.nodePointers[0].Key, Pointer: tree.rootPointer}
		newRoot.nodePointers = append(newRoot.nodePointers, oldRootPointer)

		// now split the old root from the perspective of the new one
		newRootPointer := newRoot.splitChild(0)
		// save the new root pointer in the tree
		tree.updateRootPointer(newRootPointer)
		root = newRoot
	}

	// root is not full, insert the new item
	return root.insertNonFull(item)
}

// Deletes a pair from the tree which has this key.
// If the key is not found, return BTreeErrorNotFound.
func (tree *BTree) Delete(key BTreeKeyType) error {
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

	if tree.rootPointer == InvalidBTreePointer {
		return BTreeErrorNotFound
	}

	root := tree.persistency.LoadNode(tree.rootPointer)

	removed := root.remove(key)
	if !removed {
		return BTreeErrorNotFound
	}
	// update the root pointer, just in case something has changed
	tree.rootPointer = root.selfPointer
	return nil
}

// Returns an iterator which allows iterating over all of the tree pairs, in ascending
// key order.
func (tree *BTree) Iterator() *BTreeIterator {
	if tree.rootPointer == InvalidBTreePointer {
		return nil
	}

	currentNode := tree.persistency.LoadNode(tree.rootPointer)

	// traverse the leftside of the tree until we've reached a leaf node
	for currentNode.isInternal {
		currentNode = tree.persistency.LoadNode(currentNode.nodePointers[0].Pointer)
	}
	// reached an internal node
	return &BTreeIterator{currentNode: currentNode, offsetInNode: 0, tree: tree}
}

// Returns the "value" of a pair with the key specified.
// Returns nil if the key was not found in the tree.
func (tree *BTree) Get(key BTreeKeyType) *BTreePointer {
	if tree.rootPointer == InvalidBTreePointer {
		return nil
	}

	currentNode := tree.persistency.LoadNode(tree.rootPointer)
	if currentNode == nil {
		return nil
	}

	for currentNode.isInternal {
		currentNode, _ = currentNode.findMatchingNode(key)
		if currentNode == nil {
			return nil
		}
	}

	// currentNode is a leaf containing the value
	for _, item := range currentNode.nodePointers {
		if item.Key == key {
			return &item.Pointer
		}
	}

	// should never happen but let's return nil just in case
	return nil
}
