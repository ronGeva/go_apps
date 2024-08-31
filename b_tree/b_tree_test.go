package b_tree

import (
	"math/rand"
	"sort"
	"testing"
)

type bTreePairArray struct {
	pairs []BTreeKeyPointerPair
}

func (pairs *bTreePairArray) Len() int {
	return len(pairs.pairs)
}

func (pairs *bTreePairArray) Less(i, j int) bool {
	return pairs.pairs[i].key < pairs.pairs[j].key
}

func (pairs *bTreePairArray) Swap(i, j int) {
	temp := pairs.pairs[i]
	pairs.pairs[i] = pairs.pairs[j]
	pairs.pairs[j] = temp
}

func initializeInMemoryBTree() (*BTree, error) {
	return InitializeBTree(InitializeInMemoryPersistency())
}

// insert 100 incrementing bTree pairs and fail if something crashes
func TestInsertionSanity(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	for i := 0; i < 100; i++ {
		pair := BTreeKeyPointerPair{pointer: BTreePointer(i), key: bTreeKeyType(i)}
		tree.Insert(pair)
	}
}

// insert 100 incrementing bTree pairs, then iterate them in order and make sure we get the correct
// output
func TestInsertThenIterate(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	for i := 0; i < 100; i++ {
		pair := BTreeKeyPointerPair{pointer: BTreePointer(i), key: bTreeKeyType(i)}
		tree.Insert(pair)
	}

	iterator := tree.Iterator()
	pair := iterator.Next()
	i := bTreeKeyType(0)
	for pair != nil {
		if pair.key != i {
			t.Fail()
		}
		i++
		pair = iterator.Next()
	}

	if i != 100 {
		t.Fail()
	}
}

// insert 10000 random entries, then iterate them in order and make sure we get the correct results
func TestInsertThenIterateRandomOrder(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	amount := 10000
	randomRange := 100000

	pairs := make([]BTreeKeyPointerPair, amount)
	values := make([]int, amount)
	for i := 0; i < len(pairs); i++ {
		randomNumber := rand.Intn(randomRange)
		values[i] = randomNumber
		pairs[i].key = bTreeKeyType(randomNumber)
		pairs[i].pointer = BTreePointer(i)
		tree.Insert(pairs[i])
	}

	sort.Ints(values)

	iterator := tree.Iterator()
	iteratorOutput := make([]BTreeKeyPointerPair, 0)
	pair := iterator.Next()
	for pair != nil {
		iteratorOutput = append(iteratorOutput, *pair)
		if pair.key != bTreeKeyType(values[len(iteratorOutput)-1]) {
			t.Fail()
		}
		pair = iterator.Next()
	}

	if len(iteratorOutput) != amount {
		t.Fail()
	}
}

func TestSingleInsertThenDelete(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	item := BTreeKeyPointerPair{pointer: BTreePointer(5), key: bTreeKeyType(12)}
	tree.Insert(item)
	tree.Delete(item)

	// make sure the tree is now empty
	if tree.rootPointer != InvalidBTreePointer {
		t.Fail()
	}
}

func TestMultipleInsertThenPartialDelete(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	amount := 1000
	randomRange := 100000

	pairs := bTreePairArray{}
	for i := 0; i < amount; i++ {
		randomNumber := rand.Intn(randomRange)
		pairs.pairs = append(pairs.pairs, BTreeKeyPointerPair{key: bTreeKeyType(randomNumber),
			pointer: BTreePointer(i)})
		tree.Insert(pairs.pairs[i])
	}

	deletionAmount := rand.Intn(amount / 2)
	deletionStart := rand.Intn(amount - deletionAmount)
	for i := deletionStart; i < deletionStart+deletionAmount; i++ {
		tree.Delete(pairs.pairs[i])
	}

	pairs.pairs = append(pairs.pairs[:deletionStart], pairs.pairs[deletionStart+deletionAmount:]...)
	sort.Sort(&pairs)
	iterator := tree.Iterator()

	iteratorOutput := make([]BTreeKeyPointerPair, 0)
	pair := iterator.Next()
	for pair != nil {
		iteratorOutput = append(iteratorOutput, *pair)
		if pair.key != pairs.pairs[len(iteratorOutput)-1].key {
			t.Fail()
		}
		pair = iterator.Next()
	}

	if len(iteratorOutput) != pairs.Len() {
		t.Fail()
	}
}

// A simple persistency store which holds a single buffer and always returns it
type dummyPersistencyStore struct {
	buffer []byte
}

var dummyPersistencyStoreRootPointer BTreePointer = -2

func (store *dummyPersistencyStore) Load(pointer BTreePointer) ([]byte, error) {
	if pointer == dummyPersistencyStoreRootPointer {
		return nil, BTreeNotInitialized
	}

	return store.buffer, nil
}

func (store *dummyPersistencyStore) Persist(data []byte, pointer BTreePointer) (BTreePointer, error) {
	store.buffer = data
	return pointer, nil
}

func (store *dummyPersistencyStore) Delete(BTreePointer) {
	store.buffer = []byte{}
}

func (store *dummyPersistencyStore) RootPointer() BTreePointer {
	return dummyPersistencyStoreRootPointer
}

func areNodesEqual(left *bTreeNode, right *bTreeNode) bool {
	if left.isInternal != right.isInternal {
		return false
	}

	if left.maximumDegree != right.maximumDegree {
		return false
	}

	if left.nextNode != right.nextNode {
		return false
	}

	if left.selfPointer != right.selfPointer {
		return false
	}

	if len(left.nodePointers) != len(right.nodePointers) {
		return false
	}

	for i := 0; i < len(left.nodePointers); i++ {
		if left.nodePointers[i] != right.nodePointers[i] {
			return false
		}
	}
	return true
}

func TestPersistencySanity(t *testing.T) {
	dummyPointer := BTreePointer(105)
	persistency := persistencyApi{store: &dummyPersistencyStore{}}
	pointers := []BTreeKeyPointerPair{{pointer: 11, key: 5}, {pointer: 55, key: 37}}
	node := bTreeNode{isInternal: true, maximumDegree: 17, persistency: persistency,
		nextNode: 101, nodePointers: pointers, selfPointer: dummyPointer}
	persistency.PersistNode(&node, dummyPointer)
	outputNode := persistency.LoadNode(dummyPointer)

	if !areNodesEqual(&node, outputNode) {
		t.Fail()
	}
}

func TestPersistencyInMemoryStore(t *testing.T) {
	dummyPointer := BTreePointer(105)
	persistency := persistencyApi{store: InitializeInMemoryPersistency()}
	pointers := []BTreeKeyPointerPair{{pointer: 11, key: 5}, {pointer: 55, key: 37}}
	node := bTreeNode{isInternal: true, maximumDegree: 17, persistency: persistency,
		nextNode: 101, nodePointers: pointers, selfPointer: dummyPointer}
	persistency.PersistNode(&node, dummyPointer)
	outputNode := persistency.LoadNode(dummyPointer)

	if !areNodesEqual(&node, outputNode) {
		t.Fail()
	}
}

func TestMultipleInitializationSanity(t *testing.T) {
	store := InitializeInMemoryPersistency()
	tree, err := InitializeBTree(store)
	if err != nil {
		t.Fail()
	}

	tree.Insert(BTreeKeyPointerPair{1, 2})
	tree, err = InitializeBTree(store)
	if err != nil {
		t.Fail()
	}

	iterator := tree.Iterator()
	pair := iterator.Next()
	if pair.key != 2 || pair.pointer != 1 {
		t.Fail()
	}
}
