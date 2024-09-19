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
	return pairs.pairs[i].Key < pairs.pairs[j].Key
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
		pair := BTreeKeyPointerPair{Pointer: BTreePointer(i), Key: BTreeKeyType(i)}
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
		pair := BTreeKeyPointerPair{Pointer: BTreePointer(i), Key: BTreeKeyType(i)}
		tree.Insert(pair)
	}

	iterator := tree.Iterator()
	pair := iterator.Next()
	i := BTreeKeyType(0)
	for pair != nil {
		if pair.Key != i {
			t.Fail()
		}
		i++
		pair = iterator.Next()
	}

	if i != 100 {
		t.Fail()
	}
}

// Function to generate a unique random number
func generateUniqueNumber(seenNumbers map[int]bool, maxNumber int) int {
	for {
		// Generate a random number between 0 and maxNumber-1
		num := rand.Intn(maxNumber)

		// Check if the number has already been seen
		if !seenNumbers[num] {
			// Mark the number as seen and return it
			seenNumbers[num] = true
			return num
		}
	}
}

// insert 10000 random entries, then iterate them in order and make sure we get the correct results
func TestInsertThenIterateRandomOrder(t *testing.T) {
	tree, error := initializeInMemoryBTree()
	if error != nil {
		t.Fail()
	}

	// Create a map to store the numbers we've seen
	seenNumbers := make(map[int]bool)
	amount := 10000
	randomRange := 100000

	pairs := make([]BTreeKeyPointerPair, amount)
	values := make([]int, amount)
	for i := 0; i < len(pairs); i++ {
		randomNumber := generateUniqueNumber(seenNumbers, randomRange)
		values[i] = randomNumber
		pairs[i].Key = BTreeKeyType(randomNumber)
		pairs[i].Pointer = BTreePointer(i)
		tree.Insert(pairs[i])
	}

	sort.Ints(values)

	iterator := tree.Iterator()
	iteratorOutput := make([]BTreeKeyPointerPair, 0)
	pair := iterator.Next()
	for pair != nil {
		iteratorOutput = append(iteratorOutput, *pair)
		if pair.Key != BTreeKeyType(values[len(iteratorOutput)-1]) {
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

	item := BTreeKeyPointerPair{Pointer: BTreePointer(5), Key: BTreeKeyType(12)}
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

	// Create a map to store the numbers we've seen
	seenNumbers := make(map[int]bool)
	amount := 1000
	randomRange := 100000

	pairs := bTreePairArray{}
	for i := 0; i < amount; i++ {
		randomNumber := generateUniqueNumber(seenNumbers, randomRange)
		pairs.pairs = append(pairs.pairs, BTreeKeyPointerPair{Key: BTreeKeyType(randomNumber),
			Pointer: BTreePointer(i)})
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
		if pair.Key != pairs.pairs[len(iteratorOutput)-1].Key {
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
	pointers := []BTreeKeyPointerPair{{Pointer: 11, Key: 5}, {Pointer: 55, Key: 37}}
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
	pointers := []BTreeKeyPointerPair{{Pointer: 11, Key: 5}, {Pointer: 55, Key: 37}}
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
	if pair.Key != 2 || pair.Pointer != 1 {
		t.Fail()
	}
}

func TestEnforceNoDuplicatesSmallTree(t *testing.T) {
	store := InitializeInMemoryPersistency()
	tree, err := InitializeBTree(store)
	if err != nil {
		t.Fail()
	}

	// both of those pairs have the same key
	firstPair := BTreeKeyPointerPair{1, 2}
	secondPair := BTreeKeyPointerPair{11, 2}
	err = tree.Insert(firstPair)
	if err != nil {
		t.Fail()
	}

	err = tree.Insert(secondPair)
	if err != BTreeErrorKeyAlreadyExists {
		t.Fail()
	}
}

func insertRandomPairsIntoTree(tree *BTree, amount int, t *testing.T) []BTreeKeyPointerPair {
	// Create a map to store the numbers we've seen
	seenNumbers := make(map[int]bool)

	// this should help reduce the chance of collison
	randomRange := amount * 100

	pairs := make([]BTreeKeyPointerPair, 0)
	for i := 0; i < amount; i++ {
		randomNumber := generateUniqueNumber(seenNumbers, randomRange)
		pairs = append(pairs, BTreeKeyPointerPair{Key: BTreeKeyType(randomNumber),
			Pointer: BTreePointer(i)})
		err := tree.Insert(pairs[i])
		if err != nil {
			t.FailNow()
		}
	}

	return pairs
}

func TestEnforceNoDuplicatesBigTree(t *testing.T) {
	store := InitializeInMemoryPersistency()
	tree, err := InitializeBTree(store)
	if err != nil {
		t.Fail()
	}

	pairs := insertRandomPairsIntoTree(tree, 1000, t)
	pairToChoose := rand.Intn(len(pairs) - 1)
	pairToChooseKey := pairs[pairToChoose].Key
	err = tree.Insert(BTreeKeyPointerPair{Pointer: 100, Key: pairToChooseKey})
	if err != BTreeErrorKeyAlreadyExists {
		t.FailNow()
	}
}
