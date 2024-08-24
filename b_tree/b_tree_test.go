package b_tree

import (
	"math/rand"
	"sort"
	"testing"
)

// insert 100 incrementing bTree pairs and fail if something crashes
func TestInsertionSanity(t *testing.T) {
	tree, error := InitializeBTree()
	if error != nil {
		t.Fail()
	}

	for i := 0; i < 100; i++ {
		pair := BTreeKeyPointerPair{pointer: bTreePointer(i), key: bTreeKeyType(i)}
		tree.Insert(pair)
	}
}

// insert 100 incrementing bTree pairs, then iterate them in order and make sure we get the correct
// output
func TestInsertThenIterate(t *testing.T) {
	tree, error := InitializeBTree()
	if error != nil {
		t.Fail()
	}

	for i := 0; i < 100; i++ {
		pair := BTreeKeyPointerPair{pointer: bTreePointer(i), key: bTreeKeyType(i)}
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
	tree, error := InitializeBTree()
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
		pairs[i].pointer = bTreePointer(i)
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
