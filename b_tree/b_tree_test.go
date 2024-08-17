package b_tree

import "testing"

func TestInsertion(t *testing.T) {
	tree, error := InitializeBTree()
	if error != nil {
		t.Fail()
	}

	pairs := make([]bTreeKeyPointerPair, 100)
	pairs[0].key = 1
	pairs[0].pointer = 1
	for i := 0; i < 100; i++ {
		pair := bTreeKeyPointerPair{pointer: bTreePointer(i), key: bTreeKeyType(i)}
		tree.Insert(pair)
	}
}
