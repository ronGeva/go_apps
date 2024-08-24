package b_tree

import "testing"

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
