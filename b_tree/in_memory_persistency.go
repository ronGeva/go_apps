package b_tree

import (
	"errors"
	"sync/atomic"
)

var inMemoryRootPointer BTreePointer = -2

type InMemoryPersistency struct {
	nodesInMemory map[BTreePointer][]byte
	counter       int32
}

func InitializeInMemoryPersistency() *InMemoryPersistency {
	return &InMemoryPersistency{counter: 0, nodesInMemory: make(map[BTreePointer][]byte)}
}

func (api *InMemoryPersistency) Load(pointer BTreePointer) ([]byte, error) {
	if pointer == invalidBTreePointer {
		return nil, errors.New("invliad pointer passed to Load")
	}

	node, ok := api.nodesInMemory[pointer]
	if ok {
		return node, nil
	}

	if pointer == inMemoryRootPointer {
		return nil, BTreeNotInitialized
	}

	return nil, errors.New("data unavailable")
}

func (api *InMemoryPersistency) Persist(node []byte, pointer BTreePointer) (BTreePointer, error) {
	if pointer != invalidBTreePointer {
		api.nodesInMemory[pointer] = node
		return pointer, nil
	}

	newPointer := BTreePointer(atomic.AddInt32(&api.counter, 1))

	api.nodesInMemory[newPointer] = node
	return newPointer, nil
}

func (api *InMemoryPersistency) Delete(pointer BTreePointer) {
	delete(api.nodesInMemory, pointer)
}

func (api *InMemoryPersistency) RootPointer() BTreePointer {
	return inMemoryRootPointer
}
