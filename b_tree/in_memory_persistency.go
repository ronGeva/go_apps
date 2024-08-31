package b_tree

import "sync/atomic"

type InMemoryPersistency struct {
	nodesInMemory map[bTreePointer][]byte
	counter       int32
}

func InitializeInMemoryPersistency() *InMemoryPersistency {
	return &InMemoryPersistency{counter: 0, nodesInMemory: make(map[bTreePointer][]byte)}
}

func (api *InMemoryPersistency) LoadNode(pointer bTreePointer) []byte {
	if pointer == invalidBTreePointer {
		return nil
	}

	node := api.nodesInMemory[pointer]
	return node
}

func (api *InMemoryPersistency) PersistNode(node []byte, pointer bTreePointer) bTreePointer {
	if pointer != invalidBTreePointer {
		api.nodesInMemory[pointer] = node
		return pointer
	}

	newPointer := bTreePointer(atomic.AddInt32(&api.counter, 1))

	api.nodesInMemory[newPointer] = node
	return newPointer
}

func (api *InMemoryPersistency) RemoveNode(pointer bTreePointer) {
	delete(api.nodesInMemory, pointer)
}
