package b_tree

import (
	"encoding/binary"
	"errors"
)

var BTreeNotInitialized error = errors.New("an attempt was made to load an uninitialized tree")

type PersistencyInterface interface {
	// loads a buffer from a pointer that was previously received from Persist
	Load(BTreePointer) ([]byte, error)

	// Persists a buffer into the persistent storage
	// This function receives the optional parameter of the original pointer to the buffer,
	// and returns the new points to it (that can be used to load it again in the future)
	Persist([]byte, BTreePointer) (BTreePointer, error)

	// Deletes the buffer pointed at by the pointer supplied
	Delete(BTreePointer)

	// The b_tree module needs a way to initialize itself after a reset.
	// For this reason, the Persistency implementation must supply a way for the package to save its
	// root object into some predefined location (designated by the persistency store).
	// This pointer will be used during the execution of the b_tree package to persist changes in its root
	// object, and will be used during initialization to load the root back into memory.
	//
	// The b_tree library expects the first call to persistency.Load(persistency.RootPointer()) return return
	// the error BTreeNotInitialized.
	// Only by receiving this specific error will the b_tree module know this is a regular flow in which the tree
	// was yet to be persisted.
	RootPointer() BTreePointer
}

type persistencyApi struct {
	store PersistencyInterface
}

// structure:
// 0-3: maximumDegree
// 4: isInternal
// 5-12: nextNode
// 13-16: amount of node pointers
// 17-17+<amount>*16: node pointers
//
// Each node pointer is:
// 0-7: pointer
// 8-15: key
func (api *persistencyApi) LoadNode(pointer BTreePointer) *bTreeNode {
	data, _ := api.store.Load(pointer)
	maximumDegree := binary.LittleEndian.Uint32(data[:4])
	isInternal := data[4] != '\x00'
	nextNode := binary.LittleEndian.Uint64(data[5:13])
	amountOfNodePointers := binary.LittleEndian.Uint32(data[13:17])
	nodePointers := make([]BTreeKeyPointerPair, amountOfNodePointers)
	for i := 0; i < int(amountOfNodePointers); i++ {
		nodePointer := binary.LittleEndian.Uint64(data[17+i*16 : 17+i*16+8])
		nodeKey := binary.LittleEndian.Uint64(data[17+i*16+8 : 17+(i+1)*16])
		nodePointers[i] = BTreeKeyPointerPair{pointer: BTreePointer(nodePointer),
			key: bTreeKeyType(nodeKey)}
	}

	return &bTreeNode{isInternal: isInternal, maximumDegree: int(maximumDegree),
		nextNode: BTreePointer(nextNode), nodePointers: nodePointers, selfPointer: pointer,
		persistency: *api}
}

func (api *persistencyApi) PersistNode(node *bTreeNode, pointer BTreePointer) BTreePointer {
	data := make([]byte, 17+len(node.nodePointers)*16)
	binary.LittleEndian.PutUint32(data[:4], uint32(node.maximumDegree))
	if node.isInternal {
		data[4] = '\x01'
	} else {
		data[4] = '\x00'
	}
	binary.LittleEndian.PutUint64(data[5:13], uint64(node.nextNode))
	binary.LittleEndian.PutUint32(data[13:17], uint32(len(node.nodePointers)))
	for i := 0; i < len(node.nodePointers); i++ {
		p := node.nodePointers[i]
		binary.LittleEndian.PutUint64(data[17+i*16:17+i*16+8], uint64(p.pointer))
		binary.LittleEndian.PutUint64(data[17+i*16+8:17+(i+1)*16], uint64(p.key))
	}

	pointer, _ = api.store.Persist(data, pointer)
	return pointer
}

func (api *persistencyApi) RemoveNode(pointer BTreePointer) {
	api.store.Delete(pointer)
}

func (api *persistencyApi) PersistTree(tree *BTree) {
	data := serializeBTree(tree)
	// no need to save the return value, it is the store's responsibility to update RootPointer
	api.store.Persist(data, api.store.RootPointer())
}
