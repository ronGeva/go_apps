package b_tree

import (
	"encoding/binary"
)

var globalCounter int32 = 0

type PersistencyInterface interface {
	// loads a node from a bTreePointer that was previously received from PersistNode
	LoadNode(bTreePointer) []byte

	// Persists a node into the persistent storage
	// This function receives the optional parameter of the original pointer to the node,
	// and returns the new points to it (that can be used to load it again in the future)
	PersistNode([]byte, bTreePointer) bTreePointer

	// Deletes the node
	RemoveNode(bTreePointer)
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
func (api *persistencyApi) LoadNode(pointer bTreePointer) *bTreeNode {
	data := api.store.LoadNode(pointer)
	maximumDegree := binary.LittleEndian.Uint32(data[:4])
	isInternal := data[4] != '\x00'
	nextNode := binary.LittleEndian.Uint64(data[5:13])
	amountOfNodePointers := binary.LittleEndian.Uint32(data[13:17])
	nodePointers := make([]BTreeKeyPointerPair, amountOfNodePointers)
	for i := 0; i < int(amountOfNodePointers); i++ {
		nodePointer := binary.LittleEndian.Uint64(data[17+i*16 : 17+i*16+8])
		nodeKey := binary.LittleEndian.Uint64(data[17+i*16+8 : 17+(i+1)*16])
		nodePointers[i] = BTreeKeyPointerPair{pointer: bTreePointer(nodePointer),
			key: bTreeKeyType(nodeKey)}
	}

	return &bTreeNode{isInternal: isInternal, maximumDegree: int(maximumDegree),
		nextNode: bTreePointer(nextNode), nodePointers: nodePointers, selfPointer: pointer,
		persistency: *api}
}

func (api *persistencyApi) PersistNode(node *bTreeNode, pointer bTreePointer) bTreePointer {
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

	return api.store.PersistNode(data, pointer)
}

func (api *persistencyApi) RemoveNode(pointer bTreePointer) {
	api.store.RemoveNode(pointer)
}
