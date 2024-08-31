package go_db

import "github.com/ronGeva/go_apps/b_tree"

type bTreePersistencyStore struct {
	db              *openDB
	rootPointer     dbPointer
	rootInitialized bool
}

func dataBlockPaddingSize(db *openDB) uint32 {
	return db.header.dataBlockSize - 4
}

func allocatePaddedDatablock(db *openDB) dbPointer {
	paddingSize := dataBlockPaddingSize(db)
	p := allocateNewDataBlock(db)
	appendDataToDataBlockImmutablePointer(db, make([]byte, paddingSize), p)
	p.size = paddingSize

	return p
}

func initializeNewBTreeStore(db *openDB) *bTreePersistencyStore {
	rootPointer := allocatePaddedDatablock(db)
	return &bTreePersistencyStore{db: db, rootPointer: rootPointer, rootInitialized: false}
}

func initializeExistingBTree(db *openDB, pointer dbPointer) (*b_tree.BTree, error) {
	store := bTreePersistencyStore{db: db, rootPointer: pointer, rootInitialized: true}
	return b_tree.InitializeBTree(&store)
}

func dbPointerToBTreePointer(p dbPointer) b_tree.BTreePointer {
	var treePointer int64 = 0
	treePointer |= (int64(p.offset) << 32)
	treePointer |= int64(p.size)
	return b_tree.BTreePointer(treePointer)
}

func bTreePointerToDbPointer(p b_tree.BTreePointer) dbPointer {
	pointer := dbPointer{}
	pointer.offset = uint32(int64(p) >> 32)
	pointer.size = uint32(p & 0xffffffff)

	return pointer
}

func (store *bTreePersistencyStore) Load(pointer b_tree.BTreePointer) ([]byte, error) {
	if !store.rootInitialized {
		// the b-tree was yet to be initialized, send this specific error mentioned in the BTree documentation
		// so that the b-tree library could properly intialize
		return nil, b_tree.BTreeNotInitialized
	}

	dbPointer := bTreePointerToDbPointer(pointer)
	return readAllDataFromDbPointer(store.db, dbPointer), nil
}

func (store *bTreePersistencyStore) Persist(data []byte, pointer b_tree.BTreePointer) (b_tree.BTreePointer, error) {
	var p dbPointer
	if pointer == b_tree.InvalidBTreePointer {
		// TODO: implement this better. This is a workaround to make sure we won't need to increase the size
		// of a b_tree structure's data block
		p = allocatePaddedDatablock(store.db)
	} else {
		p = bTreePointerToDbPointer(pointer)
	}

	writeToDataBlock(store.db, p, data, 0)
	if pointer == b_tree.BTreePointer(store.rootPointer.offset) {
		store.rootInitialized = true
	}

	return dbPointerToBTreePointer(p), nil
}

func (store *bTreePersistencyStore) Delete(pointer b_tree.BTreePointer) {
	p := dbPointer{offset: uint32(pointer)}
	deallocateDbPointer(store.db, p)
}

func (store *bTreePersistencyStore) RootPointer() b_tree.BTreePointer {
	return dbPointerToBTreePointer(store.rootPointer)
}
