package go_db

import "github.com/ronGeva/go_apps/b_tree"

// the persistency object used to allow a b_tree.BTree to save/load data to a persistent storage
// this persistency store implements b_tree.PersistencyInterface and uses the DB's data block bitmap
// to allocate/free data blocks for the Tree's usage.
type bTreePersistencyStore struct {
	db              *openDB
	rootPointer     dbPointer
	rootInitialized bool
}

// returns the maximum amount of bytes stored in a single DB datablock
func dataBlockPaddingSize(db *openDB) uint32 {
	return db.header.dataBlockSize - 4
}

// allocate a datablock and pad it with zeroes up to the maximum bytes we can write
// return the db pointer to it.
func allocatePaddedDatablock(db *openDB) dbPointer {
	paddingSize := dataBlockPaddingSize(db)
	p := allocateNewDataBlock(db)
	appendDataToDataBlockImmutablePointer(db, make([]byte, paddingSize), p)
	p.size = paddingSize

	return p
}

// initializes a new BTree store which uses the DB as the underlying persistency for the BTree
func initializeNewBTreeStore(db *openDB) *bTreePersistencyStore {
	rootPointer := allocatePaddedDatablock(db)
	return &bTreePersistencyStore{db: db, rootPointer: rootPointer, rootInitialized: false}
}

// initializes an existing BTree store which uses the DB as the underlying persistency for the BTree
func initializeExistingBTree(db *openDB, pointer dbPointer) (*b_tree.BTree, error) {
	store := bTreePersistencyStore{db: db, rootPointer: pointer, rootInitialized: true}
	return b_tree.InitializeBTree(&store)
}

// since sizeof(dbPointer) == sizeof(b_tree.BTreePointer), we can easily convert each of them
// to the other using bit shifts

// converts a dbPointer into a BTreePointer.
func dbPointerToBTreePointer(p dbPointer) b_tree.BTreePointer {
	var treePointer int64 = 0
	treePointer |= (int64(p.offset) << 32)
	treePointer |= int64(p.size)
	return b_tree.BTreePointer(treePointer)
}

// converts a BTreePointer into a dbPointer.
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
		// the BTree only allocates 2 types of structures - the Tree headers and a node.
		// each of those have a maximum size no more than a few hundred bytes.
		// in addition, the BTree never assumes the data returned on calls to Load isn't padded, only that its
		// start was written by it.
		// for this reason we can safely use a single fully-used datablock for all allocations, and pad non-used
		// bytes we zeroes.
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
