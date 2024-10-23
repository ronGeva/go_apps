package go_db

import (
	"fmt"

	"github.com/ronGeva/go_apps/b_tree"
)

const INDEX_RECORD_OFFSET_SIZE = 4 // 4 bytes is enough to represent a uint32 variable

// ***************************** addition to indexes *****************************

// get the B-Tree that contains all the offsets of records that has the key <key>.
// this function is used only for indexes that support non-unique keys.
func indexGetSameKeyTree(db *openDB, index *b_tree.BTree, key b_tree.BTreeKeyType) (*b_tree.BTree, error) {
	pointer := index.Get(key)
	if pointer != nil {
		// a B-Tree was already initialized for this key value, return it
		return initializeExistingBTree(db, bTreePointerToDbPointer(*pointer))
	}

	// a B-Tree was yet to be initialized for this key value, create a new one
	store := initializeNewBTreeStore(db)

	// store the pointer to this new tree in the column index
	err := index.Insert(b_tree.BTreeKeyPointerPair{Key: key, Pointer: store.RootPointer()})
	if err != nil {
		return nil, err
	}

	return b_tree.InitializeBTree(store)
}

// Add a record to the index, given that the record's key is not necessarily unique within the table.
//
// Some of the fields of a record are non-unique.
// For those fields, we can't use their value as the key to the index directly,
// but rather we must use a single key to all records in which this column has the same value.
// The value will then point to another BTree, in which the keys will be the record offsets,
// and the values will contain dummy values.
func indexAddRecordNonUniqueKey(db *openDB, index *b_tree.BTree, key b_tree.BTreeKeyType, offset uint32) error {
	tree, err := indexGetSameKeyTree(db, index, key)
	if err != nil {
		return err
	}

	return tree.Insert(b_tree.BTreeKeyPointerPair{Key: b_tree.BTreeKeyType(offset), Pointer: 0})
}

// add a record to the index, given that the record's key is unique within the table.
func indexAddRecordUniqueKey(index *b_tree.BTree, key b_tree.BTreeKeyType, recordIndex uint32) error {
	// recordIndex is the index of the record in the table's bitmap,
	// which means we can easily figure out where the record is by using it
	err := index.Insert(b_tree.BTreeKeyPointerPair{Pointer: b_tree.BTreePointer(recordIndex),
		Key: key})

	return err
}

// add the record's key->offset pair to all of the table's indexes.
func addRecordToColumnIndexes(db *openDB, columns []columnHeader, record Record, recordIndex uint32) error {
	for i := 0; i < len(columns); i++ {
		column := columns[i]
		if column.index == nil {
			// the column has no index, ignore it
			continue
		}

		// we allow non-unique keys only on provenance fields, therefore we must keep track of whether
		// this is a provenance column.
		isProv := column.columnType == FieldTypeProvenance
		key := record.getRecordKey(isProv, i)
		if key == nil {
			// this field does not support indexing
			continue
		}

		var err error = nil
		if isProv {
			err = indexAddRecordNonUniqueKey(db, column.index, *key, recordIndex)
		} else {
			err = indexAddRecordUniqueKey(column.index, *key, recordIndex)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Adds a record to all the table's indexes - be them normal indexes or provenance indexes
func addRecordToIndexes(db *openDB, scheme *tableScheme, record Record, recordIndex uint32) error {
	err := addRecordToColumnIndexes(db, scheme.columns, record, recordIndex)
	if err != nil {
		return err
	}

	return addRecordToColumnIndexes(db, scheme.provColumns, record, recordIndex)
}

// ***************************** removal from indexes *****************************

// remove a record from the index, given that the record's key is not necessarily unique within the table
func indexRemoveRecordNonUniqueKey(db *openDB, index *b_tree.BTree, key b_tree.BTreeKeyType,
	recordOffset uint32) error {
	// get the pointer to the same-key tree containing the record's key
	pointer := index.Get(key)
	if pointer == nil {
		return fmt.Errorf("no index was found for non-unique key value %d", int(key))
	}

	// This tree contains all the record offsets whose key match the paramter "key".
	// The values in this tree are all dummy values
	tree, err := initializeExistingBTree(db, bTreePointerToDbPointer(*pointer))
	if err != nil {
		return err
	}

	// delete key from the same-key tree.
	// we leave the same-key tree as-is even if it is now empty, for optimizations (as to save time
	// in case a new record with the same key will be added).
	return tree.Delete(b_tree.BTreeKeyType(recordOffset))
}

// remove a record from the index, given that the record's key is unique within the table
func indexRemoveRecordUniqueKey(index *b_tree.BTree, key b_tree.BTreeKeyType) error {
	return index.Delete(key)
}

// remove a record from all of the table's column indexes by removing the key->offset pair from all
// of its column indexes
func removeRecordFromColumnIndex(record *recordForChange, scheme *tableScheme) error {
	keyIndex := 0
	for i := 0; i < len(scheme.columns); i++ {
		if scheme.columns[i].index == nil {
			continue
		}

		if keyIndex >= len(record.partialRecord.Fields) {
			return fmt.Errorf("missing index #%d in record for deletion, len of keys in record %d", i, len(record.partialRecord.Fields))
		}

		index := scheme.columns[i].index
		key := record.partialRecord.Fields[keyIndex].ToKey()
		if key == nil {
			return fmt.Errorf("failed to convert field to key value")
		}

		keyIndex += 1
		err := indexRemoveRecordUniqueKey(index, *key)
		if err != nil {
			return err
		}
	}

	return nil
}

// remove a record from all of the table's provenance indexes by removing the key->offset pair from all
// of its provenance indexes
func removeRecordFromProvIndex(db *openDB, record *recordForChange, scheme *tableScheme) error {
	keyIndex := 0
	for i := 0; i < len(scheme.provColumns); i++ {
		if scheme.provColumns[i].index == nil {
			continue
		}

		if keyIndex >= len(record.partialRecord.Provenance) {
			return fmt.Errorf("missing index #%d in record for deletion, len of keys in record %d", i, len(record.partialRecord.Provenance))
		}

		index := scheme.provColumns[i].index
		key := record.partialRecord.Provenance[keyIndex].ToKey()
		if key == nil {
			return fmt.Errorf("failed to convert field to key value")
		}

		keyIndex += 1

		err := indexRemoveRecordNonUniqueKey(db, index, *key, record.index)
		if err != nil {
			return err
		}
	}

	return nil
}

// removes a record from all of the table's indexes - be them normal indexes or provenance indexes
func removeRecordFromIndexes(db *openDB, record *recordForChange, scheme *tableScheme) error {
	err := removeRecordFromColumnIndex(record, scheme)
	if err != nil {
		return err
	}

	return removeRecordFromProvIndex(db, record, scheme)
}

// ***************************** index iterator *****************************

// an iterator used to retrieve records from a specific table using one of the table's indexes.
// the records are returned in increasing index value (since that is the BTree implementation we're
// using).
type indexTableIterator struct {
	// a connection to the DB
	db *openDB

	// a pointer to the records table in the current table.
	// used to retrieve a record given its index (like recordsPointer[recordOffset]).
	recordsPointer dbPointer

	// the expected size of a record in this table
	sizeOfRecord uint32

	// the scheme ot the table we're iterating over
	scheme tableScheme

	// the underlying index iterator used to retrieve new records
	iterator *b_tree.BTreeIterator

	// a cache of records we've already retrieved and can pop records out of on calls
	// to "next".
	// we have this member since on non-unique-key-indexes we might retrieve more than one
	// record in a single invocation of the underlying index.
	// since each iteration of the underlying
	// iterator retrieves all records with the same index key, then if there are multiple of those
	// we can expect multiple records to be retrieved in a single call.
	// for this reason we need to cache excess records retrieved and return them on future calls
	// to next().
	retrievedRecords []tableCurrentRecord

	// whether the underlying index used is of a provenance column.
	// non-provenance indexes are guaranteed to have a single record for each unique key.
	// provenance records do not guarantee that, so we need to handle provenance-index iteration differently.
	isProv bool
}

// deserialize the record at the given record offset.
func (iterator *indexTableIterator) getRecord(offset uint32) Record {
	recordData := readFromDbPointer(iterator.db, iterator.recordsPointer, iterator.sizeOfRecord,
		iterator.sizeOfRecord*offset)
	return deserializeRecord(iterator.db, recordData, iterator.scheme)
}

// cache the next record in the case of a non-unique-key index
func (iterator *indexTableIterator) cacheNextRecordNonUniqueKey(pair *b_tree.BTreeKeyPointerPair) {
	// parse the given pair's value as the key to a Btree
	sameKeyTreePointer := bTreePointerToDbPointer(pair.Pointer)
	tree, err := initializeExistingBTree(iterator.db, sameKeyTreePointer)
	if err != nil {
		return
	}

	// the B-Tree created contains all the records with the same key.
	// iterate this tree to completion and insert all the records into the "retrievedRecord" member.
	currentKeyIterator := tree.Iterator()
	pair = currentKeyIterator.Next()
	for pair != nil {
		offset := uint32(pair.Key)
		record := iterator.getRecord(offset)
		iterator.retrievedRecords = append(iterator.retrievedRecords, tableCurrentRecord{record: record, offset: offset})
		pair = currentKeyIterator.Next()
	}
}

// cache the next record in the case of a unique-key index
func (iterator *indexTableIterator) cacheNextRecordUniqueKey(pair *b_tree.BTreeKeyPointerPair) {
	offset := uint32(pair.Pointer)
	record := iterator.getRecord(offset)
	iterator.retrievedRecords = append(iterator.retrievedRecords, tableCurrentRecord{record: record, offset: offset})
}

// cache the next records to be retrieved
// once this function returns we expect new records to be cached.
// if no records were cached, we assume we've finished iterating the table.
func (iterator *indexTableIterator) cacheNextKeyRecords() {
	if len(iterator.retrievedRecords) > 0 {
		return
	}

	pair := iterator.iterator.Next()
	if pair == nil {
		return
	}

	if iterator.isProv {
		iterator.cacheNextRecordNonUniqueKey(pair)
	} else {
		iterator.cacheNextRecordUniqueKey(pair)
	}
}

// get the next record with the smallest key
func (iterator *indexTableIterator) next() *tableCurrentRecord {
	iterator.cacheNextKeyRecords()

	if len(iterator.retrievedRecords) == 0 {
		return nil
	}

	record := iterator.retrievedRecords[0]
	iterator.retrievedRecords = iterator.retrievedRecords[1:]

	return &record
}

// initialize a new table iterator that retrieves records according to the index of the column at <columnOffset>.
// if a provenance column is to be used, isProv should be true, otherwise - false.
func indexInitializeTableIterator(db *openDB, tableId string, columnOffset uint32, isProv bool) (*indexTableIterator, error) {
	tablePointer, err := findTable(db, tableId)
	if err != nil {
		return nil, err
	}
	headers := parseTableHeaders(db, *tablePointer)
	recordsPointer := headers.records
	sizeOfRecord := int(DB_POINTER_SIZE) * headers.scheme.fieldsInRecord()

	var index *b_tree.BTree = nil
	if isProv {
		if int(columnOffset) >= len(headers.scheme.provColumns) {
			return nil, fmt.Errorf("no provenance column exists at index %d", columnOffset)
		}
		index = headers.scheme.provColumns[columnOffset].index
	} else {
		if int(columnOffset) >= len(headers.scheme.columns) {
			return nil, fmt.Errorf("no column exists at index %d", columnOffset)
		}
		index = headers.scheme.columns[columnOffset].index
	}

	if index == nil {
		return nil, fmt.Errorf("requested index was not found")
	}

	iterator := index.Iterator()
	if iterator == nil {
		return nil, fmt.Errorf("failed to initialize index iterator for table %s, isProv %t, column offset %d",
			tableId, isProv, columnOffset)
	}

	return &indexTableIterator{db: db, sizeOfRecord: uint32(sizeOfRecord), scheme: headers.scheme,
		recordsPointer: recordsPointer.pointer, iterator: iterator, isProv: isProv}, nil
}
