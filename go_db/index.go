package go_db

import (
	"fmt"

	"github.com/ronGeva/go_apps/b_tree"
)

const INDEX_RECORD_OFFSET_SIZE = 4 // 4 bytes is enough to represent a uint32 variable

func indexGetSameKeyTree(db *openDB, index *b_tree.BTree, key b_tree.BTreeKeyType) (*b_tree.BTree, error) {
	pointer := index.Get(key)
	if pointer != nil {
		return initializeExistingBTree(db, bTreePointerToDbPointer(*pointer))
	}

	// create a BTree for all records with this key
	store := initializeNewBTreeStore(db)

	// store the pointer to this new tree in the column index
	err := index.Insert(b_tree.BTreeKeyPointerPair{Key: key, Pointer: store.RootPointer()})
	if err != nil {
		return nil, err
	}

	return b_tree.InitializeBTree(store)
}

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

func indexAddRecordUniqueKey(index *b_tree.BTree, key b_tree.BTreeKeyType, recordIndex uint32) error {
	// recordIndex is the index of the record in the table's bitmap,
	// which means we can easily figure out where the record is by using it
	err := index.Insert(b_tree.BTreeKeyPointerPair{Pointer: b_tree.BTreePointer(recordIndex),
		Key: key})

	return err
}

func indexRemoveRecordNonUniqueKey(db *openDB, index *b_tree.BTree, key b_tree.BTreeKeyType,
	recordOffset uint32) error {
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

	return tree.Delete(b_tree.BTreeKeyType(recordOffset))
}

func indexRemoveRecordUniqueKey(index *b_tree.BTree, key b_tree.BTreeKeyType) error {
	return index.Delete(key)
}

func addRecordToColumnIndexes(db *openDB, columns []columnHeader, record Record, recordIndex uint32) error {
	for i := 0; i < len(columns); i++ {
		column := columns[i]
		if column.index == nil {
			continue
		}

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

// Adds a record to all the table's indexes
func addRecordToIndexes(db *openDB, scheme *tableScheme, record Record, recordIndex uint32) error {
	err := addRecordToColumnIndexes(db, scheme.columns, record, recordIndex)
	if err != nil {
		return err
	}

	return addRecordToColumnIndexes(db, scheme.provColumns, record, recordIndex)
}

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
		key := record.partialRecord.Fields[keyIndex].ToKey()
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

func removeRecordFromIndexes(db *openDB, record *recordForChange, scheme *tableScheme) error {
	err := removeRecordFromColumnIndex(record, scheme)
	if err != nil {
		return err
	}

	return removeRecordFromProvIndex(db, record, scheme)
}

type indexTableIterator struct {
	db               *openDB
	recordsPointer   dbPointer
	sizeOfRecord     uint32
	scheme           tableScheme
	iterator         *b_tree.BTreeIterator
	retrievedRecords []tableCurrentRecord
	isProv           bool
}

func (iterator *indexTableIterator) getRecord(offset uint32) Record {
	recordData := readFromDbPointer(iterator.db, iterator.recordsPointer, iterator.sizeOfRecord,
		iterator.sizeOfRecord*offset)
	return deserializeRecord(iterator.db, recordData, iterator.scheme)
}

func (iterator *indexTableIterator) cacheNextRecordNonUniqueKey(pair *b_tree.BTreeKeyPointerPair) {
	sameKeyTreePointer := bTreePointerToDbPointer(pair.Pointer)
	tree, err := initializeExistingBTree(iterator.db, sameKeyTreePointer)
	if err != nil {
		return
	}

	currentKeyIterator := tree.Iterator()
	pair = currentKeyIterator.Next()
	for pair != nil {
		offset := uint32(pair.Key)
		record := iterator.getRecord(offset)
		iterator.retrievedRecords = append(iterator.retrievedRecords, tableCurrentRecord{record: record, offset: offset})
		pair = currentKeyIterator.Next()
	}
}

func (iterator *indexTableIterator) cacheNextRecordUniqueKey(pair *b_tree.BTreeKeyPointerPair) {
	offset := uint32(pair.Pointer)
	record := iterator.getRecord(offset)
	iterator.retrievedRecords = append(iterator.retrievedRecords, tableCurrentRecord{record: record, offset: offset})
}

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

func (iterator *indexTableIterator) next() *tableCurrentRecord {
	iterator.cacheNextKeyRecords()

	if len(iterator.retrievedRecords) == 0 {
		return nil
	}

	record := iterator.retrievedRecords[0]
	iterator.retrievedRecords = iterator.retrievedRecords[1:]

	return &record
}

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
