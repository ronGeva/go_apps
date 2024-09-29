package go_db

import (
	"encoding/binary"
	"fmt"

	"github.com/ronGeva/go_apps/b_tree"
)

type columnHeader struct {
	columnName   string
	columnType   FieldType
	index        *b_tree.BTree
	indexPointer dbPointer
}

func serializeColumnHeader(columnHeader columnHeader) []byte {
	columnData := make([]byte, 0)
	columnData = append(columnData, byte(columnHeader.columnType))
	columnNameSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(columnNameSizeBytes, uint32(len(columnHeader.columnName)))
	columnData = append(columnData, columnNameSizeBytes...)
	columnData = append(columnData, []byte(columnHeader.columnName)...)
	columnData = append(columnData, serializeDbPointer(columnHeader.indexPointer)...)
	return columnData
}

func deserializeColumnHeader(db *openDB, schemeData []byte, offset int) (columnHeader, int) {
	columnType := int8(schemeData[offset])
	offset += 1
	columnNameSize := binary.LittleEndian.Uint32(schemeData[offset : offset+4])
	offset += 4
	columnName := string(schemeData[offset : offset+int(columnNameSize)])
	offset += int(columnNameSize)
	indexPointer := deserializeDbPointer(schemeData[offset : offset+int(DB_POINTER_SIZE)])
	offset += 8

	var bTree *b_tree.BTree = nil
	if indexPointer.offset != 0 {
		tree, err := initializeExistingBTree(db, indexPointer)
		if err != nil {
			// TODO: return an error
			panic(err)
		}
		bTree = tree
	}

	return columnHeader{columnType: FieldType(columnType), columnName: columnName, indexPointer: indexPointer,
		index: bTree}, offset
}

func initializeIndexInColumn(db *openDB, columns []columnHeader, columnOffset int) error {
	if columnOffset >= len(columns) {
		return fmt.Errorf("offset %d is bigger than amount of columns %d", columnOffset, len(columns))
	}

	column := &columns[columnOffset]

	// if the tree was yet to be initalized for the column, initialize it
	if column.index == nil {
		store := initializeNewBTreeStore(db)
		tree, err := b_tree.InitializeBTree(store)
		if err != nil {
			return nil
		}

		column.index = tree
		column.indexPointer = store.rootPointer
	}

	return nil
}
