package go_db

import (
	"encoding/binary"
	"os"
)

type columndHeader struct {
	columnName string
	columnType FieldType
}

type tableScheme struct {
	columns []columndHeader
}

/*
The recordID struct uniquely represents a single record within a table.
*/
type recordID struct {
	id int
}

type table struct {
	scheme    tableScheme
	recordIds []recordID
}

func createTable(scheme tableScheme, db database) *table {
	return &table{scheme: scheme, recordIds: make([]recordID, 0)}
}

func (t *table) addRecord(record *Record) {
	// write record to DB, get unique ID for it

	recordID := recordID{len(t.recordIds)}
	t.recordIds = append(t.recordIds, recordID)
}

func addNewTableToTablesArray(db *openDB, newTablePointer dbPointer) mutableDbPointer {
	// Get the table array db pointer
	tableArrayPointer := getDbPointer(db, TABLES_POINTER_OFFSET)
	db.f.Seek(int64(TABLES_POINTER_OFFSET), 0)

	// Increase the size of the table array
	tablesArrayData := readFromDbPointer(db, tableArrayPointer)
	tableArraySize := binary.LittleEndian.Uint32(tablesArrayData[:4])
	tableArraySize++
	tableArraySizeData := make([]byte, 4)
	binary.LittleEndian.PutUint32(tableArraySizeData, tableArraySize)
	writeToDataBlock(db, tableArrayPointer, tablesArrayData, 0)

	// Write the new table's pointer to the end of the table array
	serializedPointer := serializeDbPointer(newTablePointer)
	writeOffset := appendDataToDataBlock(db, serializedPointer, TABLES_POINTER_OFFSET)
	pointerLocation := writeOffset - int64(DB_POINTER_SIZE)
	return mutableDbPointer{pointer: newTablePointer, location: pointerLocation}
}

func writeTableScheme(db *openDB, scheme tableScheme, mutablePointer mutableDbPointer, offset uint32) uint32 {
	schemeData := make([]byte, 4) // column headers size will contain unitialized data at first
	headersSize := 0
	for _, columnHeader := range scheme.columns {
		schemeData = append(schemeData, byte(columnHeader.columnType))
		columnNameSizeBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(columnNameSizeBytes, uint32(len(columnHeader.columnName)))
		schemeData = append(schemeData, columnNameSizeBytes...)
		schemeData = append(schemeData, []byte(columnHeader.columnName)...)
		headersSize += 1 + 4 + len(columnHeader.columnName)
	}
	appendDataToDataBlock(db, schemeData, uint32(mutablePointer.location))
	return uint32(len(schemeData))
}

func initializeNewTableContent(db *openDB, tableID string, scheme tableScheme, mutablePointer *mutableDbPointer) {
	pointer := &mutablePointer.pointer

	// Write table ID
	tableIDBytes := []byte(tableID)
	writeVariableSizeDataToDB(db, tableIDBytes, pointer.offset)
	mutablePointer.pointer.size = uint32(4 + len(tableIDBytes))

	// Write scheme
	mutablePointer.pointer.size += writeTableScheme(db, scheme, *mutablePointer, uint32(mutablePointer.pointer.size))

	// Write records array, which is empty...
	recordsArraySizeBytes := uint32ToBytes(0)
	appendDataToDataBlock(db, recordsArraySizeBytes, uint32(mutablePointer.location))
	mutablePointer.pointer.size += 4
}

func writeNewTableLocalFile(db database, tableID string, scheme tableScheme) {
	dbPath := db.id.identifyingString
	// TODO: change this to allow multiple read-writes at the same time
	f, err := os.OpenFile(dbPath, os.O_RDWR, os.ModeExclusive)
	defer f.Close()
	check(err)

	headerData := readFromFile(f, DB_HEADER_SIZE, 0)
	header := deserializeDbHeader(headerData)
	openDatabase := openDB{f: f, header: header}
	newTablePointer := allocateNewDataBlock(&openDatabase)
	mutablePointer := addNewTableToTablesArray(&openDatabase, newTablePointer)
	initializeNewTableContent(&openDatabase, tableID, scheme, &mutablePointer)
}

func writeNewTable(db database, tableID string, scheme tableScheme) {
	if db.id.ioType != LocalFile {
		panic(UnsupportedError{})
	}
	// TODO: validate no other table exists with this tableID

	writeNewTableLocalFile(db, tableID, scheme)
}

// For testing
func WriteNewTable(db database, tableID string, scheme tableScheme) {
	writeNewTable(db, tableID, scheme)
}
