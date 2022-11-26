package go_db

import (
	"encoding/binary"
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
	tableArrayPointer := getMutableDbPointer(db, TABLES_POINTER_OFFSET)
	tablePointerData := serializeDbPointer(newTablePointer)
	endOfWrite := appendDataToDataBlock(db, tablePointerData, uint32(tableArrayPointer.location))
	return mutableDbPointer{pointer: newTablePointer, location: endOfWrite - int64(DB_POINTER_SIZE)}
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
	// Put the size of the scheme data at the start of it
	binary.LittleEndian.PutUint32(schemeData[:4], uint32(len(schemeData)-4))
	appendDataToDataBlock(db, schemeData, uint32(mutablePointer.location))
	return uint32(len(schemeData))
}

func initializeNewTableContent(db *openDB, tableID string, scheme tableScheme, tablePointer *mutableDbPointer) {
	// Write table ID
	tableIDBytes := []byte(tableID)
	appendDataToDataBlock(db, serializeVariableSizeData(tableIDBytes), uint32(tablePointer.location))
	tablePointer.pointer.size = uint32(4 + len(tableIDBytes))

	// Write scheme
	tablePointer.pointer.size += writeTableScheme(db, scheme, *tablePointer, uint32(tablePointer.pointer.size))

	// Allocate bitmap block
	bitmapPointer := allocateNewDataBlock(db)
	// Bitmap is empty, no need to write anything - there are 0 records

	// Write bitmap pointer to table content
	appendDataToDataBlock(db, serializeDbPointer(bitmapPointer), uint32(tablePointer.location))
	tablePointer.pointer.size += DB_POINTER_SIZE

	// Allocate records array block
	recordsPointer := allocateNewDataBlock(db)

	// Write records array pointer to table content
	appendDataToDataBlock(db, serializeDbPointer(recordsPointer), uint32(tablePointer.location))
	tablePointer.pointer.size += DB_POINTER_SIZE
}

func writeNewTableLocalFile(db database, tableID string, scheme tableScheme) {
	openDatabase := getOpenDB(db)
	defer closeOpenDB(&openDatabase)
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

func findTable(openDatabse *openDB, tableID string) (*dbPointer, error) {
	tablesPointer := getDbPointer(openDatabse, TABLES_POINTER_OFFSET)
	tablesArrayBytes := readFromDbPointer(openDatabse, tablesPointer)
	for i := 0; i < len(tablesArrayBytes)/int(DB_POINTER_SIZE); i++ {
		currPointer := deserializeDbPointer(
			tablesArrayBytes[i*int(DB_POINTER_SIZE) : (i+1)*int(DB_POINTER_SIZE)])
		uniqueID := readVariableSizeDataFromDB(openDatabse, currPointer.offset)
		if string(uniqueID) == tableID {
			return &currPointer, nil
		}
	}
	return nil, &TableNotFoundError{tableID}
}

func writeRecordToTable(db *openDB, recordsPointer mutableDbPointer, recordIndex uint32, data []byte) {
	// Find the offset of the new record
	// TODO: handle data block extension
	prevAmountOfRecords := recordsPointer.pointer.size / DB_POINTER_SIZE
	if prevAmountOfRecords > recordIndex {
		// Override pre-existing invalid record
		offset := DB_POINTER_SIZE * recordIndex
		writeToDataBlock(db, recordsPointer.pointer, data, uint32(offset))
	} else {
		// Write a new record
		appendDataToDataBlock(db, data, uint32(recordsPointer.location))
	}
}

func addRecordToTableInternal(db *openDB, tablePointer dbPointer, recordData []byte) {
	uniqueIDSize := binary.LittleEndian.Uint32(readFromDB(db, 4, tablePointer.offset))
	schemeOffset := tablePointer.offset + 4 + uniqueIDSize
	schemeSize := binary.LittleEndian.Uint32(readFromDB(db, 4, schemeOffset))
	bitmapOffset := schemeOffset + 4 + schemeSize
	bitmapPointer := getDbPointer(db, bitmapOffset)
	bitmapData := readFromDbPointer(db, bitmapPointer)
	firstAvailableRecordNum := findFirstAvailableBlock(bitmapData)
	recordsArrayPointerOffset := bitmapOffset + DB_POINTER_SIZE
	recordsArrayPointer := getMutableDbPointer(db, recordsArrayPointerOffset)
	writeRecordToTable(db, recordsArrayPointer, firstAvailableRecordNum, recordData)
	writeBitToBitmap(db, int64(bitmapOffset), firstAvailableRecordNum, 1)
}

func addRecordToTable(db database, tableID string, record Record) {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)

	recordData := serializeRecord(&openDatabse, record)
	tablePointer, err := findTable(&openDatabse, tableID)
	check(err)
	addRecordToTableInternal(&openDatabse, *tablePointer, recordData)
}
