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

type tableHeaders struct {
	scheme  tableScheme
	bitmap  mutableDbPointer
	records mutableDbPointer
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

func parseTableScheme(schemeData []byte) tableScheme {
	scheme := tableScheme{}
	for i := 0; i < len(schemeData); {
		columnType := int8(schemeData[i])
		columnNameSize := binary.LittleEndian.Uint32(schemeData[i+1 : i+5])
		columnName := string(schemeData[i+5 : i+5+int(columnNameSize)])
		scheme.columns = append(scheme.columns,
			columndHeader{columnType: FieldType(columnType), columnName: columnName})
		i += 5 + int(columnNameSize)
	}
	return scheme
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
	tablesArrayBytes := readAllDataFromDbPointer(openDatabse, tablesPointer)
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

func writeRecordToTable(db *openDB, headers tableHeaders, recordIndex uint32, data []byte) {
	// Find the offset of the new record
	// TODO: handle data block extension

	recordsPointer := headers.records
	prevAmountOfRecords := recordsPointer.pointer.size / (DB_POINTER_SIZE * uint32(len(headers.scheme.columns)))
	if prevAmountOfRecords > recordIndex {
		// Override pre-existing invalid record
		offset := DB_POINTER_SIZE * recordIndex
		writeToDataBlock(db, recordsPointer.pointer, data, uint32(offset))
	} else {
		// Write a new record
		appendDataToDataBlock(db, data, uint32(recordsPointer.location))
	}
}

func parseTableHeaders(db *openDB, tablePointer dbPointer) tableHeaders {
	uniqueIDSize := binary.LittleEndian.Uint32(readFromDB(db, 4, tablePointer.offset))
	schemeOffset := tablePointer.offset + 4 + uniqueIDSize
	schemeSize := binary.LittleEndian.Uint32(readFromDB(db, 4, schemeOffset))
	schemeData := readFromDB(db, schemeSize, schemeOffset+4)
	scheme := parseTableScheme(schemeData)
	bitmapOffset := schemeOffset + 4 + schemeSize
	bitmapPointer := getDbPointer(db, bitmapOffset)
	recordsPointerOffset := bitmapOffset + DB_POINTER_SIZE
	recordsPointer := getDbPointer(db, recordsPointerOffset)
	return tableHeaders{scheme: scheme,
		bitmap:  mutableDbPointer{pointer: bitmapPointer, location: int64(bitmapOffset)},
		records: mutableDbPointer{pointer: recordsPointer, location: int64(recordsPointerOffset)},
	}
}

func addRecordToTableInternal(db *openDB, tablePointer dbPointer, recordData []byte) {
	headers := parseTableHeaders(db, tablePointer)
	bitmapData := readAllDataFromDbPointer(db, headers.bitmap.pointer)
	firstAvailableRecordNum := findFirstAvailableBlock(bitmapData)

	writeRecordToTable(db, headers, firstAvailableRecordNum, recordData)
	writeBitToBitmap(db, int64(headers.bitmap.location), firstAvailableRecordNum, 1)
}

func addRecordToTable(db database, tableID string, record Record) {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)

	recordData := serializeRecord(&openDatabse, record)
	tablePointer, err := findTable(&openDatabse, tableID)
	check(err)
	addRecordToTableInternal(&openDatabse, *tablePointer, recordData)
}

func readAllRecords(db database, tableID string) []Record {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)

	tablePointer, err := findTable(&openDatabse, tableID)
	check(err)
	headers := parseTableHeaders(&openDatabse, *tablePointer)

	tableScheme := headers.scheme
	recordsData := readAllDataFromDbPointer(&openDatabse, headers.records.pointer)
	bitmapData := readAllDataFromDbPointer(&openDatabse, headers.bitmap.pointer)
	records := make([]Record, 0)
	sizeOfRecord := int(DB_POINTER_SIZE) * len(tableScheme.columns)
	for i := 0; i < len(recordsData)/sizeOfRecord; i++ {
		if checkBitFromData(bitmapData, i) {
			recordData := recordsData[i*sizeOfRecord : (i+1)*sizeOfRecord]
			records = append(records, deserializeRecord(&openDatabse, recordData, tableScheme))
		}
	}

	return records
}

func deleteRecord(db database, tableID string, recordIndex int) error {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)

	tablePointer, err := findTable(&openDatabse, tableID)
	check(err)
	headers := parseTableHeaders(&openDatabse, *tablePointer)

	if !checkBit(&openDatabse, headers.bitmap.pointer, recordIndex) {
		return &RecordNotFoundError{}
	}
	writeBitToBitmap(&openDatabse, headers.bitmap.location, uint32(recordIndex), 0)
	return nil
}
