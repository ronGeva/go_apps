package go_db

import (
	"encoding/binary"
	"fmt"
	"strings"
)

type recordsCallbackFunc[contextType any] func(record Record, index int, context contextType)

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

func (headers *tableHeaders) fieldOffset(recordIndex int, fieldIndex int) int {
	if fieldIndex >= len(headers.scheme.columns) {
		return -1
	}
	return int(DB_POINTER_SIZE) * (len(headers.scheme.columns)*recordIndex + fieldIndex)
}

type recordChange struct {
	fieldIndex int
	newData    []byte
}

type recordUpdate struct {
	changes []recordChange
}

type recordContext struct {
	record Record
	index  uint32
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

func writeNewTableLocalFileInternal(openDatabase *openDB, tableID string, scheme tableScheme) {
	newTablePointer := allocateNewDataBlock(openDatabase)
	mutablePointer := addNewTableToTablesArray(openDatabase, newTablePointer)
	initializeNewTableContent(openDatabase, tableID, scheme, &mutablePointer)
}

func writeNewTableLocalFile(db database, tableID string, scheme tableScheme) {
	openDatabase := getOpenDB(db)
	defer closeOpenDB(&openDatabase)
	writeNewTableLocalFileInternal(&openDatabase, tableID, scheme)
}

func writeNewTable(db database, tableID string, scheme tableScheme) error {
	if db.id.ioType != LocalFile {
		panic(UnsupportedError{})
	}
	openDatabase := getOpenDB(db)
	defer closeOpenDB(&openDatabase)

	_, err := findTable(&openDatabase, tableID)
	if _, ok := err.(*TableNotFoundError); !ok {
		return fmt.Errorf("table %s already exists", tableID)
	}

	writeNewTableLocalFileInternal(&openDatabase, tableID, scheme)

	return nil
}

func findTable(openDatabse *openDB, tableID string) (*dbPointer, error) {
	tablesPointer := getDbPointer(openDatabse, TABLES_POINTER_OFFSET)
	tablesArrayBytes := readAllDataFromDbPointer(openDatabse, tablesPointer)
	for i := 0; i < len(tablesArrayBytes)/int(DB_POINTER_SIZE); i++ {
		currPointer := deserializeDbPointer(
			tablesArrayBytes[i*int(DB_POINTER_SIZE) : (i+1)*int(DB_POINTER_SIZE)])
		uniqueID := readVariableSizeDataFromDB(openDatabse, currPointer.offset)
		if strings.EqualFold(string(uniqueID), tableID) {
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

func addRecordOpenDb(db *openDB, tableID string, record Record) {
	recordData := serializeRecord(db, record)
	tablePointer, err := findTable(db, tableID)
	check(err)
	addRecordToTableInternal(db, *tablePointer, recordData)
}

func addRecordToTable(db database, tableID string, record Record) {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)
	addRecordOpenDb(&openDatabse, tableID, record)
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

func deleteRecordInternal(db *openDB, headers tableHeaders, recordIndex uint32) error {
	if !checkBit(db, headers.bitmap.pointer, int(recordIndex)) {
		return &RecordNotFoundError{}
	}
	writeBitToBitmap(db, headers.bitmap.location, recordIndex, 0)
	return nil
}

func getTableHeaders(db *openDB, tableID string) (*tableHeaders, error) {
	tablePointer, err := findTable(db, tableID)
	if err != nil {
		return nil, err
	}

	headers := parseTableHeaders(db, *tablePointer)
	return &headers, nil
}

func deleteRecord(db database, tableID string, recordIndex uint32) error {
	openDatabse := getOpenDB(db)
	defer closeOpenDB(&openDatabse)

	headers, err := getTableHeaders(&openDatabse, tableID)
	if err != nil {
		return err
	}

	return deleteRecordInternal(&openDatabse, *headers, recordIndex)
}

func validateConditions(scheme tableScheme, recordsCondition conditionNode) bool {
	if recordsCondition.condition != nil {
		cond := recordsCondition.condition
		if int(cond.fieldIndex) >= len(scheme.columns) {
			return false
		}
		if !isConditionSupported(scheme, cond) {
			return false
		}
		return true
	} else {
		result := true
		for _, operand := range recordsCondition.operands {
			result = result && validateConditions(scheme, *operand)
		}
		return result
	}
}

func writeAllRecordsToChannel(openDatabase *openDB, tableID string, recordsCondition *conditionNode,
	recordsChannel chan<- recordContext) error {
	tablePointer, err := findTable(openDatabase, tableID)
	if err != nil {
		return err
	}
	headers := parseTableHeaders(openDatabase, *tablePointer)
	if recordsCondition != nil && !validateConditions(headers.scheme, *recordsCondition) {
		return fmt.Errorf("invalid condition in query")
	}

	bitmapData := readAllDataFromDbPointer(openDatabase, headers.bitmap.pointer)
	scheme := headers.scheme
	recordsPointer := headers.records
	sizeOfRecord := int(DB_POINTER_SIZE) * len(headers.scheme.columns)
	for i := 0; i < int(recordsPointer.pointer.size)/sizeOfRecord; i++ {
		if checkBitFromData(bitmapData, i) {
			recordData := readFromDbPointer(openDatabase, recordsPointer.pointer, uint32(sizeOfRecord),
				uint32(sizeOfRecord*i))
			record := deserializeRecord(openDatabase, recordData, scheme)
			recordsChannel <- recordContext{record: record, index: uint32(i)}
		}
	}
	return nil
}

func filterRecordsFromTableInternal(openDatabase *openDB, tableID string, recordsCondition *conditionNode, columns []uint32) ([]Record, error) {
	records, err := mapEachRecord(openDatabase, tableID, recordsCondition, mapGetRecords, columns)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func deleteRecordsFromTableInternal(db *openDB, tableID string, recordsCondition *conditionNode) error {
	recordsToDelete, err := mapEachRecord(db, tableID, recordsCondition, mapGetRecordIndexes, nil)
	if err != nil {
		return err
	}

	headers, err := getTableHeaders(db, tableID)
	if err != nil {
		return err
	}
	for _, index := range recordsToDelete {
		deleteRecordInternal(db, *headers, index)
	}
	return nil
}

func deleteRecordsFromTable(db database, tableID string, recordsCondition *conditionNode) error {
	openDatabase := getOpenDB(db)
	defer closeOpenDB(&openDatabase)

	return deleteRecordsFromTableInternal(&openDatabase, tableID, recordsCondition)
}

func filterRecordsFromTable(db database, tableID string, recordsCondition *conditionNode) ([]Record, error) {
	openDatabase := getOpenDB(db)
	defer closeOpenDB(&openDatabase)

	return filterRecordsFromTableInternal(&openDatabase, tableID, recordsCondition, nil)
}

func updateField(db *openDB, headers tableHeaders, offset uint32, change recordChange) error {
	fieldData := readFromDbPointer(db, headers.records.pointer, DB_POINTER_SIZE, offset)
	fieldPointer := deserializeDbPointer(fieldData)
	fieldMutablePointer := mutableDbPointer{pointer: fieldPointer, location: int64(offset)}
	if fieldPointer.offset != 0 {
		// Field contains a db pointer, actual data is in its own data block
		// Deallocate this data block
		err := deallocateDbPointer(db, fieldMutablePointer)
		if err != nil {
			return err
		}
	}

	// Write new data
	newFieldData := serializeField(db, change.newData)
	writeToDataBlock(db, headers.records.pointer, newFieldData, offset)
	return nil
}

func updateRecord(db *openDB, headers tableHeaders, recordIndex int, update recordUpdate) error {
	if !checkBit(db, headers.bitmap.pointer, recordIndex) {
		return fmt.Errorf("cannot update an invalid record index %d", recordIndex)
	}
	for _, change := range update.changes {
		offset := uint32(headers.fieldOffset(recordIndex, change.fieldIndex))
		err := updateField(db, headers, offset, change)
		if err != nil {
			return err
		}
	}

	return nil
}

func updateRecords(db *openDB, headers tableHeaders, recordIndexes []uint32, update recordUpdate) error {
	for _, recordIndex := range recordIndexes {
		err := updateRecord(db, headers, int(recordIndex), update)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateRecordsViaCondition(db *openDB, tableID string, condition *conditionNode, update recordUpdate) error {
	tableHeaders, err := getTableHeaders(db, tableID)
	if err != nil {
		return err
	}

	recordIndexes, err := mapEachRecord(db, tableID, condition, mapGetRecordIndexes, nil)
	if err != nil {
		return err
	}

	err = updateRecords(db, *tableHeaders, recordIndexes, update)
	if err != nil {
		return err
	}
	return nil
}
