package go_db

import (
	"encoding/binary"
	"fmt"
	"strings"
)

type recordsCallbackFunc[contextType any] func(record Record, index int, context contextType)

type tableScheme struct {
	columns     []columnHeader
	provColumns []columnHeader
}

type partialRecordOffsets struct {
	columns     []uint32
	provenances []uint32
}

// returns the amount of expected field in each record of the table
func (scheme *tableScheme) fieldsInRecord() int {
	return len(scheme.columns) + len(scheme.provColumns)
}

func filterIndexedFields(columns []columnHeader) []uint32 {
	indexed := make([]uint32, 0)
	for i := 0; i < len(columns); i++ {
		if columns[i].index != nil {
			indexed = append(indexed, uint32(i))
		}
	}
	return indexed
}

func (scheme *tableScheme) indexedColumns() []uint32 {
	return filterIndexedFields(scheme.columns)
}

func (scheme *tableScheme) indexedFields() partialRecordOffsets {
	columns := scheme.indexedColumns()
	provenances := filterIndexedFields(scheme.provColumns)
	return partialRecordOffsets{columns: columns, provenances: provenances}
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

// returns the offset in the records table of a specific record's field
func (headers *tableHeaders) fieldOffset(recordIndex int, fieldIndex int) int {
	if fieldIndex >= headers.scheme.fieldsInRecord() {
		return -1
	}
	return int(DB_POINTER_SIZE) * (headers.scheme.fieldsInRecord()*recordIndex + fieldIndex)
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

type innerTableRecordIterator struct {
	db             *openDB
	headers        *tableHeaders
	bitmapData     []byte
	recordsPointer dbPointer
	sizeOfRecord   uint32
	offset         uint32
}

type tableCurrentRecord struct {
	record Record
	offset uint32
}

func (iterator *innerTableRecordIterator) done() bool {
	return iterator.offset >= iterator.recordsPointer.size/iterator.sizeOfRecord
}

func (iterator *innerTableRecordIterator) next() *tableCurrentRecord {
	for !iterator.done() && !checkBitFromData(iterator.bitmapData, int(iterator.offset)) {
		iterator.offset++
	}

	if iterator.done() {
		return nil
	}

	recordData := readFromDbPointer(iterator.db, iterator.recordsPointer, iterator.sizeOfRecord,
		iterator.sizeOfRecord*iterator.offset)
	record := deserializeRecord(iterator.db, recordData, iterator.headers.scheme)
	recordOffset := iterator.offset
	iterator.offset++

	return &tableCurrentRecord{record: record, offset: recordOffset}
}

func (iterator *innerTableRecordIterator) reset() {
	iterator.offset = 0
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
	for _, columnHeader := range scheme.columns {
		schemeData = append(schemeData, serializeColumnHeader(columnHeader)...)
	}

	for _, columnHeader := range scheme.provColumns {
		schemeData = append(schemeData, serializeColumnHeader(columnHeader)...)
	}

	// Put the size of the scheme data at the start of it
	binary.LittleEndian.PutUint32(schemeData[:4], uint32(len(schemeData)-4))
	appendDataToDataBlock(db, schemeData, uint32(mutablePointer.location))
	return uint32(len(schemeData))
}

func parseTableScheme(db *openDB, schemeData []byte) tableScheme {
	scheme := tableScheme{}
	var header columnHeader
	for i := 0; i < len(schemeData); {
		header, i = deserializeColumnHeader(db, schemeData, i)
		if header.columnType == FieldTypeProvenance {
			scheme.provColumns = append(scheme.provColumns, header)
		} else {
			scheme.columns = append(scheme.columns, header)
		}
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

func writeNewTable(openDatabase *openDB, tableID string, scheme tableScheme) error {
	_, err := findTable(openDatabase, tableID)
	if _, ok := err.(*TableNotFoundError); !ok {
		return fmt.Errorf("table %s already exists", tableID)
	}

	if len(scheme.provColumns) == 0 {
		// add DB provenance to table's scheme
		scheme.provColumns = openDatabase.provenanceSchemeColumns()
	}

	// index all provenance columns
	for i := 0; i < len(scheme.provColumns); i++ {
		initializeIndexInColumn(openDatabase, scheme.provColumns, i)
	}

	newTablePointer := allocateNewDataBlock(openDatabase)
	mutablePointer := addNewTableToTablesArray(openDatabase, newTablePointer)
	initializeNewTableContent(openDatabase, tableID, scheme, &mutablePointer)

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

func writeRecordToTable(db *openDB, headers tableHeaders, recordIndex uint32, record Record) error {
	// Find the offset of the new record
	// TODO: handle data block extension
	data := serializeRecord(db, record)

	recordsPointer := headers.records
	prevAmountOfRecords := recordsPointer.pointer.size /
		(DB_POINTER_SIZE * uint32(headers.scheme.fieldsInRecord()))
	if prevAmountOfRecords > recordIndex {
		// Override pre-existing invalid record
		offset := headers.fieldOffset(int(recordIndex), 0)
		writeToDataBlock(db, recordsPointer.pointer, data, uint32(offset))
	} else {
		// Write a new record
		appendDataToDataBlock(db, data, uint32(recordsPointer.location))
	}

	return addRecordToIndexes(db, &headers.scheme, record, recordIndex)
}

func parseTableHeaders(db *openDB, tablePointer dbPointer) tableHeaders {
	uniqueIDSize := binary.LittleEndian.Uint32(readFromDB(db, 4, tablePointer.offset))
	schemeOffset := tablePointer.offset + 4 + uniqueIDSize
	schemeSize := binary.LittleEndian.Uint32(readFromDB(db, 4, schemeOffset))
	schemeData := readFromDB(db, schemeSize, schemeOffset+4)
	scheme := parseTableScheme(db, schemeData)
	bitmapOffset := schemeOffset + 4 + schemeSize
	bitmapPointer := getDbPointer(db, bitmapOffset)
	recordsPointerOffset := bitmapOffset + DB_POINTER_SIZE
	recordsPointer := getDbPointer(db, recordsPointerOffset)
	return tableHeaders{scheme: scheme,
		bitmap:  mutableDbPointer{pointer: bitmapPointer, location: int64(bitmapOffset)},
		records: mutableDbPointer{pointer: recordsPointer, location: int64(recordsPointerOffset)},
	}
}

func addRecordToTableInternal(db *openDB, tablePointer dbPointer, record Record) (uint32, error) {
	headers := parseTableHeaders(db, tablePointer)
	addProvenanceToRecord(db, &record)
	bitmapData := readAllDataFromDbPointer(db, headers.bitmap.pointer)
	firstAvailableRecordNum := findFirstAvailableBlock(bitmapData)

	err := writeRecordToTable(db, headers, firstAvailableRecordNum, record)
	if err != nil {
		return 0, err
	}

	writeBitToBitmap(db, int64(headers.bitmap.location), firstAvailableRecordNum, 1)

	return firstAvailableRecordNum, nil
}

// returns the record index in the table
func addRecordToTable(db *openDB, tableID string, record Record) (uint32, error) {
	tablePointer, err := findTable(db, tableID)
	check(err)
	return addRecordToTableInternal(db, *tablePointer, record)
}

func getRecordFromOffset(db *openDB, headers *tableHeaders, offset uint32) Record {
	tableScheme := headers.scheme
	sizeOfRecord := int(DB_POINTER_SIZE) * tableScheme.fieldsInRecord()

	recordData := readFromDbPointer(db, headers.records.pointer,
		uint32(sizeOfRecord), offset*uint32(sizeOfRecord))
	return deserializeRecord(db, recordData, headers.scheme)
}

func readAllRecords(db *openDB, tableID string) []Record {
	tablePointer, err := findTable(db, tableID)
	check(err)
	headers := parseTableHeaders(db, *tablePointer)

	tableScheme := headers.scheme
	bitmapData := readAllDataFromDbPointer(db, headers.bitmap.pointer)
	records := make([]Record, 0)
	sizeOfRecord := int(DB_POINTER_SIZE) * tableScheme.fieldsInRecord()
	for i := 0; i < int(headers.records.pointer.size)/sizeOfRecord; i++ {
		if checkBitFromData(bitmapData, i) {
			record := getRecordFromOffset(db, &headers, uint32(i))
			records = append(records, record)
		}
	}

	return records
}

func deleteRecordInternal(db *openDB, headers tableHeaders, record *recordForChange) error {
	if !checkBit(db, headers.bitmap.pointer, int(record.index)) {
		return &RecordNotFoundError{}
	}
	writeBitToBitmap(db, headers.bitmap.location, record.index, 0)
	return removeRecordFromIndexes(db, record, &headers.scheme)
}

func getTableHeaders(db *openDB, tableID string) (*tableHeaders, error) {
	tablePointer, err := findTable(db, tableID)
	if err != nil {
		return nil, err
	}

	headers := parseTableHeaders(db, *tablePointer)
	return &headers, nil
}

func deleteRecord(db *openDB, tableID string, recordIndex uint32) error {
	headers, err := getTableHeaders(db, tableID)
	if err != nil {
		return err
	}

	indexedFields := headers.scheme.indexedFields()
	partialRecord := MakeEmptyRecord()
	if len(indexedFields.columns) > 0 || len(indexedFields.provenances) > 0 {
		record := getRecordFromOffset(db, headers, recordIndex)
		partialRecord = getPartialRecord(&record, indexedFields)
	}

	return deleteRecordInternal(db, *headers,
		&recordForChange{index: recordIndex, partialRecord: partialRecord})
}

func validateConditions(columns []columnHeader, recordsCondition conditionNode) bool {
	if recordsCondition.condition != nil {
		cond := recordsCondition.condition
		if cond.leftOperand.fieldIndex != nil && int(*cond.leftOperand.fieldIndex) >= len(columns) {
			return false
		}
		if cond.rightOperand.fieldIndex != nil && int(*cond.rightOperand.fieldIndex) >= len(columns) {
			return false
		}

		return true
	} else {
		result := true
		for _, operand := range recordsCondition.operands {
			result = result && validateConditions(columns, *operand)
		}
		return result
	}
}

func validateConditionsJointTable(db *openDB, tableIDs []string, conditions *conditionNode) (bool, error) {
	if conditions == nil {
		return true, nil
	}

	columnHeaders := make([]columnHeader, 0)
	for _, tableID := range tableIDs {
		h, err := getTableHeaders(db, tableID)
		if err != nil {
			return false, fmt.Errorf("failed to open table %s", tableID)
		}

		columnHeaders = append(columnHeaders, h.scheme.columns...)
	}

	return validateConditions(columnHeaders, *conditions), nil
}

func initializeInternalRecordIterator(openDatabase *openDB, tableID string) (*innerTableRecordIterator, error) {
	tablePointer, err := findTable(openDatabase, tableID)
	if err != nil {
		return nil, err
	}
	headers := parseTableHeaders(openDatabase, *tablePointer)

	bitmapData := readAllDataFromDbPointer(openDatabase, headers.bitmap.pointer)
	recordsPointer := headers.records
	sizeOfRecord := int(DB_POINTER_SIZE) * headers.scheme.fieldsInRecord()

	return &innerTableRecordIterator{db: openDatabase, headers: &headers, bitmapData: bitmapData,
		recordsPointer: recordsPointer.pointer, sizeOfRecord: uint32(sizeOfRecord), offset: 0}, nil
}

func filterRecordsFromTables(openDatabase *openDB, tableIDs []string, recordsCondition *conditionNode, columns []uint32) ([]Record, error) {
	records, err := mapEachRecord(openDatabase, tableIDs, recordsCondition, mapGetRecords, columns)
	if err != nil {
		return nil, err
	}

	// set semantics - remove duplications from result
	uniqueRecords := removeRecordDuplications(records)
	return uniqueRecords, nil
}

func deleteRecordsFromTable(db *openDB, tableID string, recordsCondition *conditionNode) error {
	headers, err := getTableHeaders(db, tableID)
	if err != nil {
		return err
	}

	indexedFields := headers.scheme.indexedFields()

	recordsToDelete, err := mapEachRecord(db, []string{tableID}, recordsCondition, mapGetRecordForChange, indexedFields)
	if err != nil {
		return err
	}

	for _, recordForDeletion := range recordsToDelete {
		deleteRecordInternal(db, *headers, &recordForDeletion)
	}
	return nil
}

func updateField(db *openDB, headers tableHeaders, offset uint32, change recordChange) error {
	fieldData := readFromDbPointer(db, headers.records.pointer, DB_POINTER_SIZE, offset)
	fieldPointer := deserializeDbPointer(fieldData)
	if fieldPointer.offset != 0 {
		// Field contains a db pointer, actual data is in its own data block
		// Deallocate this data block
		err := deallocateDbPointer(db, fieldPointer)
		if err != nil {
			return err
		}
	}

	// Write new data
	newFieldData := serializeField(db, change.newData)
	writeToDataBlock(db, headers.records.pointer, newFieldData, offset)
	return nil
}

func updateRecord(db *openDB, headers tableHeaders, record recordForChange, update recordUpdate,
	keysChanged []uint32) error {
	recordIndex := int(record.index)
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

func updateRecords(db *openDB, headers tableHeaders, records []recordForChange, update recordUpdate,
	keysChanged []uint32) error {
	for _, record := range records {
		err := updateRecord(db, headers, record, update, keysChanged)
		if err != nil {
			return err
		}
	}
	return nil
}

func columnsIntersection(left []uint32, right []uint32) []uint32 {
	res := make([]uint32, 0)

	// do it in O(n^2) since this is much more readable than using sets and we expect the amount of columns
	// to be very small (unlikely this will result in more than 100 iterations...)
	for i := 0; i < len(left); i++ {
		for j := 0; j < len(right); j++ {
			if left[i] != right[j] {
				continue
			}
			res = append(res, left[i])
			break
		}
	}

	return res
}

func updateRecordsViaCondition(db *openDB, tableID string, condition *conditionNode, update recordUpdate) error {
	tableHeaders, err := getTableHeaders(db, tableID)
	if err != nil {
		return err
	}

	// initialize the structure indicating which fields to update with all columns/prov columns that
	// has an index.
	// Then later remove all columns that have not been changed.
	// We update all provenance columns because they might've been changed during this update.
	changedFieldOffsets := tableHeaders.scheme.indexedFields()

	columnsChanged := make([]uint32, 0)
	for i := 0; i < len(update.changes); i++ {
		columnsChanged = append(columnsChanged, uint32(update.changes[i].fieldIndex))
	}
	changedFieldOffsets.columns = columnsIntersection(changedFieldOffsets.columns, columnsChanged)

	records, err := mapEachRecord(db, []string{tableID}, condition, mapGetRecordForChange, changedFieldOffsets)
	if err != nil {
		return err
	}

	// TODO: pass provenance data to updateRecord to update the provenance as well
	err = updateRecords(db, *tableHeaders, records, update, changedFieldOffsets.columns)
	if err != nil {
		return err
	}
	return nil
}

func makeTableScheme(headers []columnHeader) tableScheme {
	return tableScheme{columns: headers, provColumns: make([]columnHeader, 0)}
}
