package go_db

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

type testContext struct {
	db      database
	cursor  Cursor
	tableID string
}

func areFieldsEqual(field1 Field, field2 Field) bool {
	if field1.getType() != field2.getType() {
		return false
	}

	data1 := field1.serialize()
	data2 := field2.serialize()
	return bytes.Compare(data1, data2) == 0
}

func areRecordsEqual(record1 Record, record2 Record) bool {
	if len(record1.Fields) != len(record2.Fields) {
		return false
	}
	for i := 0; i < len(record1.Fields); i++ {
		if !areFieldsEqual(record1.Fields[i], record2.Fields[i]) {
			return false
		}
	}
	return true
}

func initializeTestDB(path string) (database, string) {
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	InitializeDB(path)
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	return db, tableID
}

func initializeTestDB1() (database, string) {
	path := "C:\\temp\\my_db"
	return initializeTestDB(path)
}

func buildTableWithDbPath1(dbPath string) (database, string) {
	db, tableID := initializeTestDB(dbPath)

	firstColumn := columndHeader{"columnA", FieldTypeInt}
	secondColumn := columndHeader{"columnB", FieldTypeBlob}
	scheme := tableScheme{[]columndHeader{firstColumn, secondColumn}}
	writeNewTable(db, tableID, scheme)
	fields := []Field{&IntField{5}, &BlobField{make([]byte, 10)}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{13}, &BlobField{make([]byte, 5000)}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	return db, tableID
}

func buildTable1() (database, string) {
	return buildTableWithDbPath1("C:\\temp\\my_db")
}

func buildTableWithDbPath2(dbPath string) (database, string) {
	db, tableID := initializeTestDB(dbPath)

	firstColumn := columndHeader{"columnA", FieldTypeInt}
	secondColumn := columndHeader{"columnB", FieldTypeInt}
	scheme := tableScheme{[]columndHeader{firstColumn, secondColumn}}
	writeNewTable(db, tableID, scheme)
	fields := []Field{&IntField{5}, &IntField{44}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{13}, &IntField{30}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	return db, tableID
}

func buildTable2() (database, string) {
	dbPath := "c:\\temp\\my_db"
	return buildTableWithDbPath2(dbPath)
}

func getConnectionTable2() (*testContext, error) {
	db, tableID := buildTable2()
	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath)
	if err != nil {
		return nil, err
	}
	cursor := conn.OpenCursor()
	return &testContext{db: db, cursor: cursor, tableID: tableID}, nil
}

func TestFullFlow(t *testing.T) {
	db, tableID := buildTable1()
	records := readAllRecords(db, tableID)
	if len(records) != 2 {
		t.Logf("Expected 2 records after insertion, found %d", len(records))
		t.Fail()
	}

	deleteRecord(db, tableID, 1)
	records = readAllRecords(db, tableID)
	if len(records) != 1 {
		t.Logf("Expected 1 record after deletion, found %d", len(records))
		t.Fail()
	}
}

func TestConditions(t *testing.T) {
	record := Record{[]Field{IntField{5}, IntField{7}, BlobField{[]byte{11, 13, 25}}}}
	node1 := conditionNode{}
	node1.condition = &condition{0, ConditionTypeEqual, uint32ToBytes(5)}
	// Condition should succeed
	if !checkAllConditions(node1, record) {
		t.Fail()
	}
	node2 := conditionNode{}
	node2.condition = &condition{1, ConditionTypeEqual, uint32ToBytes(6)}
	// Condition should fail
	if checkAllConditions(node2, record) {
		t.Fail()
	}
	node3 := conditionNode{}
	node3.operands = []*conditionNode{&node1, &node2}
	node3.operator = ConditionOperatorOr
	// Condition should succeed
	if !checkAllConditions(node3, record) {
		t.Fail()
	}
	node4 := conditionNode{}
	node4.condition = &condition{1, ConditionTypeGreater, uint32ToBytes(6)}
	if !checkAllConditions(node4, record) {
		t.Fail()
	}
	node5 := conditionNode{}
	node5.operands = []*conditionNode{&node4}
	node5.operator = ConditionOperatorNot
	if checkAllConditions(node5, record) {
		t.Fail()
	}
}

func buildConditionTreeForTest() conditionNode {
	node1 := conditionNode{condition: &condition{0, ConditionTypeEqual, uint32ToBytes(5)}}
	node2 := conditionNode{condition: &condition{1, ConditionTypeEqual, uint32ToBytes(44)}}
	return conditionNode{operator: ConditionOperatorAnd, operands: []*conditionNode{&node1, &node2}}
}

func buildConditionTreeForTestAbove500OrBelow100() conditionNode {
	node1 := conditionNode{condition: &condition{0, ConditionTypeGreater, uint32ToBytes(500)}}
	node2 := conditionNode{condition: &condition{1, ConditionTypeLess, uint32ToBytes(100)}}
	return conditionNode{operator: ConditionOperatorOr, operands: []*conditionNode{&node1, &node2}}
}

func TestRecordFilter(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	records, err := filterRecordsFromTable(db, tableID, &cond)
	if err != nil {
		t.Fail()
	}

	if len(records) != 1 {
		t.Fail()
	}
}

func printConditionNode(node conditionNode, indent int) {
	indentString := ""
	for i := 0; i < indent; i++ {
		indentString += " "
	}
	if node.operator != ConditionOperatorNull {
		fmt.Println(indentString+"operator=", node.operator)
		for i := 0; i < len(node.operands); i++ {
			printConditionNode(*node.operands[i], indent+2)
		}
	} else {
		fmt.Println(indentString+"condition=",
			node.condition.fieldIndex, node.condition.conditionType)
	}
}

func TestDeleteRecordsFromTable(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	records := readAllRecords(db, tableID)
	// Sanity
	if len(records) != 2 {
		t.Fail()
	}
	deleteRecordsFromTable(db, tableID, &cond)

	// Now make sure records were deleted as expected
	records = readAllRecords(db, tableID)
	if len(records) != 1 {
		t.Fail()
	}
	record := records[0]
	firstField, ok := record.Fields[0].(IntField)
	if !ok {
		t.Fail()
	}
	if firstField.Value != 13 {
		t.Fail()
	}
	secondField, ok := record.Fields[1].(IntField)
	if !ok {
		t.Fail()
	}
	if secondField.Value != 30 {
		t.Fail()
	}
}

func TestParseSelectQuery1(t *testing.T) {
	db, _ := buildTable2()
	openDB := getOpenDB(db)
	defer closeOpenDB(&openDB)

	sql := "select columna, columnb from newTable where ((columna = 5) and (columnb = 13)) or ((columna = 7) and (not (columnb = 30)))"
	_, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}
}

func TestParseSelectQuery2(t *testing.T) {
	db, _ := buildTable2()
	openDB := getOpenDB(db)
	defer closeOpenDB(&openDB)

	sql := "select columna, columnb from newTable where columna = 7 and columnb = 13 or columna = 5 and columna = 10 and not columnb = 5"
	_, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}
}

func TestDivideStatementByParentheses(t *testing.T) {
	sql := "((columnA = 5) and (columnB = 13)) or columnA = 15 and columnB = 37 or ((columnA = 7) and (not (columnB = 30)))"
	indexes, err := divideStatementByParentheses(sql, 0, len(sql))
	if err != nil {
		t.Fail()
	}

	if len(indexes) != 3 {
		t.Fail()
	}
	assert(indexes[0].parentheses, "Wrong first interval")
	assert(!indexes[1].parentheses, "Wrong second interval")
	assert(indexes[2].parentheses, "Wrong third interval")
}

// e2e test made to check whether a simple select query works via the DB's cursor
func TestCursorSelect1(t *testing.T) {
	db, _ := buildTable2()
	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()
	err = cursor.Execute("Select columnA, ColumnB from newTable where columnA = 5")
	if err != nil {
		t.Fail()
	}
	records := cursor.FetchAll()
	if len(records) != 1 {
		t.Fail()
	}
	record := records[0]
	firstField := record.Fields[0]
	intField, ok := firstField.(IntField)
	if !ok {
		t.Fail()
	}
	if intField.Value != 5 {
		t.Fail()
	}
}

func TestCursorSelect2(t *testing.T) {
	db, tableID := buildTable2()
	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()
	regulardOrderRecords := readAllRecords(db, tableID)

	// sanity
	if len(regulardOrderRecords) != 2 {
		t.Fail()
	}

	err = cursor.Execute("Select columnA, ColumnB from newTable order by columnB")
	if err != nil {
		t.Fail()
	}
	records := cursor.FetchAll()
	if len(records) != 2 {
		t.Fail()
	}

	// We expect the order of the records to be reversed
	if !areRecordsEqual(regulardOrderRecords[0], records[1]) ||
		!areRecordsEqual(regulardOrderRecords[1], records[0]) {
		t.Fail()
	}
}

func TestCursorInsert1(t *testing.T) {
	db, _ := buildTable2()
	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()
	err = cursor.Execute("insert into newTable values (13, 32), (41, 55), (37, 38)")
	if err != nil {
		t.Fail()
	}
	cursor.Execute("select columnA, columnB from newTable")

	// The condition refers to a record we've just now added
	err = cursor.Execute("Select columnA, ColumnB from newTable where columnA = 37")
	if err != nil {
		t.Fail()
	}

	records := cursor.FetchAll()
	if len(records) != 1 {
		t.Fail()
	}
}

func TestCursorDelete1(t *testing.T) {
	context, err := getConnectionTable2()
	if err != nil {
		t.Fail()
	}
	records := readAllRecords(context.db, context.tableID)
	// sanity
	if len(records) != 2 {
		t.Fail()
	}

	// remove one record
	err = context.cursor.Execute("delete from newTable where columnB = 30")
	if err != nil {
		t.Fail()
	}

	records = readAllRecords(context.db, context.tableID)
	if len(records) != 1 {
		t.Fail()
	}
}

func TestCursorCreateInsertSelect1(t *testing.T) {
	db, _ := initializeTestDB1()
	connection, err := Connect(db.id.identifyingString)
	if err != nil {
		t.Fail()
	}

	cursor := connection.OpenCursor()

	cursor.Execute("create table newTable (columnA int, columnB int)")
	cursor.Execute("insert into newTable values (13, 32), (41, 55), (37, 38)")
	cursor.Execute("select columnA, columnB from newTable")
	records := cursor.FetchAll()
	if len(records) != 3 {
		t.Fail()
	}
}

func TestCursorCreateInsertSelect2(t *testing.T) {
	db, _ := initializeTestDB1()
	connection, err := Connect(db.id.identifyingString)
	if err != nil {
		t.Fail()
	}

	cursor := connection.OpenCursor()

	cursor.Execute("create table newTable (columnA blob, columnB int)")
	cursor.Execute("insert into newTable values (ab34ffffffff, 32), (0000, 55), (abcdef, 38)")
	cursor.Execute("select columnA, columnB from newTable")
	records := cursor.FetchAll()
	if len(records) != 3 {
		t.Fail()
	}
}

func TestUpdateRecordsViaCondition(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	recordsBefore := readAllRecords(db, tableID)
	// Sanity
	if len(recordsBefore) != 2 {
		t.FailNow()
	}

	update := recordUpdate{[]recordChange{{fieldIndex: 0, newData: []byte{1, 0, 0, 0}}}}
	openDatabase := getOpenDB(db)
	err := updateRecordsViaCondition(&openDatabase, tableID, &cond, update)
	closeOpenDB(&openDatabase)
	if err != nil {
		t.Fail()
	}

	recordsAfter := readAllRecords(db, tableID)
	// Sanity
	if len(recordsAfter) != 2 {
		t.Fail()
	}

	// Make sure only the expected change occur, and all else remained the same
	if !bytes.Equal(recordsAfter[0].Fields[0].serialize(), []byte{1, 0, 0, 0}) {
		t.Fail()
	}
	if !bytes.Equal(recordsAfter[0].Fields[1].serialize(), recordsBefore[0].Fields[1].serialize()) {
		t.Fail()
	}
	if !bytes.Equal(recordsAfter[1].Fields[0].serialize(), recordsBefore[1].Fields[0].serialize()) {
		t.Fail()
	}
	if !bytes.Equal(recordsAfter[1].Fields[1].serialize(), recordsBefore[1].Fields[1].serialize()) {
		t.Fail()
	}
}

func TestCursorUpdate1(t *testing.T) {
	db, tableID := buildTable2()
	dbPath := db.id.identifyingString
	recordsBefore := readAllRecords(db, tableID)
	if len(recordsBefore) != 2 {
		t.Fail()
	}

	conn, err := Connect(dbPath)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()
	err = cursor.Execute("update newTable set columnA=33,columnB=89 where columnA=5")
	if err != nil {
		t.Fail()
	}

	recordsAfter := readAllRecords(db, tableID)
	// Sanity
	if len(recordsAfter) != 2 {
		t.Fail()
	}

	if !areRecordsEqual(recordsBefore[1], recordsAfter[1]) {
		t.Fail() // unintended change
	}

	expectedFirstRecord := Record{[]Field{IntField{33}, IntField{89}}}
	if !areRecordsEqual(expectedFirstRecord, recordsAfter[0]) {
		t.Fail()
	}
}

func TestCannotCreateTheSameTableTwice(t *testing.T) {
	db, tableID := buildTable1()
	openDatabase := getOpenDB(db)
	headers, err := getTableHeaders(&openDatabase, tableID)
	closeOpenDB(&openDatabase)
	if err != nil {
		t.Fail()
	}

	err = writeNewTable(db, tableID, headers.scheme)
	if err == nil {
		// This should fail, as we've already created this table
		t.Fail()
	}
}

// Add enough data blocks to the database to make the free block bitmap to expand.
// Make sure the bitmap is in the exact size we'd expect it to be
func TestBitmapExpansion(t *testing.T) {
	db, _ := initializeTestDB1()
	ALREADY_USED_DATA_BLOCKS := 3
	openDatabase := getOpenDB(db)
	BLOCKS_TO_ADD := (int(openDatabase.header.dataBlockSize)-4)*8 - ALREADY_USED_DATA_BLOCKS

	for i := 0; i < BLOCKS_TO_ADD; i++ {
		allocateNewDataBlock(&openDatabase)
	}

	allocateNewDataBlock(&openDatabase)

	bitmapData := readAllDataFromDbPointer(&openDatabase, openDatabase.header.bitmapPointer)
	if len(bitmapData) != (int(openDatabase.header.dataBlockSize)-4)+1 {
		t.Fail()
	}
	for _, b := range bitmapData[:(int(openDatabase.header.dataBlockSize) - 4)] {
		// All bytes but the last should be all ones
		if b != 0xff {
			t.Fail()
		}
	}
	if bitmapData[(int(openDatabase.header.dataBlockSize)-4)] != 0x3 {
		// We add one additional datablock after there's no more room left in the first datablock
		// of the free block bitmap. This should cause two additional blocks to be created:
		// The first is for the free blocks bitmap itself
		// The second is the block we've requested to add
		t.Fail()
	}
}

func TestMapEachRecord(t *testing.T) {
	db, tableID := buildTableWithDbPath2(IN_MEMORY_BUFFER_PATH_MAGIC)

	// Delete all previous records
	err := deleteRecordsFromTable(db, tableID, nil)
	if err != nil {
		t.Fail()
	}

	for i := 0; i < 1000; i++ {
		fields := []Field{&IntField{i}, &IntField{i + 5}}
		record := MakeRecord(fields)
		addRecordToTable(db, tableID, record)
	}

	// Sanity
	records := readAllRecords(db, tableID)
	if len(records) != 1000 {
		t.Fail()
	}

	cond := buildConditionTreeForTestAbove500OrBelow100()
	openDatabase := getOpenDB(db)
	buildConditionTreeForTest()
	output, err := mapEachRecord(&openDatabase, tableID, &cond, mapGetRecords, nil)
	if err != nil {
		t.Fail()
	}

	// check the amount of records we get back is as expected
	if len(output) != 499+95 {
		t.Fail()
	}
}

func TestAddAlotOfRecords1(t *testing.T) {
	db, tableID := buildTableWithDbPath1(IN_MEMORY_BUFFER_PATH_MAGIC)

	// Delete all previous records
	err := deleteRecordsFromTable(db, tableID, nil)
	if err != nil {
		t.Fail()
	}

	for i := 0; i < 10000; i++ {
		fields := []Field{&IntField{i}, &BlobField{[]byte{1, 2, 3, 4, 5}}}
		record := MakeRecord(fields)
		addRecordToTable(db, tableID, record)
	}

	// Sanity
	records := readAllRecords(db, tableID)
	if len(records) != 10000 {
		t.Fail()
	}

	for i := range records {
		if !areRecordsEqual(
			records[i], Record{[]Field{&IntField{i}, &BlobField{[]byte{1, 2, 3, 4, 5}}}}) {
			t.Fail()
		}
	}
}

func TestStringField(t *testing.T) {
	db, tableID := initializeTestDB1()
	firstColumn := columndHeader{"stringColumn1", FieldTypeString}
	secondColumn := columndHeader{"stringColumn2", FieldTypeString}
	scheme := tableScheme{[]columndHeader{firstColumn, secondColumn}}
	writeNewTable(db, tableID, scheme)

	record1 := Record{[]Field{StringField{"Hello world"}, StringField{"goodbye world"}}}
	record2 := Record{[]Field{StringField{"A string"}, StringField{"another string"}}}
	record3 := Record{[]Field{StringField{"Final record"}, StringField{"This is a record"}}}
	recordsToAdd := []Record{record1, record2, record3}
	for i := 0; i < len(recordsToAdd); i++ {
		addRecordToTable(db, tableID, recordsToAdd[i])
	}

	records := readAllRecords(db, tableID)
	// Sanity
	if len(records) != len(recordsToAdd) {
		t.Fail()
	}

	for i := 0; i < len(recordsToAdd); i++ {
		if !areRecordsEqual(records[i], recordsToAdd[i]) {
			t.Fail()
		}
	}
}

// Simple sanity test - make sure we can initialize a table and then read its
// contents
func TestInMemoryBuffer(t *testing.T) {
	db, tableID := buildTableWithDbPath1(IN_MEMORY_BUFFER_PATH_MAGIC)
	records := readAllRecords(db, tableID)
	if len(records) != 2 {
		t.Fail()
	}
}
