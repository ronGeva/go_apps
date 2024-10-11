package go_db

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/ronGeva/go_apps/b_tree"
)

type testContext struct {
	db      database
	cursor  Cursor
	tableID string
}

type testTable struct {
	name    string
	records []Record
	scheme  tableScheme
}

func initializeTestDbInternal(path string, provenance bool) (database, string) {
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	InitializeDB(path, provenance)
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	return db, tableID
}

func initializeTestDB(path string) (database, string) {
	return initializeTestDbInternal(path, false)
}

func initializeTestDB1() (database, string) {
	path := "C:\\temp\\my_db"
	return initializeTestDB(path)
}

func testTable1Scheme() tableScheme {
	firstColumn := columnHeader{"columnA", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"columnB", FieldTypeBlob, nil, dbPointer{0, 0}}
	return makeTableScheme([]columnHeader{firstColumn, secondColumn})
}

func writeTestTable1(db database, table string) {
	scheme := testTable1Scheme()
	err := writeNewTable(db, table, scheme)
	assert(err == nil, "failed to initialize new test DB")
}

func addRecordTestTable1(db *openDB, tableName string, first int, second []byte) bool {
	newRecord := MakeRecord([]Field{&IntField{first}, &BlobField{second}})
	_, err := addRecordOpenDb(db, tableName, newRecord)
	return err == nil
}

func testTable2Scheme() tableScheme {
	firstColumn := columnHeader{"IDColumn", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"NameColumn", FieldTypeString, nil, dbPointer{0, 0}}
	thirdColumn := columnHeader{"intColumn2", FieldTypeInt, nil, dbPointer{0, 0}}
	return makeTableScheme([]columnHeader{firstColumn, secondColumn, thirdColumn})
}

func addIndexToTestTable(db database, prov *OpenDBProvenance, columns *[]columnHeader, offset int) {
	openDatabse := getOpenDbWithProvenance(db, prov)
	initializeIndexInColumn(&openDatabse, *columns, offset)
	closeOpenDB(&openDatabse)
}

func writeTestTable2(db database, table string, index bool, prov *OpenDBProvenance) {
	scheme := testTable2Scheme()

	if index {
		addIndexToTestTable(db, prov, &scheme.columns, 0)
	}

	writeNewTable(db, table, scheme)
}

func addRecordTestTable2(db *openDB, tableName string, first int, second string, third int) bool {
	newRecord := MakeRecord([]Field{&IntField{first}, &StringField{second}, &IntField{third}})
	_, err := addRecordOpenDb(db, tableName, newRecord)
	return err == nil
}

func buildEmptyTableWithDbPath1(dbPath string) (database, string) {
	db, table := initializeTestDB(dbPath)
	writeTestTable1(db, table)

	return db, table
}

func buildTableWithDbPath1(dbPath string) (database, string) {
	db, tableID := buildEmptyTableWithDbPath1(dbPath)
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

	firstColumn := columnHeader{"columnA", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"columnB", FieldTypeInt, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
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

func buildTableWithDbPath3(dbPath string) (database, string) {
	db, tableID := initializeTestDB(dbPath)

	firstColumn := columnHeader{"IDColumn", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"NameColumn", FieldTypeString, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
	writeNewTable(db, tableID, scheme)
	fields := []Field{&IntField{11}, &StringField{"myname"}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{1001}, &StringField{"othername"}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	return db, tableID
}

func buildTable3() (database, string) {
	dbPath := "C:\\temp\\my_db"
	return buildTableWithDbPath3(dbPath)
}

// initializes a database with an index
func buildTable4() (database, string) {
	dbPath := IN_MEMORY_BUFFER_PATH_MAGIC
	db, tableID := initializeTestDB(dbPath)

	writeTestTable2(db, tableID, true, nil)
	return db, tableID
}

// initializes a database with multiple indexes
func buildTable5() (database, string) {
	dbPath := IN_MEMORY_BUFFER_PATH_MAGIC

	db, tableID := initializeTestDB(dbPath)

	firstColumn := columnHeader{"IDColumn", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"NameColumn", FieldTypeString, nil, dbPointer{0, 0}}
	thirdColumn := columnHeader{"intColumn2", FieldTypeInt, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn, thirdColumn})
	openDatabse := getOpenDB(db)
	initializeIndexInColumn(&openDatabse, scheme.columns, 0)
	initializeIndexInColumn(&openDatabse, scheme.columns, 2)
	closeOpenDB(&openDatabse)

	writeNewTable(db, tableID, scheme)
	return db, tableID
}

// initializes a database with 2 tables
func buildTable6() (database, testTable, testTable) {
	dbPath := "C:\\temp\\my_db"

	db, _ := initializeTestDB(dbPath)

	table1column1 := columnHeader{"table1column1", FieldTypeInt, nil, dbPointer{0, 0}}
	table1column2 := columnHeader{"table1column2", FieldTypeString, nil, dbPointer{0, 0}}
	table1scheme := makeTableScheme([]columnHeader{table1column1, table1column2})
	writeNewTable(db, "table1", table1scheme)

	table2column1 := columnHeader{"table2column1", FieldTypeInt, nil, dbPointer{0, 0}}
	table2column2 := columnHeader{"table2column2", FieldTypeString, nil, dbPointer{0, 0}}
	table2column3 := columnHeader{"table2column3", FieldTypeString, nil, dbPointer{0, 0}}
	table2scheme := makeTableScheme([]columnHeader{table2column1, table2column2, table2column3})
	writeNewTable(db, "table2", table2scheme)
	return db, testTable{name: "table1", scheme: table1scheme}, testTable{name: "table2", scheme: table2scheme}
}

func buildTable7() (database, testTable, testTable) {
	db, table1, table2 := buildTable6()

	table1.records = []Record{{Fields: []Field{IntField{100}, StringField{"Hello World"}}},
		{Fields: []Field{IntField{55}, StringField{"A string"}}},
		{Fields: []Field{IntField{-199}, StringField{"AAAAAAAA"}}}}

	table2.records = []Record{{Fields: []Field{IntField{100}, StringField{"ggggg"}, StringField{"wow"}}},
		{Fields: []Field{IntField{55}, StringField{"b"}, StringField{"RonGeva"}}},
		{Fields: []Field{IntField{-199}, StringField{"am a string"}, StringField{"very looooooooooooooooooong"}}}}

	for _, record := range table1.records {
		addRecordToTable(db, table1.name, record)
	}

	for _, record := range table2.records {
		addRecordToTable(db, table2.name, record)
	}

	return db, table1, table2
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

type testExpectedProvenanceScores struct {
	scores []uint32
}

func dummyProvenance1() (OpenDBProvenance, testExpectedProvenanceScores) {
	return OpenDBProvenance{auth: ProvenanceAuthentication{user: "ron", password: "1234"},
			conn: ProvenanceConnection{ipv4: 1001}},
		testExpectedProvenanceScores{scores: []uint32{1001, 4}}
}

func dummyProvenance2() (OpenDBProvenance, testExpectedProvenanceScores) {
	return OpenDBProvenance{auth: ProvenanceAuthentication{user: "guy", password: "123456789abcdef"},
			conn: ProvenanceConnection{ipv4: 10005}},
		testExpectedProvenanceScores{scores: []uint32{10005, 16}}
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
	record := Record{[]Field{IntField{5}, IntField{7}, BlobField{[]byte{11, 13, 25}}}, nil}
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
	query, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}

	if len(query.columns) != 2 {
		t.Fail()
	}
}

func TestParseSelectQuery3(t *testing.T) {
	db, _, _ := buildTable6()
	openDB := getOpenDB(db)
	defer closeOpenDB(&openDB)

	sql := "select table1.table1column1, table1.table1column2, table2.table2column3 from table1 join table2 where ((table1.table1column1 = 5) and (table1.table1column2 = \"aaa\")) or ((table2.table2column3 = \"bbb\") and (not (table1.table1column1 = 17)))"
	query, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}

	// make sure the query only has the expected columns
	if len(query.columns) != 3 {
		t.Fail()
	}
	if query.columns[2] != 4 {
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
	if !recordsAreEqual(regulardOrderRecords[0], records[1]) ||
		!recordsAreEqual(regulardOrderRecords[1], records[0]) {
		t.Fail()
	}
}

// test select on multiple tables
func TestCursorSelect3(t *testing.T) {
	db, table1, table2 := buildTable7()
	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()

	query := fmt.Sprintf("Select %s.%s, %s.%s from %s join %s order by %s.%s", table1.name,
		table1.scheme.columns[0].columnName, table2.name, table2.scheme.columns[1].columnName,
		table1.name, table2.name, table1.name, table1.scheme.columns[0].columnName)
	err = cursor.Execute(query)
	if err != nil {
		t.Fail()
	}
	records := cursor.FetchAll()
	if len(records) != len(table1.records)*len(table2.records) {
		t.Fail()
	}

	// TODO: add tests validating the values of the joint table are correct
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

// reads all records, performs an update, then makes sure the new records are as expected
func CursorUpdateHelper(t *testing.T, db database, tableID string, query string, expectedRecords []Record) {
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
	err = cursor.Execute(query)
	if err != nil {
		t.Fail()
	}

	recordsAfter := readAllRecords(db, tableID)
	// Sanity
	if len(recordsAfter) != len(expectedRecords) {
		t.Fail()
	}

	for i := 0; i < len(expectedRecords); i++ {
		if !recordsAreEqual(recordsAfter[i], expectedRecords[i]) {
			t.Fail()
		}
	}
}

func TestCursorUpdate1(t *testing.T) {
	db, tableID := buildTable2()
	records := readAllRecords(db, tableID)

	// we will change this record and this record only
	records[0] = Record{[]Field{IntField{33}, IntField{89}}, nil}
	CursorUpdateHelper(t, db, tableID, "update newTable set columnA=33,columnB=89 where columnA=5", records)
}

func TestCursorUpdate2(t *testing.T) {
	db, tableID := buildTable3()
	records := readAllRecords(db, tableID)

	// we will change this record and this record only
	records[1] = Record{[]Field{IntField{15}, StringField{"wowname"}}, nil}

	CursorUpdateHelper(t, db, tableID,
		"update newTable set IDColumn=15, NameColumn=\"wowname\" where NameColumn=\"othername\"",
		records)
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
	output, err := mapEachRecord(&openDatabase, []string{tableID}, &cond, mapGetRecords, nil)
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
		if !recordsAreEqual(
			records[i], Record{[]Field{&IntField{i}, &BlobField{[]byte{1, 2, 3, 4, 5}}}, nil}) {
			t.Fail()
		}
	}
}

func TestStringField(t *testing.T) {
	db, tableID := initializeTestDB1()
	firstColumn := columnHeader{"stringColumn1", FieldTypeString, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"stringColumn2", FieldTypeString, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
	writeNewTable(db, tableID, scheme)

	record1 := Record{[]Field{StringField{"Hello world"}, StringField{"goodbye world"}}, nil}
	record2 := Record{[]Field{StringField{"A string"}, StringField{"another string"}}, nil}
	record3 := Record{[]Field{StringField{"Final record"}, StringField{"This is a record"}}, nil}
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
		if !recordsAreEqual(records[i], recordsToAdd[i]) {
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

// initialize
func TestIndexSanity(t *testing.T) {
	db, tableID := buildTable4()
	fields := []Field{&IntField{1050}, &StringField{"myname"}, &IntField{555}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{1001}, &StringField{"othername"}, &IntField{137}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)

	openDb := getOpenDB(db)
	headers, err := getTableHeaders(&openDb, tableID)
	if err != nil {
		t.Fail()
	}

	if headers.scheme.columns[0].index == nil {
		t.Fail()
	}

	index := headers.scheme.columns[0].index
	iterator := index.Iterator()
	a := iterator.Next()

	// make sure the first element is actually the smallest element in the table
	if a.Key != 1001 {
		t.Fail()
	}
}

type testRecord struct {
	record Record
	offset uint32
}

type recordsComparableByKey struct {
	records []testRecord
	key     int
}

func (records *recordsComparableByKey) Len() int {
	return len(records.records)
}

func (records *recordsComparableByKey) Less(i, j int) bool {
	leftKey := records.records[i].record.Fields[records.key].ToKey()
	rightKey := records.records[j].record.Fields[records.key].ToKey()
	return *leftKey < *rightKey
}

func (records *recordsComparableByKey) Swap(i, j int) {
	temp := records.records[i]
	records.records[i] = records.records[j]
	records.records[j] = temp
}

func assertIndexIteratorEqualToInsertedRecords(headers *tableHeaders, records recordsComparableByKey,
	indexOffset int, t *testing.T) {
	if headers.scheme.columns[indexOffset].index == nil {
		t.Fail()
	}

	iterator := headers.scheme.columns[indexOffset].index.Iterator()
	pairs := make([]b_tree.BTreeKeyPointerPair, 0)
	pair := iterator.Next()
	for pair != nil {
		pairs = append(pairs, *pair)
		pair = iterator.Next()
	}

	if len(pairs) != records.Len() {
		t.Fail()
	}

	for i := 0; i < len(pairs); i++ {
		if pairs[i].Key != *records.records[i].record.Fields[indexOffset].ToKey() {
			t.Fail()
		}
	}
}

func IndexAddAndRemoveHelper(t *testing.T, recordsToAdd []Record, recordsToDelete []Record,
	dbBuilder func() (database, string), indexOffsets []int) {
	db, tableID := dbBuilder()
	records := recordsComparableByKey{records: make([]testRecord, 0), key: 0}

	for i := 0; i < len(recordsToAdd); i++ {
		record := recordsToAdd[i]
		offset, err := addRecordToTable(db, tableID, record)
		if err != nil {
			t.FailNow()
		}

		records.records = append(records.records, testRecord{record: record, offset: offset})
	}

	for i := 0; i < len(recordsToDelete); i++ {
		found := false
		for j := 0; j < records.Len(); j++ {
			if recordsAreEqual(recordsToDelete[i], records.records[j].record) {
				record := records.records[j]
				// remove record
				records.records = append(records.records[:j], records.records[j+1:]...)
				err := deleteRecord(db, tableID, record.offset)
				if err != nil {
					t.Fail()
				}
				found = true
				break
			}
		}
		if !found {
			t.Fail()
		}
	}

	openDb := getOpenDB(db)
	headers, err := getTableHeaders(&openDb, tableID)
	if err != nil {
		t.Fail()
	}

	for i := 0; i < len(indexOffsets); i++ {
		// sort the records according to the index
		records.key = indexOffsets[i]
		sort.Sort(&records)

		// make sure the index iterator returns the same pairs, in the correct order
		assertIndexIteratorEqualToInsertedRecords(headers, records, indexOffsets[i], t)
	}
}

// Function to generate a unique random number
func generateUniqueNumber(seenNumbers map[int]bool, maxNumber int) int {
	for {
		// Generate a random number between 0 and maxNumber-1
		num := rand.Intn(maxNumber)

		// Check if the number has already been seen
		if !seenNumbers[num] {
			// Mark the number as seen and return it
			seenNumbers[num] = true
			return num
		}
	}
}

func generateRandomRecordForTable4(seenNumbers map[int]bool) Record {
	firstInt := generateUniqueNumber(seenNumbers, 10000)
	secondInt := rand.Intn(1000)
	randomBuffer := make([]byte, 200)
	// the documentation pretty much gurantees this function will always succeed, no need to check return value
	rand.Read(randomBuffer)
	return MakeRecord([]Field{&IntField{firstInt}, &StringField{string(randomBuffer)}, &IntField{secondInt}})
}

func generateRandomRecordForTable5(seenNumbers map[int]bool) Record {
	firstInt := generateUniqueNumber(seenNumbers, 10000)
	secondInt := generateUniqueNumber(seenNumbers, 10000)
	randomBuffer := make([]byte, 200)
	// the documentation pretty much gurantees this function will always succeed, no need to check return value
	rand.Read(randomBuffer)
	return MakeRecord([]Field{&IntField{firstInt}, &StringField{string(randomBuffer)}, &IntField{secondInt}})
}

func TestIndexAddManyRecords(t *testing.T) {
	recordsToAdd := make([]Record, 0)
	seenNumbers := make(map[int]bool)

	for i := 0; i < 1000; i++ {
		record := generateRandomRecordForTable4(seenNumbers)
		recordsToAdd = append(recordsToAdd, record)
	}

	IndexAddAndRemoveHelper(t, recordsToAdd, make([]Record, 0), buildTable4, []int{0})
}

func TestIndexAddAndRemoveManyRecords(t *testing.T) {
	recordsToAdd := make([]Record, 0)
	seenNumbers := make(map[int]bool)

	for i := 0; i < 1000; i++ {
		record := generateRandomRecordForTable4(seenNumbers)
		recordsToAdd = append(recordsToAdd, record)
	}

	recordsToDelete := recordsToAdd[100:300]

	IndexAddAndRemoveHelper(t, recordsToAdd, recordsToDelete, buildTable4, []int{0})
}

func TestMultipleIndexes(t *testing.T) {
	recordsToAdd := make([]Record, 0)
	seenNumbers := make(map[int]bool)

	for i := 0; i < 1000; i++ {
		record := generateRandomRecordForTable5(seenNumbers)
		recordsToAdd = append(recordsToAdd, record)
	}

	recordsToDelete := recordsToAdd[100:300]

	IndexAddAndRemoveHelper(t, recordsToAdd, recordsToDelete, buildTable5, []int{0, 2})
}

func TestRemoveRecordDuplication(t *testing.T) {
	fields1 := []Field{&IntField{5}, &StringField{"AAAA"}, &IntField{12}}
	fields2 := []Field{&IntField{1000}, &StringField{"bbbbbb"}, &IntField{-50}}
	fields3 := []Field{&IntField{5}, &StringField{"CCCC"}, &IntField{-1000}}
	fields4 := fields2

	record1 := Record{Fields: fields1}
	record2 := Record{Fields: fields2}
	record3 := Record{Fields: fields3}
	record4 := Record{Fields: fields4}

	records := []Record{record1, record2, record3, record4}
	uniqueRecords := removeRecordDuplications(records)

	if len(uniqueRecords) != 3 {
		t.FailNow()
	}

	for i := 0; i < len(uniqueRecords); i++ {
		if !recordsAreEqual(uniqueRecords[i], records[i]) {
			t.Fail()
		}
	}
}

// test the provenance of a single inserted record behaves as expected in the most basic terms,
// given some assumptions about the way the provenance score is calculated
func TestProvenanceSanity(t *testing.T) {
	db, tableName := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	writeTestTable1(db, tableName)

	record := Record{[]Field{&IntField{55}, &BlobField{[]byte{1, 2, 3, 4, 5}}}, nil}

	prov, expectedProvScores := dummyProvenance1()
	openDb := getOpenDbWithProvenance(db, &prov)

	_, err := addRecordOpenDb(&openDb, tableName, record)
	if err != nil {
		t.Fail()
	}

	closeOpenDB(&openDb)

	records := readAllRecords(db, tableName)
	if len(records) != 1 {
		t.Fail()
	}

	firstKey := records[0].Provenance[0].ToKey()
	if firstKey == nil || *firstKey != b_tree.BTreeKeyType(expectedProvScores.scores[0]) {
		t.Fail()
	}

	secondKey := records[0].Provenance[1].ToKey()
	if secondKey == nil || int(*secondKey) != int(expectedProvScores.scores[1]) {
		t.Fail()
	}
}

func testIsProvenanceEqual(left ProvenanceField, right ProvenanceField) bool {
	assert(left.operator == ProvenanceOperatorNil && right.operator == ProvenanceOperatorNil,
		"only supports simple provenance fields")

	if left.Type != right.Type {
		return false
	}

	return fieldsAreEqual(left.Value, right.Value)
}

func TestProvenanceJoin(t *testing.T) {
	prov, _ := dummyProvenance1()
	db, firstTable := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	writeTestTable1(db, firstTable) // "newTable"
	secondTable := "otherTable"
	writeTestTable2(db, secondTable, false, &prov)

	openDb := getOpenDbWithProvenance(db, &prov)
	defer closeOpenDB(&openDb)

	if !addRecordTestTable1(&openDb, firstTable, 1, []byte{1}) {
		t.FailNow()
	}
	if !addRecordTestTable1(&openDb, firstTable, 9, []byte{100, 100, 100, 100, 55, 55, 55}) {
		t.FailNow()
	}

	if !addRecordTestTable2(&openDb, secondTable, 10111, "Aho Corasick", 1337) {
		t.FailNow()
	}
	if !addRecordTestTable2(&openDb, secondTable, -52, "mmmmmmm", 0) {
		t.FailNow()
	}

	iterator, err := initializeJointTableRecordIterator(&openDb, []string{"newTable", "otherTable"})
	if err != nil {
		t.FailNow()
	}

	records := make([]Record, 0)
	record := iterator.next()
	for record != nil {
		records = append(records, record.record)
		record = iterator.next()
	}

	if len(records) != 4 {
		t.Fail()
	}

	for _, record := range records {
		// each table has x provenance columns, their JOIN should have the same amount
		if len(record.Provenance) != len(openDb.provFields) {
			t.Fail()
		}

		for _, provField := range record.Provenance {
			// we've perform a JOIN operation which is a logical provenance multiplication
			if provField.operator != ProvenanceOperatorMultiply {
				t.Fail()
			}

			// we've JOIN-ed 2 tables, each provenance should have 2 operands
			if len(provField.operands) != 2 {
				t.Fail()
			}

			// all fields were added by the same open db, the underlying provenance should be identical
			if !testIsProvenanceEqual(*provField.operands[0], *provField.operands[1]) {
				t.Fail()
			}
		}
	}
}

func testGetAllIndexPairs(iterator *b_tree.BTreeIterator) []b_tree.BTreeKeyPointerPair {
	pairs := make([]b_tree.BTreeKeyPointerPair, 0)
	pair := iterator.Next()
	for pair != nil {
		pairs = append(pairs, *pair)
		pair = iterator.Next()
	}

	return pairs
}

// Create a DB, connect with multiple provenances and add records from each one.
// Then iterate the provenance index and make sure we see the correct amount of
// provenances as well as the correct values for each one.
func TestProvenanceIndexMultipleProvenancesInsert(t *testing.T) {
	prov, expectedProvScores := dummyProvenance1()
	db, tableName := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)

	scheme := testTable2Scheme()
	openDb := getOpenDbWithProvenance(db, &prov)
	scheme.provColumns = openDb.provenanceSchemeColumns()
	closeOpenDB(&openDb)

	addIndexToTestTable(db, &prov, &scheme.provColumns, 0)

	writeNewTable(db, tableName, scheme)

	openDb = getOpenDbWithProvenance(db, &prov)
	defer closeOpenDB(&openDb)

	if !addRecordTestTable2(&openDb, tableName, 10111, "Aho Corasick", 1337) {
		t.FailNow()
	}
	if !addRecordTestTable2(&openDb, tableName, -52, "mmmmmmm", 0) {
		t.FailNow()
	}

	closeOpenDB(&openDb)

	prov2, expectedProv2 := dummyProvenance2()
	openDb = getOpenDbWithProvenance(db, &prov2)
	if !addRecordTestTable2(&openDb, tableName, 1131, "AHAHAHAH", -59) {
		t.FailNow()
	}

	headers, err := getTableHeaders(&openDb, tableName)
	if err != nil {
		t.FailNow()
	}

	iterator := headers.scheme.provColumns[0].index.Iterator()
	if iterator == nil {
		t.FailNow()
	}

	pairs := testGetAllIndexPairs(iterator)

	if len(pairs) != 2 {
		t.FailNow()
	}

	if pairs[0].Key != b_tree.BTreeKeyType(expectedProvScores.scores[0]) {
		t.Fail()
	}
	if pairs[1].Key != b_tree.BTreeKeyType(expectedProv2.scores[0]) {
		t.Fail()
	}
}
