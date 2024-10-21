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

type proveTypeToScore map[ProvenanceType]ProvenanceScore

const TEST_DB_PATH1 = "c:\\temp\\my_db"

func testDatabaseFromPath(path string) database {
	return database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
}

func initializeTestDbInternal(path string, provenance bool) (database, string) {
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	InitializeDB(path, provenance)
	db := testDatabaseFromPath(path)
	return db, tableID
}

func initializeTestDB(path string) (database, string) {
	return initializeTestDbInternal(path, false)
}

func initializeTestDB1() (database, string) {
	path := TEST_DB_PATH1
	return initializeTestDB(path)
}

func testTable1Scheme() tableScheme {
	firstColumn := columnHeader{"columnA", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"columnB", FieldTypeBlob, nil, dbPointer{0, 0}}
	return makeTableScheme([]columnHeader{firstColumn, secondColumn})
}

func writeTestTable1(db *openDB, table string) {
	scheme := testTable1Scheme()
	err := writeNewTable(db, table, scheme)
	assert(err == nil, "failed to initialize new test DB")
}

func addRecordTestTable1(db *openDB, tableName string, first int, second []byte) bool {
	newRecord := MakeRecord([]Field{&IntField{first}, &BlobField{second}})
	_, err := addRecordToTable(db, tableName, newRecord)
	return err == nil
}

func testTable2Scheme() tableScheme {
	firstColumn := columnHeader{"IDColumn", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"NameColumn", FieldTypeString, nil, dbPointer{0, 0}}
	thirdColumn := columnHeader{"intColumn2", FieldTypeInt, nil, dbPointer{0, 0}}
	return makeTableScheme([]columnHeader{firstColumn, secondColumn, thirdColumn})
}

func writeTestTable2(db *openDB, table string, index bool, prov *DBProvenance) {
	scheme := testTable2Scheme()

	if index {
		initializeIndexInColumn(db, scheme.columns, 0)
	}

	writeNewTable(db, table, scheme)
}

func addRecordTestTable2(db *openDB, tableName string, first int, second string, third int) bool {
	newRecord := MakeRecord([]Field{&IntField{first}, &StringField{second}, &IntField{third}})
	_, err := addRecordToTable(db, tableName, newRecord)
	return err == nil
}

func testTable3Scheme() tableScheme {
	firstColumn := columnHeader{"aaa", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"bbb", FieldTypeInt, nil, dbPointer{0, 0}}
	return makeTableScheme([]columnHeader{firstColumn, secondColumn})
}

func addRecordTestTable3(db *openDB, tableName string, first int, second int) bool {
	newRecord := MakeRecord([]Field{&IntField{first}, &IntField{second}})
	_, err := addRecordToTable(db, tableName, newRecord)
	return err == nil
}

func buildEmptyTableWithDbPath1(dbPath string) (*openDB, string) {
	db, table := initializeTestDB(dbPath)
	openDb, err := getOpenDB(db, nil)
	assert(err == nil, "failed to open db in buildEmptyTableWithDbPath1")

	writeTestTable1(openDb, table)

	return openDb, table
}

func buildTableWithDbPath1(dbPath string) (*openDB, string) {
	db, tableID := buildEmptyTableWithDbPath1(dbPath)

	fields := []Field{&IntField{5}, &BlobField{make([]byte, 10)}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{13}, &BlobField{make([]byte, 5000)}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	return db, tableID
}

func buildTable1() (*openDB, string) {
	return buildTableWithDbPath1(TEST_DB_PATH1)
}

func buildTableWithDbPath2(dbPath string) (*openDB, string) {
	db, tableID := initializeTestDB(dbPath)
	openDb, err := getOpenDB(db, nil)
	assert(err == nil, "failed to open db in buildTableWithDbPath2")

	firstColumn := columnHeader{"columnA", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"columnB", FieldTypeInt, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
	writeNewTable(openDb, tableID, scheme)
	fields := []Field{&IntField{5}, &IntField{44}}
	newRecord := MakeRecord(fields)
	addRecordToTable(openDb, tableID, newRecord)
	fields = []Field{&IntField{13}, &IntField{30}}
	newRecord = MakeRecord(fields)
	addRecordToTable(openDb, tableID, newRecord)
	return openDb, tableID
}

func buildTable2() (*openDB, string) {
	dbPath := TEST_DB_PATH1
	return buildTableWithDbPath2(dbPath)
}

func buildTableWithDbPath3(dbPath string) (*openDB, string) {
	db, tableID := initializeTestDB(dbPath)

	openDb, err := getOpenDB(db, nil)
	assert(err == nil, "failed opening DB in buildTableWithDbPath3")

	firstColumn := columnHeader{"IDColumn", FieldTypeInt, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"NameColumn", FieldTypeString, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
	writeNewTable(openDb, tableID, scheme)
	fields := []Field{&IntField{11}, &StringField{"myname"}}
	newRecord := MakeRecord(fields)
	addRecordToTable(openDb, tableID, newRecord)
	fields = []Field{&IntField{1001}, &StringField{"othername"}}
	newRecord = MakeRecord(fields)
	addRecordToTable(openDb, tableID, newRecord)
	return openDb, tableID
}

func buildTable3() (*openDB, string) {
	dbPath := TEST_DB_PATH1
	return buildTableWithDbPath3(dbPath)
}

// initializes a database with an index
func buildTable4() (*openDB, string) {
	dbPath := IN_MEMORY_BUFFER_PATH_MAGIC
	db, tableID := initializeTestDB(dbPath)

	openDb, err := getOpenDB(db, nil)
	assert(err == nil, "failed opening DB in buildTable4")

	writeTestTable2(openDb, tableID, true, nil)
	return openDb, tableID
}

// initializes a database with multiple indexes
func buildTable5() (*openDB, string) {
	dbPath := IN_MEMORY_BUFFER_PATH_MAGIC

	db, tableID := initializeTestDB(dbPath)

	scheme := testTable2Scheme()
	openDatabase, err := getOpenDB(db, nil)
	assert(err == nil, "failed opening DB in buildTable5")

	initializeIndexInColumn(openDatabase, scheme.columns, 0)
	initializeIndexInColumn(openDatabase, scheme.columns, 2)
	writeNewTable(openDatabase, tableID, scheme)

	return openDatabase, tableID
}

// initializes a database with 2 tables
func buildTable6() (*openDB, testTable, testTable) {
	db, _ := initializeTestDB(TEST_DB_PATH1)

	openDatabase, err := getOpenDB(db, nil)
	assert(err == nil, "failed opening DB in buildTable6")

	table1column1 := columnHeader{"table1column1", FieldTypeInt, nil, dbPointer{0, 0}}
	table1column2 := columnHeader{"table1column2", FieldTypeString, nil, dbPointer{0, 0}}
	table1scheme := makeTableScheme([]columnHeader{table1column1, table1column2})
	writeNewTable(openDatabase, "table1", table1scheme)

	table2column1 := columnHeader{"table2column1", FieldTypeInt, nil, dbPointer{0, 0}}
	table2column2 := columnHeader{"table2column2", FieldTypeString, nil, dbPointer{0, 0}}
	table2column3 := columnHeader{"table2column3", FieldTypeString, nil, dbPointer{0, 0}}
	table2scheme := makeTableScheme([]columnHeader{table2column1, table2column2, table2column3})
	writeNewTable(openDatabase, "table2", table2scheme)
	return openDatabase, testTable{name: "table1", scheme: table1scheme},
		testTable{name: "table2", scheme: table2scheme}
}

func buildTable7() (*openDB, testTable, testTable) {
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
	closeOpenDB(db)

	conn, err := Connect(TEST_DB_PATH1, nil)
	if err != nil {
		return nil, err
	}
	cursor := conn.OpenCursor()
	return &testContext{db: db.db, cursor: cursor, tableID: tableID}, nil
}

type testExpectedProvenanceScores struct {
	scores proveTypeToScore
}

func dummyProvenance1() (DBProvenance, testExpectedProvenanceScores) {
	return DBProvenance{Auth: ProvenanceAuthentication{User: "ron", Password: "123456"},
			Conn: ProvenanceConnection{Ipv4: 14 << 24}},
		testExpectedProvenanceScores{
			scores: proveTypeToScore{ProvenanceTypeConnection: 14, ProvenanceTypeAuthentication: (32 - 6) * 8}}
}

func dummyProvenance2() (DBProvenance, testExpectedProvenanceScores) {
	return DBProvenance{Auth: ProvenanceAuthentication{User: "guy", Password: "1234"},
			Conn: ProvenanceConnection{Ipv4: 18 << 24}},
		testExpectedProvenanceScores{
			scores: proveTypeToScore{ProvenanceTypeConnection: 18, ProvenanceTypeAuthentication: (32 - 4) * 8}}
}

func dummyProvenance3() (DBProvenance, testExpectedProvenanceScores) {
	return DBProvenance{Auth: ProvenanceAuthentication{User: "aaaaab", Password: "aaaaa"},
			Conn: ProvenanceConnection{Ipv4: 10 << 14}},
		testExpectedProvenanceScores{
			scores: proveTypeToScore{ProvenanceTypeConnection: 10, ProvenanceTypeAuthentication: (32 - 5) * 8}}
}

func TestFullFlow(t *testing.T) {
	db, tableID := buildTable1()
	defer closeOpenDB(db)

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
	node1FieldIndex := uint32(0)
	node1.condition = &condition{leftOperand: operand{fieldIndex: &node1FieldIndex},
		conditionType: ConditionTypeEqual, rightOperand: operand{valueLiteral: uint32ToBytes(5)}}
	// Condition should succeed
	if !checkAllConditions(node1, record) {
		t.Fail()
	}
	node2 := conditionNode{}
	node2FieldIndex := uint32(1)
	node2.condition = &condition{leftOperand: operand{fieldIndex: &node2FieldIndex},
		conditionType: ConditionTypeEqual, rightOperand: operand{valueLiteral: uint32ToBytes(6)}}
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
	node4FieldIndex := uint32(1)
	node4.condition = &condition{leftOperand: operand{fieldIndex: &node4FieldIndex},
		conditionType: ConditionTypeGreater, rightOperand: operand{valueLiteral: uint32ToBytes(6)}}
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
	node1FieldIndex := uint32(0)
	node2FieldIndex := uint32(1)
	node1 := conditionNode{condition: &condition{leftOperand: operand{fieldIndex: &node1FieldIndex},
		conditionType: ConditionTypeEqual, rightOperand: operand{valueLiteral: uint32ToBytes(5)}}}
	node2 := conditionNode{condition: &condition{leftOperand: operand{fieldIndex: &node2FieldIndex},
		conditionType: ConditionTypeEqual, rightOperand: operand{valueLiteral: uint32ToBytes(44)}}}
	return conditionNode{operator: ConditionOperatorAnd, operands: []*conditionNode{&node1, &node2}}
}

func buildConditionTreeForTestAbove500OrBelow100() conditionNode {
	node1FieldIndex := uint32(0)
	node2FieldIndex := uint32(1)
	node1 := conditionNode{condition: &condition{leftOperand: operand{fieldIndex: &node1FieldIndex},
		conditionType: ConditionTypeGreater, rightOperand: operand{valueLiteral: uint32ToBytes(500)}}}
	node2 := conditionNode{condition: &condition{leftOperand: operand{fieldIndex: &node2FieldIndex},
		conditionType: ConditionTypeLess, rightOperand: operand{valueLiteral: uint32ToBytes(100)}}}

	return conditionNode{operator: ConditionOperatorOr, operands: []*conditionNode{&node1, &node2}}
}

func TestRecordFilter(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	defer closeOpenDB(db)
	records, err := filterRecordsFromTables(db, []string{tableID}, &cond, nil)
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
			*node.condition.leftOperand.fieldIndex, node.condition.conditionType)
	}
}

func TestDeleteRecordsFromTable(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	defer closeOpenDB(db)
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
	defer closeOpenDB(db)

	sql := "select columna, columnb from newTable where ((columna = 5) and (columnb = 13)) or ((columna = 7) and (not (columnb = 30)))"
	_, err := parseSelectQuery(db, sql)
	if err != nil {
		t.Fail()
	}
}

func TestParseSelectQuery2(t *testing.T) {
	db, _ := buildTable2()
	defer closeOpenDB(db)

	sql := "select columna, columnb from newTable where columna = 7 and columnb = 13 or columna = 5 and columna = 10 and not columnb = 5"
	query, err := parseSelectQuery(db, sql)
	if err != nil {
		t.Fail()
	}

	if len(query.columns) != 2 {
		t.Fail()
	}
}

func TestParseSelectQuery3(t *testing.T) {
	db, _, _ := buildTable6()
	defer closeOpenDB(db)

	sql := "select table1.table1column1, table1.table1column2, table2.table2column3 from table1 join table2 where ((table1.table1column1 = 5) and (table1.table1column2 = \"aaa\")) or ((table2.table2column3 = \"bbb\") and (not (table1.table1column1 = 17)))"
	query, err := parseSelectQuery(db, sql)
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

// verify SELECT on non-existent column results in a failure
func TestParseSelectQueryNonExistentColumn(t *testing.T) {
	db, _, _ := buildTable6()
	defer closeOpenDB(db)

	sql := "select blablabla from table1 join table2"
	_, err := parseSelectQuery(db, sql)
	if err == nil {
		t.FailNow()
	}

	_, ok := err.(*nonExistentColumnError)
	if !ok {
		t.Fail()
	}
}

// verify SELECT with "best" keyword
func TestParseSelectQueryBestKeyword(t *testing.T) {
	db, _, _ := buildTable6()
	defer closeOpenDB(db)

	sql := "select table1.table1column1 from table1 join table2 order by table1.table1column1 best 10"
	query, err := parseSelectQuery(db, sql)
	if err != nil {
		t.FailNow()
	}

	if query.bestAmount == nil || *query.bestAmount != 10 {
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
	closeOpenDB(db)

	dbPath := db.db.id.identifyingString
	conn, err := Connect(dbPath, nil)
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

	regularOrderRecords := readAllRecords(db, tableID)
	// sanity
	if len(regularOrderRecords) != 2 {
		t.Fail()
	}
	closeOpenDB(db)

	dbPath := db.db.id.identifyingString
	conn, err := Connect(dbPath, nil)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()

	err = cursor.Execute("Select columnA, ColumnB from newTable order by columnB")
	if err != nil {
		t.Fail()
	}
	records := cursor.FetchAll()
	if len(records) != 2 {
		t.Fail()
	}

	// We expect the order of the records to be reversed
	if !recordsAreEqual(regularOrderRecords[0], records[1]) ||
		!recordsAreEqual(regularOrderRecords[1], records[0]) {
		t.Fail()
	}
}

// test select on multiple tables
func TestCursorSelect3(t *testing.T) {
	db, table1, table2 := buildTable7()
	closeOpenDB(db)

	dbPath := db.db.id.identifyingString
	conn, err := Connect(dbPath, nil)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()

	query := fmt.Sprintf("Select %s.%s, %s.%s from %s join %s order by %s.%s",
		table2.name, table2.scheme.columns[1].columnName, // first column
		table1.name, table1.scheme.columns[0].columnName, // second column
		table1.name, table2.name, // tables
		table1.name, table1.scheme.columns[0].columnName) // order by expression
	err = cursor.Execute(query)
	if err != nil {
		t.Fail()
	}
	records := cursor.FetchAll()
	if len(records) != len(table1.records)*len(table2.records) {
		t.Fail()
	}

	if len(cursor.columnNames) != 2 {
		t.FailNow()
	}

	if cursor.columnNames[0] != fmt.Sprintf("%s.%s", table2.name, table2.scheme.columns[1].columnName) {
		t.Fail()
	}

	if cursor.columnNames[1] != fmt.Sprintf("%s.%s", table1.name, table1.scheme.columns[0].columnName) {
		t.Fail()
	}
	// TODO: add tests validating the values of the joint table are correct
}

// test select execution with "best" keyword
func TestCursorSelectWithBestKeyword(t *testing.T) {
	db, _ := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	bigProv, _ := dummyProvenance2()

	dbPath := db.id.identifyingString
	conn, err := Connect(dbPath, &bigProv)
	if err != nil {
		t.Fail()
	}
	cursor := conn.OpenCursor()

	err = cursor.Execute("create table newTable (columnA int, columnB int)")
	if err != nil {
		t.FailNow()
	}
	err = cursor.Execute("insert into newTable values (13, 32), (41, 55), (37, 38)")
	if err != nil {
		t.FailNow()
	}

	// dummy provenance 1 has smaller score on all provenance fields
	smallProv, _ := dummyProvenance1()
	conn, err = Connect(dbPath, &smallProv)
	if err != nil {
		t.FailNow()
	}

	err = cursor.Execute("insert into newTable values (1, 2), (1000, 5000)")
	if err != nil {
		t.FailNow()
	}

	err = cursor.Execute("select columnA, columnB from newTable best 2")
	if err != nil {
		t.FailNow()
	}

	records := cursor.FetchAll()
	if len(records) != 2 {
		t.Fail()
	}

	// assert the retrieved records are exactly the records inserted under the second provenance,
	// which has a smaller score
	if !recordsAreEqual(records[0], Record{Fields: []Field{IntField{1}, IntField{2}}}) {
		t.Fail()
	}
	if !recordsAreEqual(records[1], Record{Fields: []Field{IntField{1000}, IntField{5000}}}) {
		t.Fail()
	}
}

func TestCursorInsert1(t *testing.T) {
	db, _ := buildTable2()
	closeOpenDB(db)

	dbPath := db.db.id.identifyingString
	conn, err := Connect(dbPath, nil)
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

func testReadAllRecords(db database, table string) ([]Record, error) {
	openDb, err := getOpenDB(db, nil)
	if err != nil {
		return nil, err
	}

	records := readAllRecords(openDb, table)
	closeOpenDB(openDb)

	return records, nil
}

func TestCursorDelete1(t *testing.T) {
	context, err := getConnectionTable2()
	if err != nil {
		t.Fail()
	}

	records, err := testReadAllRecords(context.db, context.tableID)
	if err != nil {
		t.FailNow()
	}

	// sanity
	if len(records) != 2 {
		t.Fail()
	}

	// remove one record
	err = context.cursor.Execute("delete from newTable where columnB = 30")
	if err != nil {
		t.Fail()
	}

	records, err = testReadAllRecords(context.db, context.tableID)
	if err != nil || len(records) != 1 {
		t.Fail()
	}
}

func TestCursorCreateInsertSelect1(t *testing.T) {
	db, _ := initializeTestDB1()
	connection, err := Connect(db.id.identifyingString, nil)
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
	connection, err := Connect(db.id.identifyingString, nil)
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
	defer closeOpenDB(db)

	recordsBefore := readAllRecords(db, tableID)
	// Sanity
	if len(recordsBefore) != 2 {
		t.FailNow()
	}

	update := recordUpdate{[]recordChange{{fieldIndex: 0, newData: []byte{1, 0, 0, 0}}}}

	err := updateRecordsViaCondition(db, tableID, &cond, update)
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
func CursorUpdateHelper(t *testing.T, db *openDB, tableID string, query string, expectedRecords []Record) {
	dbPath := db.db.id.identifyingString
	recordsBefore := readAllRecords(db, tableID)
	if len(recordsBefore) != 2 {
		t.Fail()
	}

	conn, err := Connect(dbPath, nil)
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
	defer closeOpenDB(db)

	records := readAllRecords(db, tableID)

	// we will change this record and this record only
	records[0] = Record{[]Field{IntField{33}, IntField{89}}, nil}
	CursorUpdateHelper(t, db, tableID, "update newTable set columnA=33,columnB=89 where columnA=5", records)
}

func TestCursorUpdate2(t *testing.T) {
	db, tableID := buildTable3()
	defer closeOpenDB(db)

	records := readAllRecords(db, tableID)

	// we will change this record and this record only
	records[1] = Record{[]Field{IntField{15}, StringField{"wowname"}}, nil}

	CursorUpdateHelper(t, db, tableID,
		"update newTable set IDColumn=15, NameColumn=\"wowname\" where NameColumn=\"othername\"",
		records)
}

func TestCannotCreateTheSameTableTwice(t *testing.T) {
	db, tableID := buildTable1()
	defer closeOpenDB(db)

	headers, err := getTableHeaders(db, tableID)
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
	openDatabase, err := getOpenDB(db, nil)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDatabase)

	BLOCKS_TO_ADD := (int(openDatabase.header.dataBlockSize)-4)*8 - ALREADY_USED_DATA_BLOCKS

	for i := 0; i < BLOCKS_TO_ADD; i++ {
		allocateNewDataBlock(openDatabase)
	}

	allocateNewDataBlock(openDatabase)

	bitmapData := readAllDataFromDbPointer(openDatabase, openDatabase.header.bitmapPointer)
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
	defer closeOpenDB(db)

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

	output, err := mapEachRecord(db, []string{tableID}, &cond, mapGetRecords, nil)
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
	defer closeOpenDB(db)

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
	openDb, err := getOpenDB(db, nil)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDb)

	firstColumn := columnHeader{"stringColumn1", FieldTypeString, nil, dbPointer{0, 0}}
	secondColumn := columnHeader{"stringColumn2", FieldTypeString, nil, dbPointer{0, 0}}
	scheme := makeTableScheme([]columnHeader{firstColumn, secondColumn})
	writeNewTable(openDb, tableID, scheme)

	record1 := Record{[]Field{StringField{"Hello world"}, StringField{"goodbye world"}}, nil}
	record2 := Record{[]Field{StringField{"A string"}, StringField{"another string"}}, nil}
	record3 := Record{[]Field{StringField{"Final record"}, StringField{"This is a record"}}, nil}
	recordsToAdd := []Record{record1, record2, record3}
	for i := 0; i < len(recordsToAdd); i++ {
		addRecordToTable(openDb, tableID, recordsToAdd[i])
	}

	records := readAllRecords(openDb, tableID)
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
	defer closeOpenDB(db)

	records := readAllRecords(db, tableID)
	if len(records) != 2 {
		t.Fail()
	}
}

// initialize
func TestIndexSanity(t *testing.T) {
	db, tableID := buildTable4()
	defer closeOpenDB(db)

	fields := []Field{&IntField{1050}, &StringField{"myname"}, &IntField{555}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{1001}, &StringField{"othername"}, &IntField{137}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)

	headers, err := getTableHeaders(db, tableID)
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
	dbBuilder func() (*openDB, string), indexOffsets []int) {
	db, tableID := dbBuilder()
	defer closeOpenDB(db)

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

	headers, err := getTableHeaders(db, tableID)
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
	prov, expectedProvScores := dummyProvenance1()

	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDb)

	writeTestTable1(openDb, tableName)

	record := Record{[]Field{&IntField{55}, &BlobField{[]byte{1, 2, 3, 4, 5}}}, nil}
	_, err = addRecordToTable(openDb, tableName, record)
	if err != nil {
		t.Fail()
	}

	records := readAllRecords(openDb, tableName)
	if len(records) != 1 {
		t.Fail()
	}

	firstKey := records[0].Provenance[0].ToKey()
	if firstKey == nil || *firstKey != b_tree.BTreeKeyType(expectedProvScores.scores[ProvenanceTypeConnection]) {
		t.Fail()
	}

	secondKey := records[0].Provenance[1].ToKey()
	if secondKey == nil || int(*secondKey) != int(expectedProvScores.scores[ProvenanceTypeAuthentication]) {
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

func testGetAllJointRecords(openDb *openDB, t *testing.T, tableNames []string) []Record {
	iterator, err := initializeJointTableRecordIterator(openDb, tableNames)
	if err != nil {
		t.FailNow()
	}

	records := make([]Record, 0)
	record := iterator.next()
	for record != nil {
		records = append(records, record.record)
		record = iterator.next()
	}

	return records
}

func TestProvenanceJoin(t *testing.T) {
	prov, _ := dummyProvenance1()
	db, firstTable := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}

	defer closeOpenDB(openDb)

	writeTestTable1(openDb, firstTable) // "newTable"
	secondTable := "otherTable"
	writeTestTable2(openDb, secondTable, false, &prov)

	if !addRecordTestTable1(openDb, firstTable, 1, []byte{1}) {
		t.FailNow()
	}
	if !addRecordTestTable1(openDb, firstTable, 9, []byte{100, 100, 100, 100, 55, 55, 55}) {
		t.FailNow()
	}

	if !addRecordTestTable2(openDb, secondTable, 10111, "Aho Corasick", 1337) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, secondTable, -52, "mmmmmmm", 0) {
		t.FailNow()
	}

	records := testGetAllJointRecords(openDb, t, []string{"newTable", "otherTable"})

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
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}

	scheme.provColumns = openDb.provenanceSchemeColumns()
	writeNewTable(openDb, tableName, scheme)

	if !addRecordTestTable2(openDb, tableName, 10111, "Aho Corasick", 1337) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, tableName, -52, "mmmmmmm", 0) {
		t.FailNow()
	}
	closeOpenDB(openDb)

	prov2, expectedProv2 := dummyProvenance2()
	openDb, err = getOpenDB(db, &prov2)
	if err != nil {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, tableName, 1131, "AHAHAHAH", -59) {
		t.FailNow()
	}

	headers, err := getTableHeaders(openDb, tableName)
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

	if pairs[0].Key != b_tree.BTreeKeyType(expectedProvScores.scores[ProvenanceTypeConnection]) {
		t.Fail()
	}

	if pairs[1].Key != b_tree.BTreeKeyType(expectedProv2.scores[ProvenanceTypeConnection]) {
		t.Fail()
	}
}

// create a table and add a few records to it.
// then filter record from the table such that some records will be merged due to having
// the same values when filtered by the selection.
// we then assert the provenance looks as we expect it to look (contains the operator "+" for
// merged records).
func TestProvenanceSelect(t *testing.T) {
	prov, _ := dummyProvenance1()
	db, firstTable := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDb)

	writeTestTable2(openDb, firstTable, false, &prov)

	if !addRecordTestTable2(openDb, firstTable, 1111, "Hello", 0) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, firstTable, 55, "a", 1) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, firstTable, 1111, "Goodbye", 12) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, firstTable, 1111, "heyheyhey", 1005) {
		t.FailNow()
	}

	node1 := conditionNode{}
	node1FieldIndex := uint32(0)
	node1.condition = &condition{
		leftOperand:   operand{fieldIndex: &node1FieldIndex},
		conditionType: ConditionTypeEqual,
		rightOperand:  operand{valueLiteral: uint32ToBytes(1111)}}
	records, err := filterRecordsFromTables(openDb, []string{firstTable}, nil, []uint32{0})
	if err != nil {
		t.FailNow()
	}

	if len(records) != 2 {
		t.Fail()
	}

	for _, record := range records {
		// regardless of the record, there should be exactly 2 provenance fields
		if len(record.Provenance) != 2 {
			t.Fail()
		}

		// the order of records isn't deterministic, check for the value of each record
		// and verify the provenance is as expected
		if *record.Fields[0].ToKey() == 55 {
			// only one such record was inserted, no operator should be applied to it
			if record.Provenance[0].operator != ProvenanceOperatorNil {
				t.Fail()
			}
		}
		if *record.Fields[0].ToKey() == 1111 {
			// only one such record was inserted, no operator should be applied to it
			if record.Provenance[0].operator != ProvenanceOperatorPlus {
				t.Fail()
			}
			// 3 such records were inserted
			if len(record.Provenance[0].operands) != 3 {
				t.Fail()
			}

			if record.Provenance[0].ToKey() == nil {
				t.Fail()
			}
		}
	}
}

func testCreateMultiProvenanceDB(t *testing.T) (database, string, string) {
	// create a DB
	db, _ := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)
	firstTable := "firstTable"
	secondTable := "secondTable"

	firstProv, _ := dummyProvenance1()
	openDb, err := getOpenDB(db, &firstProv)
	if err != nil {
		t.FailNow()
	}

	// create 2 tables
	writeTestTable1(openDb, firstTable)
	writeTestTable2(openDb, secondTable, false, nil)

	if !addRecordTestTable1(openDb, firstTable, 105, []byte{0}) {
		t.FailNow()
	}
	if !addRecordTestTable1(openDb, firstTable, 12, []byte{0}) {
		t.FailNow()
	}

	if !addRecordTestTable2(openDb, secondTable, 1111, "Hello", 0) {
		t.FailNow()
	}
	if !addRecordTestTable2(openDb, secondTable, 55, "a", 1) {
		t.FailNow()
	}

	closeOpenDB(openDb)

	secondProv, _ := dummyProvenance2()
	openDb, err = getOpenDB(db, &secondProv)
	if err != nil {
		t.FailNow()
	}
	if !addRecordTestTable1(openDb, firstTable, 113, []byte{17, 176}) {
		t.FailNow()
	}
	if !addRecordTestTable1(openDb, firstTable, 105, []byte{0, 1, 2}) {
		t.FailNow()
	}

	closeOpenDB(openDb)
	return db, firstTable, secondTable
}

func testGetIntFieldValue(field Field, t *testing.T) int {
	intField, ok := field.(IntField)
	if !ok {
		t.FailNow()
	}

	return intField.Value
}

func testRecordProvenanceScoresByProvenanceType(record Record) proveTypeToScore {
	provByType := make(map[ProvenanceType]ProvenanceScore)
	for _, provFied := range record.Provenance {
		provByType[provFied.Type] = provFied.Score()
	}

	return provByType
}

func testDummyProvenanceAdditionAggregationFunc(operands []ProvenanceScore) ProvenanceScore {
	// sum the scores
	s := 0
	for _, op := range operands {
		s += int(op)
	}

	return ProvenanceScore(s)
}

// Adds records into a DB from multiple sources, causing the records to have different provenance
// values.
// Then retrieve records using both JOIN and SELECT to create a few different expressions from
// the used provenances.
// Finally, verify the score of each record correctly reflects the provenance of all the information
// that was used to retrieve it.
func TestProvenanceAggregation(t *testing.T) {
	db, table1, table2 := testCreateMultiProvenanceDB(t)

	firstProv, firstExpectedProv := dummyProvenance1()
	_, secondExpectedProv := dummyProvenance2()
	openDb, err := getOpenDB(db, &firstProv)
	if err != nil {
		t.FailNow()
	}

	// override the implemented aggregation funcs with a simple sum/multiplication aggregation funcs
	openDb.provSettings.additionAggregation = testDummyProvenanceAdditionAggregationFunc
	openDb.provSettings.multiplicationAggregation = ProvenanceAggregationMultiplication

	// perform a JOIN between the two table and retrieve all the unique values of the first
	// column in the first table
	records, err := filterRecordsFromTables(openDb, []string{table1, table2}, nil, []uint32{0})
	if err != nil {
		t.FailNow()
	}

	// we should have 3 unique values: 12, 113 and 105
	if len(records) != 3 {
		t.FailNow()
	}

	firstProvExpectedConnectionScore := int(firstExpectedProv.scores[ProvenanceTypeConnection])
	secondProvExpectedConnectionScore := int(secondExpectedProv.scores[ProvenanceTypeConnection])
	firstProvExpectedAuthenticationScore := int(firstExpectedProv.scores[ProvenanceTypeAuthentication])
	secondProvExpectedAuthenticationScore := int(secondExpectedProv.scores[ProvenanceTypeAuthentication])
	for _, record := range records {
		val := testGetIntFieldValue(record.Fields[0], t)
		scoreByType := testRecordProvenanceScoresByProvenanceType(record)
		recordAuthenticationScore := scoreByType[ProvenanceTypeAuthentication]
		recordConnectionScore := scoreByType[ProvenanceTypeConnection]

		if val == 12 {
			// The aggregation of all lines with this value should have the provenance:
			// <firstProv>*<firstProv> + <firstProv>*<firstProv>
			// In all current aggregation funcs this will be equivalent to the original provenance score.
			if int(recordAuthenticationScore) !=
				firstProvExpectedAuthenticationScore*firstProvExpectedAuthenticationScore*2 {
				t.Fail()
			}
			if int(recordConnectionScore) != firstProvExpectedConnectionScore*firstProvExpectedConnectionScore*2 {
				t.Fail()
			}
		}
		if val == 113 {
			// The aggregation of all lines with this value should have the provenance:
			// <secondProv>*<firstProv> + <secondProv>*<firstProv>
			// In all current aggregation funcs this will be equivalent to the minimum between the two
			if int(recordAuthenticationScore) !=
				secondProvExpectedAuthenticationScore*firstProvExpectedAuthenticationScore*2 {
				t.Fail()
			}
			if int(recordConnectionScore) !=
				secondProvExpectedConnectionScore*firstProvExpectedConnectionScore*2 {
				t.Fail()
			}
		}
		if val == 105 {
			// The aggregation of all lines with this value should have the provenance:
			// <firstProv>*<firstProv> + <firstProv>*<firstProv> + <secondProv>*<firstProv> + <secondProv>*<firstProv>
			// In all current aggregation funcs this will be equivalent to the maximum between the two
			if int(recordAuthenticationScore) != firstProvExpectedAuthenticationScore*firstProvExpectedAuthenticationScore*2+
				firstProvExpectedAuthenticationScore*secondProvExpectedAuthenticationScore*2 {
				t.Fail()
			}
			if int(recordConnectionScore) != firstProvExpectedConnectionScore*firstProvExpectedConnectionScore*2+
				firstProvExpectedConnectionScore*secondProvExpectedConnectionScore*2 {
				t.Fail()
			}
		}
	}
}

// Use a Cursor object to create a table and insert some records into it.
// Then, using this same cursor, retrieve all of the table's contents and make sure each
// record has the expected provenance.
func TestCursorProvenanceSanity(t *testing.T) {
	// initialize a DB that supports provenance
	db, _ := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)

	prov, expectedProvScores := dummyProvenance1()
	conn, err := Connect(db.id.identifyingString, &prov)
	if err != nil {
		t.FailNow()
	}

	cursor := conn.OpenCursor()
	cursor.Execute("create table newTable (columnA blob, columnB int)")
	cursor.Execute("insert into newTable values (ab34ffffffff, 32), (0000, 55), (abcdef, 38)")
	cursor.Execute("select columnA, columnB from newTable")
	records := cursor.FetchAll()
	if len(records) != 3 {
		t.Fail()
	}

	for _, record := range records {
		if len(record.Provenance) != 2 {
			t.FailNow()
		}

		scores := testRecordProvenanceScoresByProvenanceType(record)

		if scores[ProvenanceTypeConnection] !=
			ProvenanceScore(expectedProvScores.scores[ProvenanceTypeConnection]) {
			t.Fail()
		}
		if scores[ProvenanceTypeAuthentication] !=
			ProvenanceScore(expectedProvScores.scores[ProvenanceTypeAuthentication]) {
			t.Fail()
		}
	}
}

func testProvenanceGetRecordsUsingProvIterator(t *testing.T, db *openDB, tables []string,
	provType ProvenanceType) []Record {
	iterator, err := provenanceInitializeTableIterator(db, tables, provType)
	if err != nil {
		t.FailNow()
	}

	records := make([]Record, 0)
	record := iterator.next()
	for record != nil {
		records = append(records, record.record)
		record = iterator.next()
	}

	return records
}

type testRecordProvAggregationFunc func(Record) ProvenanceScore

func testProvenanceAssertRecordsAreInAscendingProvenanceOrder(t *testing.T, records []Record,
	aggregation testRecordProvAggregationFunc) {
	// verify the records are ordered by provenance score (ascending order)
	for i := 0; i < len(records)-1; i++ {
		prevScore := aggregation(records[i])
		currentScore := aggregation(records[i+1])

		if prevScore > currentScore {
			t.Fail()
		}
	}
}

func testProvenanceCheckProvenanceIteratorOnAllTypes(t *testing.T, db *openDB, tables []string,
	expectedAmount uint32) {
	for _, provField := range db.provFields {
		provType := provField.Type
		records := testProvenanceGetRecordsUsingProvIterator(t, db, tables, provType)
		if len(records) != int(expectedAmount) {
			t.Fail()
		}

		aggregation := func(record Record) ProvenanceScore {
			return testRecordProvenanceScoresByProvenanceType(record)[provType]
		}

		testProvenanceAssertRecordsAreInAscendingProvenanceOrder(t, records, aggregation)
	}
}

func TestIndexIterator(t *testing.T) {
	db, table := buildTable5()

	recordsInput := []Record{
		{Fields: []Field{IntField{100}, StringField{"aa"}, IntField{5}}},
		{Fields: []Field{IntField{102}, StringField{"aa"}, IntField{3}}},
		{Fields: []Field{IntField{50}, StringField{"aa"}, IntField{1}}},
		{Fields: []Field{IntField{12}, StringField{"aa"}, IntField{10}}},
	}
	for _, record := range recordsInput {
		_, err := addRecordToTable(db, table, record)
		if err != nil {
			t.FailNow()
		}
	}

	firstIterator, err := indexInitializeTableIterator(db, table, 0, false)
	if err != nil {
		t.FailNow()
	}

	records := make([]tableCurrentRecord, 0)
	record := firstIterator.next()
	for record != nil {
		records = append(records, *record)
		record = firstIterator.next()
	}
	if len(records) != len(records) {
		t.Fail()
	}
	expectedOrder := []int{3, 2, 0, 1}
	for i := 0; i < len(records); i++ {
		expected := recordsInput[expectedOrder[i]]
		actual := records[i].record
		if !recordsAreEqual(actual, expected) {
			t.Fail()
		}
	}
}

func testGenerateRandomBuffer() []byte {
	bufferSize := rand.Intn(100) + 1 // always make it non-empty
	randomBuffer := make([]byte, bufferSize)
	// the documentation pretty much gurantees this function will always succeed, no need to check return value
	rand.Read(randomBuffer)

	return randomBuffer
}

func testGenerateRandomStringField() StringField {
	randomBuffer := testGenerateRandomBuffer()
	return StringField{string(randomBuffer)}
}

func testGenerateRandomBlobField() BlobField {
	randomBuffer := testGenerateRandomBuffer()
	return BlobField{randomBuffer}
}

func testGenerateRandomIntField() IntField {
	integer := rand.Int31()
	return IntField{int(integer)}
}

func testGenerateRandomRecord(scheme tableScheme) Record {
	fields := make([]Field, 0)
	for _, header := range scheme.columns {
		if header.columnType == FieldTypeInt {
			fields = append(fields, testGenerateRandomIntField())
		}
		if header.columnType == FieldTypeString {
			fields = append(fields, testGenerateRandomStringField())
		}
		if header.columnType == FieldTypeBlob {
			fields = append(fields, testGenerateRandomBlobField())
		}
	}
	return Record{Fields: fields}
}

type testTableInfo struct {
	records []Record
	scheme  tableScheme
}

func testProvAddRecordToTable(t *testing.T, db *openDB, tables map[string]*testTableInfo, table string) {
	record := testGenerateRandomRecord(tables[table].scheme)
	_, err := addRecordToTable(db, table, record)
	if err != nil {
		t.FailNow()
	}
	record.Provenance = db.provFields
	tables[table].records = append(tables[table].records, record)
}

func testCreateMultiProvenanceDbScheme(t *testing.T) (database, map[string]*testTableInfo) {
	prov, _ := dummyProvenance1()
	db, _ := initializeTestDbInternal(IN_MEMORY_BUFFER_PATH_MAGIC, true)

	table1 := "table1"
	table2 := "table2"
	table3 := "table3"

	scheme1 := testTable1Scheme()
	scheme2 := testTable2Scheme()
	scheme3 := testTable3Scheme()
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDb)

	scheme1.provColumns = openDb.provenanceSchemeColumns()
	scheme2.provColumns = openDb.provenanceSchemeColumns()
	scheme3.provColumns = openDb.provenanceSchemeColumns()
	writeNewTable(openDb, table1, scheme1)
	writeNewTable(openDb, table2, scheme2)
	writeNewTable(openDb, table3, scheme3)

	tables := map[string]*testTableInfo{
		table1: {records: make([]Record, 0), scheme: scheme1},
		table2: {records: make([]Record, 0), scheme: scheme2},
		table3: {records: make([]Record, 0), scheme: scheme3},
	}

	return db, tables
}

func testCreateTablesWithMultiProvenances1(t *testing.T) (database, map[string]*testTableInfo) {
	db, tables := testCreateMultiProvenanceDbScheme(t)

	tableNames := make([]string, 0)
	for name := range tables {
		tableNames = append(tableNames, name)
	}

	prov1, _ := dummyProvenance1()
	openDb, err := getOpenDB(db, &prov1)
	if err != nil {
		t.FailNow()
	}

	// under prov1
	testProvAddRecordToTable(t, openDb, tables, tableNames[0])
	testProvAddRecordToTable(t, openDb, tables, tableNames[1])
	testProvAddRecordToTable(t, openDb, tables, tableNames[2])
	closeOpenDB(openDb)

	prov2, _ := dummyProvenance2()
	openDb, err = getOpenDB(db, &prov2)
	if err != nil {
		t.FailNow()
	}
	// under prov2
	testProvAddRecordToTable(t, openDb, tables, tableNames[1])
	closeOpenDB(openDb)

	prov3, _ := dummyProvenance3()
	openDb, err = getOpenDB(db, &prov3)
	if err != nil {
		t.FailNow()
	}
	defer closeOpenDB(openDb)
	// under prov3
	testProvAddRecordToTable(t, openDb, tables, tableNames[0])
	testProvAddRecordToTable(t, openDb, tables, tableNames[1])
	testProvAddRecordToTable(t, openDb, tables, tableNames[2])

	return db, tables
}

func testProvRandomizeProvenance(prov *DBProvenance) {
	prov.Auth.Password = testGenerateRandomStringField().Value
	prov.Auth.User = testGenerateRandomStringField().Value

	// limit the score to 700
	prov.Conn.Ipv4 = uint32(testGenerateRandomIntField().Value) % 700
}

// Create a DB with 3 tables, each with index enabled for all of its provenance fields.
// We then add 900 records into the DB, evenly split between the 3 tables.
// Each record is inserted using a different randomly generated provenance.
// Note that the total amount of joint records in this DB (the result of performing a JOIN
// on all 3 tables and retrieving all records) is 27 million records.
func testCreateTablesWithMultiProvenances2(t *testing.T) (database, map[string]*testTableInfo) {
	prov, _ := dummyProvenance1()
	db, tables := testCreateMultiProvenanceDbScheme(t)

	tableNames := make([]string, 0)
	for name := range tables {
		tableNames = append(tableNames, name)
	}

	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}

	for i := 0; i < 900; i++ {
		testProvRandomizeProvenance(&prov)
		closeOpenDB(openDb)
		openDb, err = getOpenDB(db, &prov)
		if err != nil {
			t.FailNow()
		}

		// each iteration add a record into one of the tables in round-robin fashion
		testProvAddRecordToTable(t, openDb, tables, tableNames[i%3])
	}

	closeOpenDB(openDb)
	return db, tables
}

// create 3 tables, add a few records in each of them, using 3 different provenances.
// then, for each provenance type, create a provenance iterator and retrieve all records
// using it.
// verify the amount of records retrieved is as expected, and that their aggregated provenance
// score is in ascending order (as expected).
func TestProvIndexIterator(t *testing.T) {
	prov, _ := dummyProvenance1()
	db, tables := testCreateTablesWithMultiProvenances1(t)
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}

	tableNames := make([]string, 0)
	expectedAmount := 1
	for name := range tables {
		tableNames = append(tableNames, name)
		expectedAmount *= len(tables[name].records)
	}

	testProvenanceCheckProvenanceIteratorOnAllTypes(t, openDb, tableNames, uint32(expectedAmount))
	openDb.provSettings.multiplicationAggregation = ProvenanceAggregationMax
	testProvenanceCheckProvenanceIteratorOnAllTypes(t, openDb, tableNames, uint32(expectedAmount))
}

func TestProvGetTopRecordManyRecords(t *testing.T) {
	prov, _ := dummyProvenance1()
	db, tables := testCreateTablesWithMultiProvenances2(t)
	openDb, err := getOpenDB(db, &prov)
	if err != nil {
		t.FailNow()
	}

	tableNames := make([]string, 0)
	expectedAmount := 1
	for name := range tables {
		tableNames = append(tableNames, name)
		expectedAmount *= len(tables[name].records)
	}

	aggregationFuncs := []ProvenanceAggregationFunc{
		ProvenanceAggregationAverage,
		ProvenanceAggregationMin,
		ProvenanceAggregationMax,
		ProvenanceAggregationMultiplication,
	}

	// verify the function always return records in ascending provenance order, regarding of the aggregation
	// function used
	for _, aggregationFunc := range aggregationFuncs {
		records, err := provenanceGetTopRecords(openDb, tableNames, aggregationFunc, 11, nil)
		if err != nil {
			t.FailNow()
		}

		if len(records) != 11 {
			t.Fail()
		}

		aggregation := func(record Record) ProvenanceScore {
			scores := make([]ProvenanceScore, len(record.Provenance))
			for i := 0; i < len(record.Provenance); i++ {
				scores[i] = record.Provenance[i].Score()
			}

			return aggregationFunc(scores)
		}
		testProvenanceAssertRecordsAreInAscendingProvenanceOrder(t, records, aggregation)
	}

}
