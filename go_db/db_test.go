package go_db

import (
	"fmt"
	"os"
	"testing"
)

func initializeTestDB1() (database, string) {
	path := "C:\\temp\\my_db"
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	initializeDB(path)
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	return db, tableID
}

func buildTable1() (database, string) {
	db, tableID := initializeTestDB1()

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

func buildTable2() (database, string) {
	db, tableID := initializeTestDB1()

	firstColumn := columndHeader{"columnA", FieldTypeInt}
	secondColumn := columndHeader{"columnB", FieldTypeInt}
	scheme := tableScheme{[]columndHeader{firstColumn, secondColumn}}
	writeNewTable(db, tableID, scheme)
	fields := []Field{&IntField{5}, &IntField{11}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{13}, &IntField{30}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	return db, tableID
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
	node2 := conditionNode{condition: &condition{1, ConditionTypeEqual, uint32ToBytes(11)}}
	return conditionNode{operator: ConditionOperatorAnd, operands: []*conditionNode{&node1, &node2}}
}

func TestRecordFilter(t *testing.T) {
	cond := buildConditionTreeForTest()
	db, tableID := buildTable2()
	records := filterRecordsFromTable(db, tableID, cond)
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

func TestParseSelectQuery1(t *testing.T) {
	db, _ := buildTable2()
	openDB := getOpenDB(db)
	defer closeOpenDB(&openDB)

	//sql := "select * from table1\r\n where\t columnA = 5   \r\n "
	sql := "Select columnA, columnB from newTable where ((columnA = 5) and (columnB = 13)) or ((columnA = 7) and (not (columnB = 30)))"
	query, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}
	printConditionNode(query.condition, 0)
}

func TestParseSelectQuery2(t *testing.T) {
	db, _ := buildTable2()
	openDB := getOpenDB(db)
	defer closeOpenDB(&openDB)

	sql := "Select columnA, columnB from newTable where columnA = 7 and columnb = 13 or columna = 5 and columna = 10 and not columnb = 5"
	query, err := parseSelectQuery(&openDB, sql)
	if err != nil {
		t.Fail()
	}
	printConditionNode(query.condition, 0)
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
