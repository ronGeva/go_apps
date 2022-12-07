package go_db

import (
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
	node3.left = &node1
	node3.right = &node2
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
	node5.left = &node4
	node5.operator = ConditionOperatorNot
	if checkAllConditions(node5, record) {
		t.Fail()
	}
}

func buildConditionTree() conditionNode {
	node1 := conditionNode{condition: &condition{0, ConditionTypeEqual, uint32ToBytes(5)}}
	node2 := conditionNode{condition: &condition{1, ConditionTypeEqual, uint32ToBytes(11)}}
	return conditionNode{operator: ConditionOperatorAnd, left: &node1, right: &node2}
}

func TestRecordFilter(t *testing.T) {
	cond := buildConditionTree()
	db, tableID := buildTable2()
	records := filterRecordsFromTable(db, tableID, cond)
	if len(records) != 1 {
		t.Fail()
	}
}
