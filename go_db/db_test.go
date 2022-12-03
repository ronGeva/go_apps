package go_db

import (
	"os"
	"testing"
)

func TestFullFlow(t *testing.T) {
	path := "C:\\temp\\my_db"
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	initializeDB(path)
	firstColumn := columndHeader{"columnA", FieldTypeInt}
	secondColumn := columndHeader{"columnB", FieldTypeBlob}
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	scheme := tableScheme{[]columndHeader{firstColumn, secondColumn}}
	writeNewTable(db, tableID, scheme)
	fields := []Field{&IntField{5}, &BlobField{make([]byte, 10)}}
	newRecord := MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)
	fields = []Field{&IntField{13}, &BlobField{make([]byte, 5000)}}
	newRecord = MakeRecord(fields)
	addRecordToTable(db, tableID, newRecord)

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
