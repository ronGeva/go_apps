package main

import (
	"go_db"
	"os"
)

func main() {
	path := "C:\\temp\\my_db"
	tableID := "newTable"
	os.Remove(path) // don't care about errors

	go_db.InitializeDB(path)
	os.Chmod(path, 0600) // this is a bug workaround, file is always created as read-only
	columnNames := []string{"columnA", "columnB"}
	columnTypes := []go_db.FieldType{go_db.FieldTypeInt, go_db.FieldTypeInt}
	scheme := go_db.TestScheme{ColumnNames: columnNames, ColumnTypes: columnTypes}
	go_db.CreateNewTableForTest("newTable", scheme, "C:\\temp\\my_db")
	fields := []go_db.Field{&go_db.IntField{5}, &go_db.IntField{1024}}
	newRecord := go_db.MakeRecord(fields)
	db := go_db.GetDB(go_db.GenerateDBUniqueID(path))
	go_db.AddRecordToTable(db, tableID, newRecord)
	fields = []go_db.Field{&go_db.IntField{13}, &go_db.IntField{-12}}
	newRecord = go_db.MakeRecord(fields)
	go_db.AddRecordToTable(db, tableID, newRecord)

	records := go_db.ReadAllRecords(db, tableID)
	println(records)
}
