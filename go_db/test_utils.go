package go_db

// This file is a workaround to more easily test the functions during development

type TestScheme struct {
	ColumnNames []string
	ColumnTypes []FieldType
}

func CreateNewTableForTest(tableID string, newTableScheme TestScheme, path string) {
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	scheme := tableScheme{}
	for i, _ := range newTableScheme.ColumnNames {
		scheme.columns = append(scheme.columns,
			columndHeader{columnName: newTableScheme.ColumnNames[i],
				columnType: newTableScheme.ColumnTypes[i]})
	}
	WriteNewTable(db, tableID, scheme)
}

func AddRecordToTable(db database, tableID string, record Record) {
	addRecordToTable(db, tableID, record)
}

func ReadAllRecords(db database, tableID string) []Record {
	return readAllRecords(db, tableID)
}
