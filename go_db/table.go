package go_db

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

type table struct {
	scheme    tableScheme
	recordIds []recordID
}

func createTable(scheme tableScheme) *table {
	return &table{scheme: scheme, recordIds: make([]recordID, 0)}
}

func (t *table) addRecord(record *Record) {
	// write record to DB, get unique ID for it
	recordID := recordID{len(t.recordIds)}
	t.recordIds = append(t.recordIds, recordID)
}
