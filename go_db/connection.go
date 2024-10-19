package go_db

import (
	"fmt"
	"sort"
)

type Connection struct {
	db   database
	prov *DBProvenance
}

type Cursor struct {
	conn            *Connection
	records         []Record
	columnNames     []string
	provenanceNames []string
}

var QUERY_TYPE_TO_FUNC = map[queryType]func(*openDB, *Cursor, string) error{
	QueryTypeSelect: ExecuteSelectQuery,
	QueryTypeInsert: ExecuteInsertQuery,
	QueryTypeDelete: ExecuteDeleteQuery,
	QueryTypeCreate: ExecuteCreateQuery,
	QueryTypeUpdate: ExecuteUpdateQuery,
}

func Connect(path string, provenance *DBProvenance) (Connection, error) {
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}

	return Connection{db: db, prov: provenance}, nil
}

func (conn *Connection) OpenCursor() Cursor {
	return Cursor{conn: conn}
}

func orderRecords(records []Record, pivot uint32) {
	// TODO: check pivot is valid - we need to make sure it exists and has a less method

	// sorts in place
	sort.Slice(records, func(i, j int) bool {
		firstField := records[i].Fields[pivot]
		otherValue := records[j].Fields[pivot].serialize()
		// TODO: handle errors
		isLess, _ := checkLess(firstField, otherValue)
		return isLess
	})
}

func selectQueryRetrieveRecords(db *openDB, query *selectQuery) ([]Record, error) {
	if query.bestAmount != nil {
		if !db.header.provenanceOn {
			return nil, fmt.Errorf("'best' keyword requires provenance to be turned on for the database")
		}
		records, err := ProvenanceGetTopRecords(db, query.tableIDs, provenanceAggregationMinFunc,
			*query.bestAmount, query.condition)
		if err != nil {
			return nil, err
		}
		return records, nil
	} else {
		return filterRecordsFromTables(db, query.tableIDs, query.condition, query.columns)
	}
}

func ExecuteSelectQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseSelectQuery(openDatabse, sql)
	if err != nil {
		return err
	}

	records, err := selectQueryRetrieveRecords(openDatabse, query)
	if err != nil {
		return err
	}

	if query.orderBy != nil {
		orderRecords(records, *query.orderBy)
	}

	cursor.records = records
	cursor.columnNames = query.columnNames
	cursor.provenanceNames = openDatabse.provenanceNames()
	return nil
}

func ExecuteInsertQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseInsertQuery(openDatabse, sql)
	if err != nil {
		return err
	}
	// TODO: optimize this for a slice of records instead of inserting each one
	for _, record := range query.records {
		addRecordToTable(openDatabse, query.tableID, record)
	}

	return nil
}

func ExecuteDeleteQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseDeleteQuery(openDatabse, sql)
	if err != nil {
		return err
	}

	return deleteRecordsFromTable(openDatabse, query.tableID, query.condition)
}

func ExecuteCreateQuery(openDatabase *openDB, cursor *Cursor, sql string) error {
	query, err := parseCreateQuery(openDatabase, sql)
	if err != nil {
		return err
	}
	return writeNewTable(openDatabase, query.tableID, query.scheme)
}

func ExecuteUpdateQuery(openDatabase *openDB, cursor *Cursor, sql string) error {
	query, err := parseUpdateQuery(openDatabase, sql)
	if err != nil {
		return err
	}
	return updateRecordsViaCondition(openDatabase, query.tableID, query.condition, query.update)
}

func (cursor *Cursor) Execute(sql string) error {
	sql = normalizeQuery(sql)
	queryType, err := parseQueryType(sql)
	if err != nil {
		return nil
	}

	openDatabase, err := getOpenDB(cursor.conn.db, cursor.conn.prov)
	if err != nil {
		return err
	}

	defer closeOpenDB(openDatabase)

	err = QUERY_TYPE_TO_FUNC[queryType](openDatabase, cursor, sql)
	if err != nil {
		return err
	}

	return nil
}

func (cursor *Cursor) FetchAll() []Record {
	return cursor.records
}

func (cursor *Cursor) ColumnNames() []string {
	return append(cursor.columnNames, cursor.provenanceNames...)
}
