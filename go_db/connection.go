package go_db

import "sort"

type Connection struct {
	db   database
	auth *ProvenanceAuthentication
	conn *ProvenanceConnection
}

type Cursor struct {
	conn    *Connection
	records []Record
}

var QUERY_TYPE_TO_FUNC = map[queryType]func(*openDB, *Cursor, string) error{
	QueryTypeSelect: ExecuteSelectQuery,
	QueryTypeInsert: ExecuteInsertQuery,
	QueryTypeDelete: ExecuteDeleteQuery,
	QueryTypeCreate: ExecuteCreateQuery,
	QueryTypeUpdate: ExecuteUpdateQuery,
}

func Connect(path string, auth *ProvenanceAuthentication, conn *ProvenanceConnection) (Connection, error) {
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	return Connection{db: db, auth: auth, conn: conn}, nil
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

func ExecuteSelectQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseSelectQuery(openDatabse, sql)
	if err != nil {
		return err
	}
	records, err := filterRecordsFromTables(openDatabse, query.tableIDs, query.condition, query.columns)
	if err != nil {
		return err
	}

	if query.orderBy != nil {
		orderRecords(records, *query.orderBy)
	}

	cursor.records = records
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

	var prov *OpenDBProvenance = nil
	if cursor.conn.auth != nil && cursor.conn.conn != nil {
		prov = &OpenDBProvenance{auth: *cursor.conn.auth, conn: *cursor.conn.conn}
	}

	openDatabase, err := getOpenDB(cursor.conn.db, prov)
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
