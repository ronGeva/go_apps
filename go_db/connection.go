package go_db

type Connection struct {
	db database
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
}

func Connect(path string) (Connection, error) {
	db := database{id: databaseUniqueID{ioType: LocalFile, identifyingString: path}}
	return Connection{db: db}, nil
}

func (conn *Connection) OpenCursor() Cursor {
	return Cursor{conn: conn}
}

func ExecuteSelectQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseSelectQuery(openDatabse, sql)
	if err != nil {
		return err
	}
	records := filterRecordsFromTableInternal(openDatabse, query.tableID, query.condition)
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
		addRecordOpenDb(openDatabse, query.tableID, record)
	}

	return nil
}

func ExecuteDeleteQuery(openDatabse *openDB, cursor *Cursor, sql string) error {
	query, err := parseDeleteQuery(openDatabse, sql)
	if err != nil {
		return err
	}

	return deleteRecordsFromTableInternal(openDatabse, query.tableID, query.condition)
}

func ExecuteCreateQuery(openDatabase *openDB, cursor *Cursor, sql string) error {
	query, err := parseCreateQuery(openDatabase, sql)
	if err != nil {
		return err
	}
	writeNewTableLocalFileInternal(openDatabase, query.tableID, query.scheme)
	return nil
}

func (cursor *Cursor) Execute(sql string) error {
	queryType, err := parseQueryType(sql)
	if err != nil {
		return nil
	}
	openDatabse := getOpenDB(cursor.conn.db)
	defer closeOpenDB(&openDatabse)

	err = QUERY_TYPE_TO_FUNC[queryType](&openDatabse, cursor, sql)
	if err != nil {
		return err
	}

	return nil
}

func (cursor *Cursor) FetchAll() []Record {
	return cursor.records
}
