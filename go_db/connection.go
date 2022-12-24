package go_db

type Connection struct {
	db database
}

type Cursor struct {
	conn    *Connection
	records []Record
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

func (cursor *Cursor) Execute(sql string) error {
	queryType, err := parseQueryType(sql)
	if err != nil {
		return nil
	}
	openDatabse := getOpenDB(cursor.conn.db)
	defer closeOpenDB(&openDatabse)
	if queryType == QueryTypeSelect {
		err := ExecuteSelectQuery(&openDatabse, cursor, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cursor *Cursor) FetchAll() []Record {
	return cursor.records
}
