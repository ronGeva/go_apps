package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/ronGeva/go_apps/go_db"
)

// TODO: make this configurable
const DB_DIRECTORY = "db_directory"

type queryResult struct {
	records     []go_db.Record
	columnNames []string
	resultType  string
}

type request interface {
	handle() (*queryResult, error)
}

type createDbRequest struct {
	path string
}

func (r *createDbRequest) handle() (*queryResult, error) {
	go_db.InitializeDB(r.path, true)
	return &queryResult{resultType: "DBCreation"}, nil
}

type listDbsRequest struct {
}

func (r *listDbsRequest) getDBDirectory() (*string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("error getting executable path: %v", err)
	}

	dbDirectory := filepath.Join(filepath.Dir(exePath), DB_DIRECTORY)

	_, err = os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		err = os.Mkdir(dbDirectory, os.ModePerm)
	}

	if err != nil {
		return nil, err
	}

	return &dbDirectory, nil
}

func (r *listDbsRequest) handle() (*queryResult, error) {
	dbDirectory, err := r.getDBDirectory()
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(*dbDirectory)
	if err != nil {
		return nil, err
	}
	records := make([]go_db.Record, 0)
	for _, e := range entries {
		records = append(records, go_db.Record{Fields: []go_db.Field{go_db.StringField{e.Name()}}})
	}
	return &queryResult{records: records, resultType: "DBs"}, nil
}

type queryDbsRequest struct {
	db    string
	query string
	prov  *go_db.DBProvenance
}

func (r *queryDbsRequest) handle() (*queryResult, error) {
	conn, err := go_db.Connect(r.db, r.prov)
	if err != nil {
		return nil, err
	}
	cursor := conn.OpenCursor()
	err = cursor.Execute(r.query)
	if err != nil {
		return nil, err
	}

	records := cursor.FetchAll()
	columnNames := cursor.ColumnNames()
	return &queryResult{records: records, resultType: "query", columnNames: columnNames}, nil
}

func getStringValue(msg map[string]interface{}, key string) (*string, error) {
	v, ok := msg[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in message", key)
	}

	switch v.(type) {
	case string:
		value, _ := v.(string)
		return &value, nil
	default:
		return nil, fmt.Errorf("wrong type of value \"db\"")
	}
}

func createCreateDbRequest(msg map[string]interface{}) (request, error) {
	dbName, err := getStringValue(msg, "db")
	if err != nil {
		return nil, err
	}

	if strings.Contains(*dbName, "\\") {
		return nil, fmt.Errorf("db name includes invalid character \\: %s", *dbName)
	}

	dbPath := path.Join(DB_DIRECTORY, *dbName)
	if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("db file with name %s already exists", *dbName)
	}

	return &createDbRequest{path: dbPath}, nil
}

func createListDbsRequest(msg map[string]interface{}) (request, error) {
	return &listDbsRequest{}, nil
}

func getAggregationFunc(msg map[string]interface{}) *go_db.ProvenanceAggregationFunc {
	var aggregationNameToFunc = map[string]go_db.ProvenanceAggregationFunc{
		"max":      go_db.ProvenanceAggregationMax,
		"min":      go_db.ProvenanceAggregationMin,
		"average":  go_db.ProvenanceAggregationAverage,
		"multiply": go_db.ProvenanceAggregationMultiplication,
	}

	aggregationName, err := getStringValue(msg, "aggregation")
	if err != nil {
		return nil
	}

	aggregation, ok := aggregationNameToFunc[*aggregationName]
	if !ok {
		return nil
	}

	return &aggregation
}

func createQueryDbsRequest(msg map[string]interface{}, prov *go_db.DBProvenance) (request, error) {
	db, err := getStringValue(msg, "db")
	if err != nil {
		return nil, err
	}
	*db = path.Join(DB_DIRECTORY, *db)
	query, err := getStringValue(msg, "query")
	if err != nil {
		return nil, err
	}

	log.Printf("db=%s, query=%s", *db, *query)

	sourceIP, err := getStringValue(msg, "source_ip")
	if err == nil && len(*sourceIP) > 0 {
		connProv := ipToProvenanceConnection(*sourceIP)
		if connProv != nil {
			prov.Conn = *connProv
		}
	}

	multiProvAggregation := getAggregationFunc(msg)
	if multiProvAggregation != nil {
		settings := go_db.DEFAULT_PROVENANCE_SETTINGS
		settings.MultiProvAggregation = *multiProvAggregation
		prov.Settings = &settings
	}

	return &queryDbsRequest{query: *query, db: *db, prov: prov}, nil
}

func parseRequest(msg map[string]interface{}, prov *go_db.DBProvenance) (request, error) {
	msgType, err := getStringValue(msg, "type")
	if err != nil {
		return nil, err
	}
	switch *msgType {
	case "create":
		return createCreateDbRequest(msg)
	case "query":
		return createQueryDbsRequest(msg, prov)
	case "queryDBs":
		return createListDbsRequest(msg)
	default:
		return nil, fmt.Errorf("invalid request type %s", *msgType)
	}
}
