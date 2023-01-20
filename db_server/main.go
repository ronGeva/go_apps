package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/ronGeva/go_apps/go_db"

	"github.com/gorilla/websocket"
)

// We'll need to define an Upgrader
// this will require a Read and Write buffer size
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type queryResult struct {
	records []go_db.Record
}

type queryResponse struct {
	Success bool
	Error   string
	Data    [][]string
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

func performRequest(db string, query string) ([]go_db.Record, error) {
	conn, err := go_db.Connect(db)
	if err != nil {
		return nil, err
	}
	cursor := conn.OpenCursor()
	err = cursor.Execute(query)
	if err != nil {
		return nil, err
	}

	return cursor.FetchAll(), nil
}

func handleQueryRequest(msg map[string]interface{}) (*queryResult, error) {
	db, err := getStringValue(msg, "db")
	if err != nil {
		return nil, err
	}
	query, err := getStringValue(msg, "query")
	if err != nil {
		return nil, err
	}

	log.Printf("db=%s, query=%s", *db, *query)
	records, err := performRequest(*db, *query)

	return &queryResult{records: records}, err
}

func handleCreationRequest(msg map[string]interface{}) (*queryResult, error) {
	dbPath, err := getStringValue(msg, "db")
	if err != nil {
		return nil, err
	}
	go_db.InitializeDB(*dbPath)
	return nil, nil
}

func handleNewMessage(conn *websocket.Conn) (*queryResult, error) {
	// read in a message
	_, p, err := conn.ReadMessage()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	var msg map[string]interface{}

	// print out that message for clarity
	err = json.Unmarshal(p, &msg)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	msgType, err := getStringValue(msg, "type")
	if err != nil {
		return nil, err
	}
	switch *msgType {
	case "create":
		return handleCreationRequest(msg)
	case "query":
		return handleQueryRequest(msg)
	default:
		return nil, fmt.Errorf("invalid request type %s", *msgType)
	}
}

func sendResult(conn *websocket.Conn, err error, res *queryResult) {
	var result queryResponse
	result.Success = (err == nil)
	if result.Success {
		if res != nil {
			allRecordsStr := make([][]string, 0)
			for _, record := range res.records {
				recordStr := make([]string, 0)
				for _, f := range record.Fields {
					recordStr = append(recordStr, f.Stringify())
				}
				allRecordsStr = append(allRecordsStr, recordStr)
			}
			result.Data = allRecordsStr
		}
	} else {
		result.Error = err.Error()
	}
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return
	}

	if err := conn.WriteMessage(1, data); err != nil {
		log.Println(err)
		return
	}
}

// define a reader which will listen for
// new messages being sent to our WebSocket
// endpoint
func reader(conn *websocket.Conn) {
	for {
		// read in a message
		res, err := handleNewMessage(conn)
		sendResult(conn, err, res)
	}
}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Client Connected")

	// listen indefinitely for new messages coming
	// through on our WebSocket connection
	reader(ws)
}

func main() {
	fmt.Println("Starting server...")

	http.HandleFunc("/ws", wsEndpoint)
	err := http.ListenAndServe(":5678", nil)
	if err != nil {
		fmt.Println("Encountered an error")
		return
	}
}
