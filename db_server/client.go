package main

import (
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
)

type client struct {
	conn *websocket.Conn
}

type queryResponse struct {
	Success bool
	Error   string
	Type    string
	Data    [][]string
}

func (c *client) handleNewMessage() (*queryResult, error) {
	// read in a message
	_, p, err := c.conn.ReadMessage()
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

	r, err := parseRequest(msg)
	if err != nil {
		return nil, err
	}

	return r.handle()
}

func (c *client) sendResult(err error, res *queryResult) {
	var result queryResponse
	result.Success = (err == nil)
	if result.Success {
		if res != nil {
			result.Type = res.resultType

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

	if err := c.conn.WriteMessage(1, data); err != nil {
		log.Println(err)
		return
	}
}

func handleNewClient(conn *websocket.Conn) {
	c := client{conn: conn}
	for {
		// read in a message
		res, err := c.handleNewMessage()
		c.sendResult(err, res)
	}
}
