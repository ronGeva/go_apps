package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"net"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/ronGeva/go_apps/go_db"
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

type clientProvenance struct {
	auth *go_db.ProvenanceAuthentication
	conn *go_db.ProvenanceConnection
}

func parseAuthentication(msg map[string]interface{}) *go_db.ProvenanceAuthentication {
	username := ""
	password := ""

	userValue, err := getStringValue(msg, "username")
	if err == nil {
		username = *userValue
	}

	passValue, err := getStringValue(msg, "password")
	if err == nil {
		password = *passValue
	}

	return &go_db.ProvenanceAuthentication{User: username, Password: password}
}

func (c *client) connectionProvenance() *go_db.ProvenanceConnection {
	remote_address := c.conn.RemoteAddr().String()
	addressParts := strings.Split(remote_address, ":")
	if len(addressParts) != 2 {
		return nil
	}

	ip := net.ParseIP(addressParts[0])
	ipv4 := ip.To4()
	if ipv4 == nil {
		return nil
	}

	ipv4NumericVal := binary.BigEndian.Uint32(ipv4)
	return &go_db.ProvenanceConnection{Ipv4: ipv4NumericVal}
}

func (c *client) provenance(msg map[string]interface{}) clientProvenance {
	return clientProvenance{conn: c.connectionProvenance(), auth: parseAuthentication(msg)}
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

	prov := c.provenance(msg)
	r, err := parseRequest(msg, prov)
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
				for _, f := range record.Provenance {
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
