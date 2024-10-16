package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// We'll need to define an Upgrader
// this will require a Read and Write buffer size
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
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
	handleNewClient(ws)
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
