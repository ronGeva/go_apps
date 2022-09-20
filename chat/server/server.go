package main

import (
	"fmt"
	"net"
)

func serveClient(client net.Conn, broadcast_chan chan<- []byte) {

	var err error
	current_msg_len := 0

	for {
		data := make([]byte, 200)                // create a 200 bytes empty buffer
		current_msg_len, err = client.Read(data) // TODO: check length?
		if err != nil {
			fmt.Println("Failed to read from remote address", client.RemoteAddr())
			return
		}
		broadcast_chan <- data[:current_msg_len]
	}
}

func broadcastMessages(incoming_message <-chan []byte, clients *[]net.Conn) {
	for {
		new_message := <-incoming_message
		for _, client := range *clients {
			client.Write(new_message) // TODO: don't send message to client who sent it
		}
	}
}

func main() {
	fmt.Println("Starting the server...")

	ln, _ := net.Listen("tcp", ":8080")

	broadcast_chan := make(chan []byte)
	var clients []net.Conn
	go broadcastMessages(broadcast_chan, &clients)
	for {
		client, err := ln.Accept()
		if err != nil {
			fmt.Println("Failed to accept client")
			continue
		}
		clients = append(clients, client)
		go serveClient(client, broadcast_chan)
	}

	// data := make([]byte, 200) // create a 200 bytes empty buffer
	// _, err = client.Read(data)
	// if err != nil {
	// 	fmt.Println("Failed to read data from client")
	// 	return
	// }
	// msg := chat_framework.HelloMessage{}
	// copy(msg.Header[:], data[:4])
	// msg.Greeting = string(data[4:])

	// fmt.Println("User msg: ", msg)
}
