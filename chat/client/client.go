package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func printMessagesToScreen(sock net.Conn) {
	var string_builder strings.Builder
	var err error
	data := make([]byte, 200) // create a 200 bytes empty buffer
	current_msg_len := 0
	for {
		// clean buffer from last message
		for i := 0; i < current_msg_len; i++ {
			data[i] = 0
		}

		current_msg_len, err = sock.Read(data)

		if err != nil {
			// TODO: trace error
			return
		}
		string_builder.Reset()
		string_builder.Write(data)
		fmt.Println(string_builder.String())
	}
}

func main() {

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Failed to connect to server")
		return
	}

	go printMessagesToScreen(conn)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("==>")
		text, _ := reader.ReadString('\n')
		conn.Write([]byte(text))
	}
}
