package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on :6379")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read array header like "*1\r\n"
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "*") {
			conn.Write([]byte("-ERR Protocol error\r\n"))
			return
		}

		// Number of elements
		numElems, _ := strconv.Atoi(line[1:])

		// Read each bulk string
		var parts []string
		for i := 0; i < numElems; i++ {
			// Read $<len>
			_, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			// Read actual data
			arg, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts = append(parts, strings.TrimSpace(arg))
		}

		// Handle command
		if len(parts) > 0 {
			cmd := strings.ToUpper(parts[0])
			switch cmd {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(parts) > 1 {
					msg := parts[1]
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)))
				} else {
					conn.Write([]byte("$0\r\n\r\n"))
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
