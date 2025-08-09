package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}

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

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		text := scanner.Text()
		if strings.TrimSpace(text) == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		}else{
			inpString := strings.Split(text, " ")
			if len(inpString) > 1 && inpString[0] == "ECHO" {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(inpString[1]), inpString[1])
			}
		}
		//conn.Write([]byte("-Error invalid command: '" + text + "'\r\n"))
	}
}