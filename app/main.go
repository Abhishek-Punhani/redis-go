package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Entry stores value + optional expiry
type Entry struct {
	Value  string
	Expiry time.Time // zero means no expiry
}

// Thread-safe store
var (
	store = make(map[string]Entry)
	mu    sync.RWMutex
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
		// Read array header: *<count>
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
		numElems, err := strconv.Atoi(line[1:])
		if err != nil {
			conn.Write([]byte("-ERR invalid multibulk length\r\n"))
			return
		}

		// Read arguments
		var parts []string
		for i := 0; i < numElems; i++ {
			// Read $<len>
			_, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			// Read data
			arg, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts = append(parts, strings.TrimSpace(arg))
		}

		if len(parts) == 0 {
			continue
		}

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

		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'SET'\r\n"))
				continue
			}
			key := parts[1]
			value := parts[2]
			expiry := time.Time{} // No expiry by default
			if len(parts) > 4 && strings.ToUpper(parts[3]) == "PX" {
				expiryMs, err := strconv.Atoi(parts[4])
				if err != nil {
					conn.Write([]byte("-ERR invalid expiry value\r\n"))
					continue
				}
				expiry = time.Now().Add(time.Duration(expiryMs) * time.Millisecond)
			}
				
			mu.Lock()
			store[key] = Entry{Value: value,Expiry: expiry} // No expiry for now
			mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
				continue
			}
			key := parts[1]
			mu.RLock()
			entry, exists := store[key]
			mu.RUnlock()

			if !exists || (entry.Expiry.After(time.Time{}) && time.Now().After(entry.Expiry)) {
				conn.Write([]byte("$-1\r\n")) // Null bulk string
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.Value), entry.Value)))
			}

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
