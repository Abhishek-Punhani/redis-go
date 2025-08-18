package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Value  string
	Expiry time.Time
}

var (
	store      = make(map[string]Entry)
	mu         sync.RWMutex
	list_store = make(map[string][]string)

	// Per-list locks for fine-grained locking on lists
	listLocks   = make(map[string]*sync.Mutex)
	listLocksMu sync.Mutex
)

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

var (
	streams   = make(map[string][]StreamEntry)
	streamsMu sync.RWMutex
)

func main() {
	ln := startServer(":6379")
	defer ln.Close()
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

func startServer(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	return ln
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		parts, err := readCommand(reader, conn)
		if err != nil {
			return
		}
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "PING":
			handlePing(conn)
		case "ECHO":
			handleEcho(conn, parts)
		case "SET":
			handleSet(conn, parts)
		case "GET":
			handleGet(conn, parts)
		case "RPUSH":
			handleRPush(conn, parts)
		case "LPUSH":
			handleLPush(conn, parts)
		case "LRANGE":
			handleLRange(conn, parts)
		case "LLEN":
			handleLLen(conn, parts)
		case "LPOP":
			handleLPop(conn, parts)
		case "BLPOP":
			handleBLPop(conn, parts)
		case "TYPE":
			handleType(conn, parts)
		case "XADD":
			handleXAdd(conn, parts)
		case "XRANGE":
			handleXRange(conn, parts)
		case "XREAD":
			handleXRead(conn, parts)
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
