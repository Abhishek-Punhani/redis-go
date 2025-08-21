package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Value  string
	Expiry time.Time
}
type StreamEntry struct {
	ID     string
	Fields map[string]string
}
type clientState struct {
	inMulti bool
	queue   [][]string
}
type Config struct {
	Port         string
	Role         string
	MasterHost   string
	MasterPort   string
	replicaConns map[string]net.Conn
	ReplicaMu    sync.Mutex
	ReplOffset   int64
}

var (
	store       = make(map[string]Entry)
	mu          sync.RWMutex
	list_store  = make(map[string][]string)
	listLocks   = make(map[string]*sync.Mutex)
	listLocksMu sync.Mutex
)

var (
	streams   = make(map[string][]StreamEntry)
	streamsMu sync.RWMutex
)

var WRITE_COMMANDS = map[string]bool{
	"SET":    true,
	"RPUSH":  true,
	"LPUSH":  true,
	"XADD":   true,
	"INCR":   true,
	"DEL":    true,
	"LPUSHX": true,
	"RPUSHX": true,
}

func main() {
	args := os.Args[1:]
	config := Config{
		Port:         "6379",
		Role:         "master",
		MasterHost:   "",
		MasterPort:   "6379",
		replicaConns: make(map[string]net.Conn),
	}
	for i := 0; i < len(args); i++ {
		if args[i] == "--port" && i+1 < len(args) {
			config.Port = args[i+1]
			i++
		} else if args[i] == "--replicaof" {
			config.Role = "slave"
			config.MasterHost, config.MasterPort = strings.Split(args[i+1], " ")[0], strings.Split(args[i+1], " ")[1]
			i++
			go connectToMaster(&config)
		}
	}
	ln := startServer(":" + config.Port)
	defer ln.Close()
	fmt.Printf("Listening on :%s\n", config.Port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		state := &clientState{
			inMulti: false,
			queue:   nil,
		}
		go handleConnection(conn, state, &config)
	}
}

func startServer(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	return ln
}

func handleCommand(conn net.Conn, parts []string, config *Config) {
	cmd := strings.ToUpper(parts[0])
	switch cmd {
	case "REPLCONF":
		hadleReplconf(conn, parts, config)
	case "PSYNC":
		handlePsync(conn, parts, config)
	case "PING":
		handlePing(conn, config)
	case "ECHO":
		handleEcho(conn, parts)
	case "SET":
		handleSet(conn, parts,config)
	case "GET":
		handleGet(conn, parts)
	case "RPUSH":
		handleRPush(conn, parts,config)
	case "LPUSH":
		handleLPush(conn, parts,config)
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
		handleXAdd(conn, parts,config)
	case "XRANGE":
		handleXRange(conn, parts)
	case "XREAD":
		handleXRead(conn, parts)
	case "INCR":
		handleIncr(conn, parts,config)
	case "INFO":
		handleInfo(conn, parts, config)
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func handleConnection(conn net.Conn, state *clientState, config *Config) {
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
		if config.Role == "slave" {
			fmt.Println("Slave received command:", cmd)
		}
		if state.inMulti {
			if cmd == "EXEC" {
				handleExec(conn, parts, state, config)
				continue
			}
			if cmd == "MULTI" {
				conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
				continue
			}
			if cmd == "DISCARD" {
				state.inMulti = false
				state.queue = nil
				conn.Write([]byte("+OK\r\n"))
				continue
			}
			state.queue = append(state.queue, parts)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}
		switch cmd {
		case "MULTI":
			state.inMulti = true
			state.queue = nil
			conn.Write([]byte("+OK\r\n"))
		case "EXEC":
			handleExec(conn, parts, state, config)
		case "DISCARD":
			conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
		default:
			handleCommand(conn, parts, config)
		}

		if config.Role == "master" && WRITE_COMMANDS[cmd] {
			propagateToReplicas(parts, config)
		}

	}
}
