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

func getListLock(key string) *sync.Mutex {
	listLocksMu.Lock()
	defer listLocksMu.Unlock()
	if l, ok := listLocks[key]; ok {
		return l
	}
	l := &sync.Mutex{}
	listLocks[key] = l
	return l
}

func readCommand(reader *bufio.Reader, conn net.Conn) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "*") {
		conn.Write([]byte("-ERR Protocol error\r\n"))
		return nil, fmt.Errorf("protocol error")
	}

	numElems, err := strconv.Atoi(line[1:])
	if err != nil {
		conn.Write([]byte("-ERR invalid multibulk length\r\n"))
		return nil, err
	}

	var parts []string
	for i := 0; i < numElems; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		arg, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		parts = append(parts, strings.TrimSpace(arg))
	}
	return parts, nil
}

func handlePing(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func handleEcho(conn net.Conn, parts []string) {
	if len(parts) > 1 {
		msg := parts[1]
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)))
	} else {
		conn.Write([]byte("$0\r\n\r\n"))
	}
}

func handleSet(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'SET'\r\n"))
		return
	}
	key := parts[1]
	value := parts[2]
	expiry := time.Time{} // No expiry by default
	if len(parts) > 4 && strings.ToUpper(parts[3]) == "PX" {
		expiryMs, err := strconv.Atoi(parts[4])
		if err != nil {
			conn.Write([]byte("-ERR invalid expiry value\r\n"))
			return
		}
		expiry = time.Now().Add(time.Duration(expiryMs) * time.Millisecond)
	}

	mu.Lock()
	store[key] = Entry{Value: value, Expiry: expiry}
	mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
		return
	}
	key := parts[1]
	mu.RLock()
	entry, exists := store[key]
	mu.RUnlock()

	if !exists || (entry.Expiry.After(time.Time{}) && time.Now().After(entry.Expiry)) {
		conn.Write([]byte("$-1\r\n")) // Null bulk string
		if exists && entry.Expiry.After(time.Time{}) && time.Now().After(entry.Expiry) {
			mu.Lock()
			delete(store, key)
			mu.Unlock()
		}
		return
	}
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.Value), entry.Value)))
}

func handleRPush(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'RPUSH'\r\n"))
		return
	}
	key := parts[1]
	values := parts[2:]

	listLock := getListLock(key)
	listLock.Lock()
	defer listLock.Unlock()

	list_store[key] = append(list_store[key], values...)
	fmt.Fprintf(conn, ":%d\r\n", len(list_store[key]))
}

func handleLRange(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LRANGE'\r\n"))
		return
	}
	key := parts[1]
	start, err := strconv.Atoi(parts[2])
	if err != nil {
		conn.Write([]byte("-ERR invalid start index\r\n"))
		return
	}
	end, err := strconv.Atoi(parts[3])
	if err != nil {
		conn.Write([]byte("-ERR invalid end index\r\n"))
		return
	}

	listLock := getListLock(key)
	listLock.Lock()
	defer listLock.Unlock()

	values, ok := list_store[key]
	if !ok {
		conn.Write([]byte("*0\r\n"))
		return
	}
	if start < 0 && end < 0 {
		start += len(values)
		end += len(values)
	}
	if end < 0 {
		end += len(values)
	}
	if start < 0 {
		start = 0
	}
	if end >= len(values) {
		end = len(values) - 1
	}
	if start >= len(values) {
		conn.Write([]byte("*0\r\n"))
		return
	}
	if start < 0 || end >= len(values) || start > end {
		conn.Write([]byte("*0\r\n"))
		return
	}

	result := values[start : end+1]
	// Write RESP array header
	fmt.Fprintf(conn, "*%d\r\n", len(result))
	for _, v := range result {
		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
	}
}
