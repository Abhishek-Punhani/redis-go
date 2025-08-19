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

func parseStreamID(id string) (ms int64, seq int64, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid id format")
	}
	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

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

func handleLPush(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPUSH'\r\n"))
		return
	}
	key := parts[1]
	values := parts[2:]
	fmt.Println("LPUSH values:", values)
	listLock := getListLock(key)
	listLock.Lock()
	defer listLock.Unlock()
	for i := 0; i < len(values); i++ {
		list_store[key] = append([]string{values[i]}, list_store[key]...)
	}
	fmt.Printf("LPUSH updated list for key '%s': %v\n", key, list_store[key])
	fmt.Fprintf(conn, ":%d\r\n", len(list_store[key]))
}

func handleLLen(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LLEN'\r\n"))
		return
	}
	key := parts[1]

	listLock := getListLock(key)
	listLock.Lock()
	defer listLock.Unlock()

	length := len(list_store[key])
	fmt.Fprintf(conn, ":%d\r\n", length)
}

func handleLPop(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPOP'\r\n"))
		return
	}
	key := parts[1]
	turns := 1
	if len(parts) > 2 {
		var err error
		turns, err = strconv.Atoi(parts[2])
		if err != nil || turns < 1 {
			conn.Write([]byte("-ERR invalid number of turns\r\n"))
			return
		}
	}
	listLock := getListLock(key)
	listLock.Lock()
	defer listLock.Unlock()

	values, ok := list_store[key]
	if !ok || len(values) == 0 || turns > len(values) {
		conn.Write([]byte("$-1\r\n")) // Null bulk string
		return
	}

	if turns == 1 {
		value := values[0]
		list_store[key] = values[1:]
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
		if len(list_store[key]) == 0 {
			delete(list_store, key)
		}
		return
	}

	conn.Write([]byte(fmt.Sprintf("*%d\r\n", turns)))
	for i := 0; i < turns && len(values) > 0; i++ {
		value := values[0]
		values = values[1:]
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
	}
	list_store[key] = values
	if len(values) == 0 {
		delete(list_store, key)
	}
}

func handleBLPop(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'BLPOP'\r\n"))
		return
	}
	key := parts[1]
	timeout := -1.0
	if len(parts) > 2 {
		var err error
		timeout, err = strconv.ParseFloat(parts[2], 64)

		if err != nil || timeout < 0 {
			conn.Write([]byte("-ERR invalid timeout value\r\n"))
			return
		}
	}

	start := time.Now()
	for {
		listLock := getListLock(key)
		listLock.Lock()
		values, ok := list_store[key]
		if ok && len(values) > 0 {
			if timeout > 0 {
				time.Sleep(time.Duration(timeout) * time.Millisecond)
			}
			value := values[0]
			list_store[key] = values[1:]
			listLock.Unlock()
			// RESP array: [key, value]
			fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			if len(list_store[key]) == 0 {
				// Clean up empty list
				listLock.Lock()
				delete(list_store, key)
				listLock.Unlock()
			}
			return
		}
		listLock.Unlock()

		if timeout > 0 && time.Since(start) >= time.Duration(timeout*float64(time.Second)) {
			conn.Write([]byte("$-1\r\n"))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func handleType(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'TYPE'\r\n"))
		return
	}
	key := parts[1]
	streamsMu.RLock()
	_, isStream := streams[key]
	streamsMu.RUnlock()
	if isStream {
		conn.Write([]byte("+stream\r\n"))
		return
	}
	mu.RLock()
	entry, exists := store[key]
	mu.RUnlock()

	if !exists {
		conn.Write([]byte(fmt.Sprintf("$4\r\n%s\r\n", "none")))
		return
	}

	if entry.Expiry.After(time.Time{}) && time.Now().After(entry.Expiry) {
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		conn.Write([]byte(fmt.Sprintf("$4\r\n%s\r\n", "none")))
		return
	}

	conn.Write([]byte(fmt.Sprintf("$6\r\n%s\r\n", "string")))
}

func handleXAdd(conn net.Conn, parts []string) {
	if len(parts) < 5 || (len(parts)-3)%2 != 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XADD'\r\n"))
		return
	}
	key := parts[1]
	id := parts[2]
	fields := make(map[string]string)
	for i := 3; i < len(parts); i += 2 {
		fields[parts[i]] = parts[i+1]
	}

	if strings.HasSuffix(id, "-*") || id == "*" {
		ms := time.Now().UnixMilli()
		if id != "*" {
			timePart := strings.TrimSuffix(id, "-*")
			ms_, err := strconv.ParseInt(timePart, 10, 64)
			if err != nil || ms_ < 0 {
				conn.Write([]byte("-ERR invalid stream ID format\r\n"))
				return
			}
			ms = ms_
		}
		seq := int64(0)
		streamsMu.Lock()
		entries := streams[key]
		for _, entry := range entries {
			entryMs, entrySeq, err := parseStreamID(entry.ID)
			if err == nil && entryMs == ms && entrySeq >= seq {
				seq = entrySeq + 1
			}
		}
		if ms == 0 && seq == 0 && len(entries) == 0 {
			seq = 1
		}
		id = fmt.Sprintf("%d-%d", ms, seq)
		streams[key] = append(streams[key], StreamEntry{ID: id, Fields: fields})
		streamsMu.Unlock()
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
		return
	}

	// Existing explicit ID logic
	newMs, newSeq, err := parseStreamID(id)
	if err != nil || (newMs < 0 || newSeq < 0) {
		conn.Write([]byte("-ERR invalid stream ID format\r\n"))
		return
	}
	if newMs == 0 && newSeq == 0 {
		conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
		return
	}

	streamsMu.Lock()
	defer streamsMu.Unlock()
	entries := streams[key]
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		lastMs, lastSeq, err := parseStreamID(last.ID)
		if err != nil {
			conn.Write([]byte("-ERR internal stream ID error\r\n"))
			return
		}
		if newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq) {
			conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
			return
		}
	}
	streams[key] = append(streams[key], StreamEntry{ID: id, Fields: fields})
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
}

func handleXRange(conn net.Conn, parts []string) {
	if len(parts) != 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XRANGE'\r\n"))
		return
	}
	key := parts[1]
	startID := parts[2]
	if startID == "-" {
		startID = "0-1"
	}
	endID := parts[3]
	maxId := strconv.FormatInt(1<<63-1, 10)
	if endID == "+" {
		endID = maxId + "-" + maxId
	}

	streamsMu.RLock()
	entries, ok := streams[key]
	streamsMu.RUnlock()
	if !ok || len(entries) == 0 {
		conn.Write([]byte("*0\r\n"))
		return
	}

	// Parse start and end IDs, defaulting sequence part if missing
	parseID := func(id string, isStart bool) (int64, int64, error) {
		parts := strings.Split(id, "-")
		if len(parts) == 1 {
			seq := int64(0)
			if !isStart {
				seq = 1<<63 - 1 // max int64
			}
			ms, err := strconv.ParseInt(parts[0], 10, 64)
			return ms, seq, err
		}
		ms, err1 := strconv.ParseInt(parts[0], 10, 64)
		seq, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil {
			return 0, 0, err1
		}
		if err2 != nil {
			return 0, 0, err2
		}
		return ms, seq, nil
	}

	startMs, startSeq, err := parseID(startID, true)
	if err != nil {
		conn.Write([]byte("-ERR invalid start ID\r\n"))
		return
	}
	endMs, endSeq, err := parseID(endID, false)
	if err != nil {
		conn.Write([]byte("-ERR invalid end ID\r\n"))
		return
	}

	// Collect matching entries
	var resp strings.Builder
	var selected []StreamEntry
	for _, entry := range entries {
		entryMs, entrySeq, err := parseStreamID(entry.ID)
		if err != nil {
			continue
		}
		// Check if entry.ID in [startID, endID]
		if (entryMs > startMs || (entryMs == startMs && entrySeq >= startSeq)) &&
			(entryMs < endMs || (entryMs == endMs && entrySeq <= endSeq)) {
			selected = append(selected, entry)
		}
	}
	resp.WriteString(fmt.Sprintf("*%d\r\n", len(selected)))
	for _, entry := range selected {
		resp.WriteString("*2\r\n")
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))
		// Write fields as RESP array
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
		for k, v := range entry.Fields {
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v))
		}
	}
	conn.Write([]byte(resp.String()))
}

func handleXRead(conn net.Conn, part []string) {
	if len(part) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD'\r\n"))
		return
	}
	if len(part) > 4 && len(part)%2 != 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD'\r\n"))
		return
	}

	blockMs := 0
	streamIdx := 2
	if len(part) > 4 && strings.ToUpper(part[1]) == "BLOCK" {
		b, err := strconv.Atoi(part[2])
		if err != nil || b < 0 {
			conn.Write([]byte("-ERR invalid block value\r\n"))
			return
		}
		blockMs = b
		streamIdx = 4
	}

	streamCount := (len(part) - streamIdx) / 2
	keys := part[streamIdx : streamIdx+streamCount]
	ids := part[streamIdx+streamCount:]

	start := time.Now()
	for {
		resp := strings.Builder{}
		foundAny := false
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(keys)))
		for i, key := range keys {
			streamsMu.RLock()
			entries, ok := streams[key]
			streamsMu.RUnlock()
			if !ok || len(entries) == 0 {
				resp.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*0\r\n", len(key), key))
				continue
			}
			var matching []StreamEntry
			if ids[i] == "$" {
				lastIdx := len(entries) - 1
				for {
					streamsMu.RLock()
					entries, ok = streams[key]
					streamsMu.RUnlock()

					if ok && len(entries)-1 > lastIdx {
						matching = append(matching, entries[lastIdx+1:]...)
						foundAny = true
						break
					}
					if blockMs > 0 && time.Since(start) >= time.Duration(blockMs)*time.Millisecond {
						conn.Write([]byte("$-1\r\n"))
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			} else {
				ms, sq, err := parseStreamID(ids[i])
				if err != nil {
					conn.Write([]byte("-ERR invalid stream ID format\r\n"))
					return
				}
				found := false
				for _, entry := range entries {
					entryMs, entrySeq, err := parseStreamID(entry.ID)
					if err != nil {
						continue
					}
					if !found && (entryMs > ms || (entryMs == ms && entrySeq > sq)) {
						found = true
					}
					if found {
						matching = append(matching, entry)
					}
				}
			}
			resp.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(key), key))
			if len(matching) == 0 {
				resp.WriteString("*0\r\n")
			} else {
				foundAny = true
				resp.WriteString(fmt.Sprintf("*%d\r\n", len(matching)))
				for _, entry := range matching {
					resp.WriteString("*2\r\n")
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))
					resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
					for k, v := range entry.Fields {
						resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v))
					}
				}
			}
		}
		if foundAny {
			conn.Write([]byte(resp.String()))
			return
		}
		if blockMs > 0 && time.Since(start) >= time.Duration(blockMs)*time.Millisecond {
			conn.Write([]byte("$-1\r\n"))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func handleIncr(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'INCR'\r\n"))
		return
	}
	key := parts[1]

	mu.Lock()
	defer mu.Unlock()

	entry, exists := store[key]
	if !exists || entry.Expiry.After(time.Time{}) && time.Now().After(entry.Expiry) {
		store[key] = Entry{Value: "1", Expiry: time.Time{}}
		conn.Write([]byte(":1\r\n"))
		return
	}

	value, err := strconv.Atoi(entry.Value)
	if err != nil {
		conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
		return
	}
	value++
	store[key] = Entry{Value: strconv.Itoa(value), Expiry: entry.Expiry}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", value)))
}

func handleExec(conn net.Conn, parts []string, state *clientState) {
	if !state.inMulti {
		conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
		return
	}
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(state.queue))))
	for _, cmd := range state.queue {
		handleCommand(conn, cmd)
	}
	state.inMulti = false
	state.queue = nil
}
