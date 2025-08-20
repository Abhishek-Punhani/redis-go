package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func connectToMaster(config *Config) (net.Conn, error) {
	if config.Role != "slave" {
		return nil, fmt.Errorf("not a slave configuration")
	}
	conn, err := net.Dial("tcp", config.MasterHost+":"+config.MasterPort)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		os.Exit(1)
	}
	reader := bufio.NewReader(conn)

	if err := sendPing(conn, reader); err != nil {
		conn.Close()
		return nil, err
	}
	if err := sendReplconfListeningPort(conn, reader, config.Port); err != nil {
		conn.Close()
		return nil, err
	}
	if err := sendReplconfCapa(conn, reader); err != nil {
		conn.Close()
		return nil, err
	}
	if err := sendPsync(conn, reader); err != nil {
		conn.Close()
		return nil, err
	}
	defer conn.Close()
	for {
		parts, err := ParseRESP(reader)
		if err != nil {
			fmt.Println("Error reading RESP from master:", err)
			conn.Close()
			return nil, err
		}
		handleCommand(conn, parts, config)
	}
}

// ParseRESP parses a RESP array from the reader and returns a slice of strings.
func ParseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected RESP array, got: %s", line)
	}
	numElements, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}
	parts := make([]string, 0, numElements)
	for i := 0; i < numElements; i++ {
		// Read bulk string header
		bulkHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimSpace(bulkHeader)
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got: %s", bulkHeader)
		}
		strLen, err := strconv.Atoi(bulkHeader[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %v", err)
		}
		// Read the actual string
		str := make([]byte, strLen+2) // +2 for \r\n
		if _, err := io.ReadFull(reader, str); err != nil {
			return nil, err
		}
		parts = append(parts, string(str[:strLen]))
	}
	return parts, nil
}

func sendPing(conn net.Conn, reader *bufio.Reader) error {
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Println("Error sending PING to master: ", err.Error())
		return err
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response from master: ", err.Error())
		return err
	}
	if strings.ToUpper(strings.TrimSpace(resp)) != "+PONG" {
		fmt.Println("Unexpected response from master: ", resp)
		return fmt.Errorf("unexpected response from master: %s", resp)
	}
	return nil
}

func sendReplconfListeningPort(conn net.Conn, reader *bufio.Reader, port string) error {
	fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response from master: ", err.Error())
		return err
	}
	if strings.ToUpper(strings.TrimSpace(resp)) != "+OK" {
		fmt.Println("Unexpected response from master: ", resp)
		return fmt.Errorf("unexpected response from master: %s", resp)
	}
	return nil
}

func sendReplconfCapa(conn net.Conn, reader *bufio.Reader) error {
	fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response from master: ", err.Error())
		return err
	}
	if strings.ToUpper(strings.TrimSpace(resp)) != "+OK" {
		fmt.Println("Unexpected response from master: ", resp)
		return fmt.Errorf("unexpected response from master: %s", resp)
	}
	return nil
}

func sendPsync(conn net.Conn, reader *bufio.Reader) error {
	fmt.Fprintf(conn, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading PSYNC reply from master: ", err)
		return err
	}
	if !strings.HasPrefix(strings.TrimSpace(line), "+FULLRESYNC") {
		return fmt.Errorf("unexpected response from master: %s", line)
	}
	// Next the master will send a bulk header for the RDB: $<len>\r\n
	header, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading RDB header from master:", err)
		return err
	}
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, "$") {
		return fmt.Errorf("expected bulk header for RDB, got: %s", header)
	}
	sizeStr := strings.TrimPrefix(header, "$")
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid RDB size: %v", err)
	}

	// Read the exact number of bytes for the RDB file
	rdb := make([]byte, size)
	if _, err := io.ReadFull(reader, rdb); err != nil {
		fmt.Println("Error reading RDB bytes from master:", err)
		return err
	}
	return nil
}

func handleInfo(conn net.Conn, parts []string, config *Config) {
	info := fmt.Sprintf(
		"# Replication\r\nrole:%s\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
		config.Role,
	)
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))

}

func hadleReplconf(conn net.Conn, parts []string, config *Config) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'replconf' command\r\n"))
		return
	}
	if config.Role == "slave" && strings.ToUpper(parts[1]) == "GETACK" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"))
		return
	}
	if config.Role != "master" {
		conn.Write([]byte("-ERR replconf is only supported in master mode\r\n"))
		return
	}
	if parts[1] == "listening-port" {
		if len(parts) < 3 {
			conn.Write([]byte("-ERR wrong number of arguments for 'replconf listening-port' command\r\n"))
			return
		}
		port := parts[2]
		if _, err := strconv.Atoi(port); err != nil {
			conn.Write([]byte("-ERR invalid port number\r\n"))
			return
		}
		remote := conn.RemoteAddr().String()
		fmt.Printf("New replica connected: %s (listening-port=%s)\n", remote, port)
		config.ReplicaMu.Lock()
		config.replicaConns[remote] = conn
		config.ReplicaMu.Unlock()
	}
	conn.Write([]byte("+OK\r\n"))
}

func handlePsync(conn net.Conn, parts []string, config *Config) {
	if len(parts) < 3 || parts[1] != "?" || parts[2] != "-1" {
		conn.Write([]byte("-ERR wrong number of arguments for 'psync' command\r\n"))
		return
	}

	if config.Role != "master" {
		conn.Write([]byte("-ERR psync is only supported in master mode\r\n"))
		return
	}
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
	empty_rdb_file := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	rdbBytes, err := hex.DecodeString(empty_rdb_file)
	if err != nil {
		fmt.Println("failed to decode rdb hex:", err)
		conn.Write([]byte("-ERR internal error\r\n"))
		return
	}
	fmt.Fprintf(conn, "$%d\r\n%s", len(rdbBytes), rdbBytes)
}

func buildRespArray(parts []string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, p := range parts {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
	}
	return sb.String()
}

func propagateToReplicas(parts []string, config *Config) {
	partsCopy := append([]string(nil), parts...)
	payload := buildRespArray(partsCopy)
	config.ReplicaMu.Lock()
	var activeConns []net.Conn
	for _, conn := range config.replicaConns {
		activeConns = append(activeConns, conn)
	}
	config.ReplicaMu.Unlock()

	success := 0
	for _, conn := range activeConns {
		_, err := conn.Write([]byte(payload))
		if err != nil {
			addr := conn.RemoteAddr().String()
			fmt.Printf("Error sending command to replica %s: %v\n", addr, err)
			config.ReplicaMu.Lock()
			delete(config.replicaConns, addr)
			config.ReplicaMu.Unlock()
			continue
		}
		success++
	}
	fmt.Printf("Propagated command to %d live replica connections\n", success)
}
