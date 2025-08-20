package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
)

func connectToMaster(config Config) (net.Conn, error) {
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
	return nil, nil
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
	parts, err := readCommand(reader, conn)
	if err != nil {
		fmt.Println("Error reading response from master: ", err.Error())
		return err
	}
	if strings.ToUpper(strings.TrimSpace(parts[0])) != "+FULLRESYNC" {
		return fmt.Errorf("unexpected response from master: %s", parts[0])
	}
	return nil
}

func handleInfo(conn net.Conn, parts []string, config Config) {
	info := fmt.Sprintf(
		"# Replication\r\nrole:%s\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
		config.Role,
	)
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))

}

func hadleReplconf(conn net.Conn, parts []string, config Config) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'replconf' command\r\n"))
		return
	}
	if config.Role != "master" {
		conn.Write([]byte("-ERR replconf is only supported in master mode\r\n"))
		return
	}

	conn.Write([]byte("+OK\r\n"))

}

func handlePsync(conn net.Conn, parts []string, config Config) {
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
