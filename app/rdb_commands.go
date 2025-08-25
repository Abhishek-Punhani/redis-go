package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
)

func handleConfig(conn net.Conn, parts []string, config *Config) {
	if len(parts) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'CONFIG'\r\n"))
		return
	}
	subcommand := strings.ToUpper(parts[1])
	switch subcommand {
	case "GET":
		if len(parts) != 3 {
			conn.Write([]byte("-ERR wrong number of arguments for 'CONFIG GET'\r\n"))
			return
		}
		pattern := parts[2]
		var response []string
		if pattern == "*" || pattern == "dir" {
			response = append(response, "dir", config.rdb_dir)
		}
		if pattern == "*" || pattern == "dbfilename" {
			response = append(response, "dbfilename", config.rdb_filename)
		}
		conn.Write(encodeArray(response))
	default:
		conn.Write([]byte("-ERR unknown subcommand for 'CONFIG'\r\n"))
	}
}
func handleKeys(conn net.Conn, parts []string,config *Config) {
	RDBfile, err := os.ReadFile(path.Join(config.rdb_dir, config.rdb_filename))
	if err != nil {
		respArrayEmpty := "*0\r\n"
		conn.Write([]byte(respArrayEmpty))
	}

	keysCommand := strings.ToLower(parts[1])
	if keysCommand == "*" {

		FBidx := bytes.Index(RDBfile, []byte{0xFB})
		keyStart := int(FBidx + 5)
		keyLength := int(RDBfile[FBidx+4])
		keyName := (string(RDBfile[keyStart : keyStart+keyLength]))
		respArrayKeyName := fmt.Sprintf("*1\r\n$%d\r\n%s\r\n", len(keyName), keyName)
		conn.Write([]byte(respArrayKeyName))
	}
}
func encodeArray(arr []string) []byte {
	result := "*" + strconv.Itoa(len(arr)) + "\r\n"
	for _, s := range arr {
		result += "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
	}
	return []byte(result)
}
