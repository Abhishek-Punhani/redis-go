package main

import (
	"net"
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
func encodeArray(arr []string) []byte {
	result := "*" + strconv.Itoa(len(arr)) + "\r\n"
	for _, s := range arr {
		result += "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
	}
	return []byte(result)
}
