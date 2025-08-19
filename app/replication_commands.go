package main

import (
	"fmt"
	"net"
)

func handleInfo(conn net.Conn, parts []string, role string) {
    info := fmt.Sprintf(
        "# Replication\r\nrole:%s\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
        role,
    )
    conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))

}
