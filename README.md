# Redis-Go

_A minimal, Redis-compatible server implemented in Go_

---

## 🚀 Overview

**Redis-Go** is an educational, in-memory Redis-compatible server written in Go. It demonstrates practical implementation of the RESP protocol, common Redis commands, and core data structures—showcasing networking, concurrency, and server design.

---

## ✨ Features

- **RESP** protocol parsing & encoding
- **Strings:** `SET` (with optional expiry), `GET`, `INCR`
- **Lists:** `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP` (with timeout)
- **Streams:** `XADD`, `XRANGE`, `XREAD` (auto-generated IDs, blocking reads)
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Introspection:** `TYPE` command
- **Blocking/Non-blocking** operations
- **Configurable port** via `--port` flag
- **Replication:** Leader/follower, handshake, RDB snapshot transfer
- **Persistence:** RDB file read/write
- **Pub/Sub:** `SUBSCRIBE`, `PUBLISH`, delivery semantics
- **Sorted Sets:** Full ZSET support (`ZADD`, `ZRANK`, `ZRANGE`, etc.)

---

## 🛠️ Implementation Notes

- All state is **in-memory**; data structures prioritize clarity and correctness.
- **Mutexes** ensure data consistency across concurrent client goroutines (per-key locking where appropriate).
- **Expiry** is enforced on access; expirations are stored with keys and checked lazily.
- **Concurrency:** Goroutine per connection, safe synchronization for shared state.
- **RESP:** Expects well-formed requests, produces standard responses.
- **Replication:** Leader/follower, RDB snapshot transfer, replica initialization.
- **Pub/Sub:** Message delivery, subscribed mode, command semantics.

---

## 📝 Supported Commands

<details>
<summary><strong>Connection / Info</strong></summary>

- `PING`, `ECHO`, `INFO`
- `INFO` supports leader/replica-specific fields
</details>

<details>
<summary><strong>Replication</strong></summary>

- Configurable port, handshake, RDB transfer (empty/full)
- Command propagation, ACK semantics, `WAIT` command
</details>

<details>
<summary><strong>Strings</strong></summary>

- `SET key value [PX milliseconds]`
- `GET key`
- `INCR key`
</details>

<details>
<summary><strong>Lists</strong></summary>

- `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`
</details>

<details>
<summary><strong>Streams</strong></summary>

- `XADD`, `XRANGE`, `XREAD` (`STREAMS`, `BLOCK`, `$`, `+`, `-`)
</details>

<details>
<summary><strong>Transactions</strong></summary>

- `MULTI`, `EXEC`, `DISCARD`
</details>

<details>
<summary><strong>Pub/Sub</strong></summary>

- `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- Subscribed mode, `PING` in subscribed mode, message delivery
</details>

<details>
<summary><strong>Sorted Sets</strong></summary>

- `ZADD key score member [score member ...]`
- `ZRANK`, `ZREVRANK`
- `ZRANGE` (negative indexes, `COUNT`)
- `ZCOUNT key min max`
- `ZSCORE key member`
- `ZREM key member [member ...]`
</details>

<details>
<summary><strong>Persistence / RDB</strong></summary>

- RDB file config, snapshot read/write
- Key reads, string values, multi-key, expiry-aware reads
</details>

---

## 🏗️ Build & Run

**Requirements:**  
- Go 1.18+

**Build:**
```sh
go build -o redis-go app/*.go
```

**Run (default port 6379):**
```sh
./redis-go
```

**Specify port:**
```sh
./redis-go --port 6380
```

---

## 💡 Usage Examples

**With redis-cli:**
```sh
redis-cli -p 6379 PING
redis-cli -p 6379 SET mykey "hello"
redis-cli -p 6379 GET mykey
redis-cli -p 6379 INFO
redis-cli -p 6379 PUBLISH mychannel "msg"
```

**With netcat (RESP raw):**
```sh
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 6379
```

---

## ⚠️ Notes

- Focuses on clarity and correctness for educational purposes; **not production-optimized**.
- Mutexes ensure consistency across concurrent goroutines.
- Replication, persistence (RDB), pub/sub, and sorted sets are fully implemented.

---

## 🤝 Contributing

- Submit small, focused PRs with tests where applicable.
- Keep RESP behavior consistent with the documented examples.

---

## 📄 License

[MIT](LICENSE)

