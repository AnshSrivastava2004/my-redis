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

var store = make(map[string]string)
var expirations = make(map[string]int64)
var mu sync.RWMutex

func isExpired(key string) bool {
	expireTime, ok := expirations[key]
	if !ok {
		return false
	}

	if time.Now().Unix() > expireTime {
		delete(store, key)
		delete(expirations, key)
		return true
	}

	return false
}

func main() {
	fmt.Println("Starting MyRedis server at port 6389")

	listener, err := net.Listen("tcp", ":6389")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Server started!")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection: ", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
	}()
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Connection closed")
			return
		}

		line = strings.TrimSpace(line)
		parts := strings.Fields(strings.TrimSpace(line))
		command := strings.ToUpper(parts[0])

		switch command {
		case "HELP":
			conn.Write([]byte(fmt.Sprintln("Available commands:")))
			conn.Write([]byte(fmt.Sprintln("GET: Get value from key - Usage: GET key")))
			conn.Write([]byte(fmt.Sprintln("SET: Set key and value - Usage: SET key value")))
			conn.Write([]byte(fmt.Sprintln("SETEX: Set key with ttl - Usage: SETEX key ttl value")))
			conn.Write([]byte(fmt.Sprintln("EXISTS: Check if key exists - Usage: EXISTS key")))
			conn.Write([]byte(fmt.Sprintln("DELETE: Delete key - Usage: DEL key")))
			conn.Write([]byte(fmt.Sprintln("KEYS: Find all keys matching pattern - Usage: KEYS pattern")))
			conn.Write([]byte(fmt.Sprintln("TTL: Find ttl of key - Usage: TTL key")))
			conn.Write([]byte(fmt.Sprintln("EXPIRE: Set ttl of key - Usage: EXPIRE key ttl")))
			conn.Write([]byte(fmt.Sprintln("PERSIST: Remove ttl of key - Usage: PERSIST key")))
			conn.Write([]byte(fmt.Sprintln("FLUSHALL: Delete all keys - Usage: FLUSHALL")))
			conn.Write([]byte(fmt.Sprintln("CLOSE: Close connection - Usage: CLOSE")))

		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: SET command -> SET key value\n"))
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")

			mu.Lock()
			store[key] = value
			mu.Unlock()

			conn.Write([]byte("OK\r\n"))

		case "SETEX":
			if len(parts) < 4 {
				conn.Write([]byte("ERROR: SETEX command -> SETEX key ttl value\n"))
				continue
			}

			key := parts[1]
			seconds, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("Invalid expiration time\n"))
				continue
			}
			value := strings.Join(parts[3:], " ")

			mu.Lock()
			store[key] = value
			expirations[key] = int64(seconds) + time.Now().Unix()
			mu.Unlock()

			conn.Write([]byte("OK\r\n"))

		case "GET":
			key := parts[1]
			if isExpired(key) {
				conn.Write([]byte("-1\r\n"))
				continue
			}

			mu.RLock()
			value, ok := store[key]
			mu.RUnlock()

			if ok {
				conn.Write([]byte(value + "\r\n"))
			} else {
				conn.Write([]byte("-1\r\n"))
			}

		case "EXISTS":
			key := parts[1]
			_, exists := store[key]

			if exists {
				conn.Write([]byte("1\r\n"))
			} else {
				conn.Write([]byte("0\r\n"))
			}

		case "KEYS":
			pattern := parts[1]

			index := 1
			if pattern == "*" {
				for key := range store {
					conn.Write([]byte(fmt.Sprintf("%d) %s\n", index, key)))
					index += 1
				}
			}

		case "TTL":
			key := parts[1]
			_, exists := store[key]
			if isExpired(key) || !exists {
				conn.Write([]byte("-2\r\n"))
				continue
			}
			expireTime, canExpire := expirations[key]
			ttl := expireTime - time.Now().Unix()
			if !canExpire {
				conn.Write([]byte("-1\r\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%d seconds\n", ttl)))

		case "EXPIRE":
			key := parts[1]
			seconds, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("Invalid expiration time\n"))
				continue
			}

			mu.Lock()
			expirations[key] = int64(seconds) + time.Now().Unix()
			mu.Unlock()

			conn.Write([]byte("OK\r\n"))

		case "PERSIST":
			key := parts[1]

			_, exists := expirations[key]

			if exists {
				delete(expirations, key)
			}

			conn.Write([]byte("OK\r\n"))

		case "DEL":
			key := parts[1]

			_, exists1 := store[key]
			_, exists2 := expirations[key]

			if exists1 {
				delete(store, key)
			}
			if exists2 {
				delete(expirations, key)
			}

			if exists1 || exists2 {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}

		case "FLUSHALL":
			for key := range store {
				delete(store, key)
			}
			for key := range expirations {
				delete(expirations, key)
			}

			conn.Write([]byte("OK\r\n"))

		case "CLOSE":
			conn.Write([]byte("Closing connection\r\n"))

			conn.Close()

		default:
			conn.Write([]byte("ERROR: Unknown command. Use 'HELP' for a list of available commands.\n"))
		}
	}
}
