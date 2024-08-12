package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/Puneet-Pal-Singh/go-redis/redisprotocol"
)

type KeyValueStore struct {
	Strings               map[string]string
	sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		Strings:               make(map[string]string),
	}
}

type CommandFunc func([]string) string

type Server struct {
	kvstore    *KeyValueStore
	commands   map[string]CommandFunc
}

func NewServer() *Server {
	s := &Server{
		kvstore:  NewKeyValueStore(),
		commands: make(map[string]CommandFunc),
	}
	s.registerCommands()
	return s
}

func (s *Server) registerCommands() {
    s.commands = map[string]CommandFunc{
        "GET":    s.handleGet,
        "SET":    s.handleSet,
		"DEL":    s.handleDel,
        "EXISTS": s.handleExists,
        "INCR":   s.handleIncr,
        "DECR":   s.handleDecr,
        "INCRBY": s.handleIncrBy,
        "DECRBY": s.handleDecrBy,
		"MSET":   s.handleMSet,
		"MGET":   s.handleMGet,
        //TODO: More commands will be added here
    }
}

func (s *Server) handleGet(args []string) string {
    if len(args) != 1 {
		return "ERROR 'GET' command requires 1 argument"
	}
	key := args[0]
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()

	if value, ok := s.kvstore.Strings[key]; ok {
		return value
	}
	return "(nil)"
}

func (s *Server) handleSet(args []string) string {
    if len(args) != 2 {
        return "ERROR 'GET' command requires 2 arguments"
    }
    key, value := args[0], args[1]
    s.kvstore.Lock()
	defer s.kvstore.Unlock()

	s.kvstore.Strings[key] = value
	return "OK"
}

func (s *Server) handleDel(args []string) string {
    if len(args) < 1 {
        return "ERROR 'DEL' command requires at least 1 argument"
    }
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    deletedCount := 0
    for _, key := range args {
        if _, exists := s.kvstore.Strings[key]; exists {
            delete(s.kvstore.Strings, key)
            deletedCount++
        }
    }
    return fmt.Sprintf("(integer) %d", deletedCount)
}

func (s *Server) handleExists(args []string) string {
    if len(args) != 1 {
        return "ERROR 'EXISTS' command requires 1 argument"
    }
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()
    if _, exists := s.kvstore.Strings[args[0]]; exists {
        return ":1"
    }
    return ":0"
}

func (s *Server) handleIncr(args []string) string {
    return s.handleIncrDecr(args, 1)
}

func (s *Server) handleDecr(args []string) string {
    return s.handleIncrDecr(args, -1)
}

func (s *Server) handleIncrDecr(args []string, delta int64) string {
    if len(args) != 1 {
        return fmt.Sprintf("ERROR '%s' command requires 1 argument", strings.ToUpper(args[0]))
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    value, exists := s.kvstore.Strings[key]
    if !exists {
        s.kvstore.Strings[key] = "0"
        value = "0"
    }
    intValue, err := strconv.ParseInt(value, 10, 64)
    if err != nil {
        return "ERROR value is not an integer or out of range"
    }
    intValue += delta
    s.kvstore.Strings[key] = strconv.FormatInt(intValue, 10)
    return fmt.Sprintf("(integer) %d", intValue)
}

func (s *Server) handleIncrBy(args []string) string {
    return s.handleIncrDecrBy(args)
}

func (s *Server) handleDecrBy(args []string) string {
    return s.handleIncrDecrBy(args)
}

func (s *Server) handleIncrDecrBy(args []string) string {
    if len(args) != 2 {
        return fmt.Sprintf("ERROR '%s' command requires 2 arguments", strings.ToUpper(args[0]))
    }
    key := args[0]
    delta, err := strconv.ParseInt(args[1], 10, 64)
    if err != nil {
        return "ERROR increment/decrement value is not an integer"
    }
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    value, exists := s.kvstore.Strings[key]
    if !exists {
        s.kvstore.Strings[key] = "0"
        value = "0"
    }
    intValue, err := strconv.ParseInt(value, 10, 64)
    if err != nil {
        return "ERROR value is not an integer or out of range"
    }
    intValue += delta
    s.kvstore.Strings[key] = strconv.FormatInt(intValue, 10)
    return fmt.Sprintf("(integer) %d", intValue)
}

func (s *Server) handleMSet(args []string) string {
    if len(args)%2 != 0 {
        return "ERROR 'MSET' command requires an even number of arguments"
    }
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    for i := 0; i < len(args); i += 2 {
        s.kvstore.Strings[args[i]] = args[i+1]
    }
    return "OK"
}

func (s *Server) handleMGet(args []string) string {
    if len(args) < 1 {
        return "ERROR 'MGET' command requires at least 1 argument"
    }
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()
    results := make([]string, len(args))
    for i, key := range args {
        if value, exists := s.kvstore.Strings[key]; exists {
            results[i] = value
        } else {
            results[i] = "(nil)"
        }
    }
    return strings.Join(results, "\n")
}

func readCommand(resp *redisprotocol.Resp) ([]string, error) {
    value, err := resp.Read()
    if err != nil {
        return nil, err
    }

    if value.Type != "array" {
        return nil, fmt.Errorf("invalid command format")
    }

    command := make([]string, len(value.Array))
    for i, v := range value.Array {
        if v.Type != "bulk" {
            return nil, fmt.Errorf("invalid command argument")
        }
        command[i] = v.Bulk
    }

    return command, nil
}

func (s *Server) processCommand(command []string) string {
    fmt.Println("Received command:", command) // yo
    if len(command) == 0 {
        return "ERR empty command"
    }

    cmd := strings.ToUpper(command[0])
    args := command[1:]

    if handler, ok := s.commands[cmd]; ok {
		return handler(args)
	}

	return "ERR unknown command '" + cmd + "'"
}

func handleConnection(conn net.Conn, server *Server) {
    defer conn.Close()
    resp := redisprotocol.NewResp(conn, conn)

    for {
        command, err := readCommand(resp)
        if err != nil {
            if err == io.EOF {
                fmt.Println("Client disconnected")
                return
            }
            fmt.Println("Error reading command:", err)
            return
        }

        response := server.processCommand(command)
        err = resp.Write(redisprotocol.Value{Type: "bulk", Bulk: response})
        if err != nil {
            fmt.Println("Error writing response:", err)
            return
        }
    }
}

func main() {
	port := "8000"
	server := NewServer()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on :%s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn, server)
	}
}
