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
    Lists                 map[string][]string
    Hashes                map[string]map[string]string
	sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		Strings:               make(map[string]string),
        Lists:                 make(map[string][]string),
        Hashes:                make(map[string]map[string]string),
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
        // Lists
        "LPUSH":  s.handleLPush,
        "LPOP":   s.handleLPop,
        "LLEN":   s.handleLLen,
        "RPUSH":  s.handleRPush,
        "RPOP":   s.handleRPop,
        // Hashes
        "HSET":   s.handleHSet,
        "HGET":   s.handleHGet,
        "HDEL":   s.handleHDel,
        "HLEN":   s.handleHLen,
        "HMGET":  s.handleHMGet,
        "HGETALL": s.handleHGetAll,
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

func (s *Server) handleLPush(args []string) string {
    if len(args) < 2 {
        return "ERROR 'LPUSH' command requires at least 2 arguments"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    
    // Initialize the list if it doesn't exist
    if _, exists := s.kvstore.Lists[key]; !exists {
        s.kvstore.Lists[key] = make([]string, 0)
    }
    // Prepend the new values to the list
    for _, value := range args[1:] {
        s.kvstore.Lists[key] = append([]string{value}, s.kvstore.Lists[key]...)
    }
    return fmt.Sprintf("(integer) %d", len(s.kvstore.Lists[key]))
}

func (s *Server) handleLPop(args []string) string {
    if len(args) != 1 {
        return "ERROR 'LPOP' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if list, exists := s.kvstore.Lists[key]; exists && len(list) > 0 {
        poppedValue := list[0]
        // Remove the first element
        s.kvstore.Lists[key] = s.kvstore.Lists[key][1:]
        return poppedValue
    }
    return "(nil)"
}

func (s *Server) handleLLen(args []string) string {
    if len(args) != 1 {
        return "ERROR 'LLEN' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if list, exists := s.kvstore.Lists[key]; exists {
        return fmt.Sprintf("(integer) %d", len(list))
    }
    return "(integer) 0"
}

func (s *Server) handleRPush(args []string) string {
    if len(args) < 2 {
        return "ERROR 'RPUSH' command requires at least 2 arguments"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    // Initialize the list if it doesn't exist
    if _, exists := s.kvstore.Lists[key]; !exists {
        s.kvstore.Lists[key] = make([]string, 0)
    }
    values := args[1:]
    // Append the new values to the list
    s.kvstore.Lists[key] = append(s.kvstore.Lists[key], values...)
    
    return fmt.Sprintf("(integer) %d", len(s.kvstore.Lists[key]))
}

func (s *Server) handleRPop(args []string) string {
    if len(args) != 1 {
        return "ERROR 'RPOP' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if value, exists := s.kvstore.Lists[key]; exists && len(value) > 0 {
        poppedValue := value[len(value)-1] // Get the last element
        // Remove the last element
        s.kvstore.Lists[key] = value[:len(value)-1]
        return poppedValue
    }
    return "(nil)"
}

func (s *Server) handleHSet(args []string) string {
    if len(args) < 3 || len(args)%2 != 1 {
        return "ERROR 'HSET' command requires at least 3 arguments with key-value pairs"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    // Initialize the hash if it doesn't exist
    if _, exists := s.kvstore.Hashes[key]; !exists {
        s.kvstore.Hashes[key] = make(map[string]string)
    }

    for i := 1; i < len(args)-1; i += 2 {
        s.kvstore.Hashes[key][args[i]] = args[i+1]
    }

    return fmt.Sprintf("(integer) %d", len(s.kvstore.Hashes[key]))
}

func (s *Server) handleHGet(args []string) string {
    if len(args) != 2 {
        return "ERROR 'HGET' command requires 2 arguments"
    }
    key := args[0]
    field := args[1]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if value, exists := s.kvstore.Hashes[key][field]; exists {
        return value
    }
    return "(nil)"
}

func (s *Server) handleHDel(args []string) string {
    if len(args) < 2 {
        return "ERROR 'HDEL' command requires at least 2 arguments"
    }
    key := args[0]
    fields := args[1:]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.Hashes[key]; !exists {
        return "(integer) 0"
    }

    count := 0
    for _, field := range fields {
        if _, exists := s.kvstore.Hashes[key][field]; exists {
            delete(s.kvstore.Hashes[key], field)
            count++
        }
    }

    return fmt.Sprintf("(integer) %d", count)
}

func (s *Server) handleHLen(args []string) string {
    if len(args) != 1 {
        return "ERROR 'HLEN' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if fields, exists := s.kvstore.Hashes[key]; exists {
        return fmt.Sprintf("(integer) %d", len(fields))
    }
    return "(integer) 0"
}

// func (s *Server) handleHMGet(args []string) string {
//     if len(args) < 2 {
//         return "ERROR 'HMGET' command requires at least 2 arguments"
//     }
//     key := args[0]
//     fields := args[1:]
//     s.kvstore.RLock()
//     defer s.kvstore.RUnlock()

//     if values, exists := s.kvstore.Hashes[key]; exists {
//         var result []string
//         for _, field := range fields {
//             if value, fieldExists := values[field]; fieldExists {
//                 result = append(result, value)
//             } else {
//                 result = append(result, "(nil)")
//             }
//         }
//         return strings.Join(result, "\n")
//     }
//     return "(nil)"
// }

func (s *Server) handleHMGet(args []string) string {
    if len(args) < 2 {
        return "ERROR 'HMGET' command requires at least 2 arguments"
    }
    key := args[0]
    fields := args[1:]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if values, exists := s.kvstore.Hashes[key]; exists {
        var result []string
        for i, field := range fields {
            var value string
            if val, fieldExists := values[field]; fieldExists {
                value = fmt.Sprintf(`"%s"`, val)
            } else {
                value = "(nil)"
            }
            result = append(result, fmt.Sprintf("%d) %s", i+1, value))
        }
        return strings.Join(result, "\n")
    }
    return "(nil)"
}

func (s *Server) handleHGetAll(args []string) string {
    if len(args) != 1 {
        return "ERROR 'HGETALL' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if fields, exists := s.kvstore.Hashes[key]; exists {
        var result []string
        index := 1
        for field, value := range fields {
            result = append(result, fmt.Sprintf("%d) \"%s\"", index, field))
            index++
            result = append(result, fmt.Sprintf("%d) \"%s\"", index, value))
            index++
        }
        return strings.Join(result, "\n")
    }
    return "(empty)"
}

// TODO: Add more commands
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
