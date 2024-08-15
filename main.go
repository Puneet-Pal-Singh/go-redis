package main

import (
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/Puneet-Pal-Singh/go-redis/redisprotocol"
)

type KeyValueStore struct {
	Strings               map[string]string
    Lists                 map[string][]string
    Hashes                map[string]map[string]string
    Sets                  map[string]map[string]struct{}
    SortedSets            map[string]map[string]float64
    Expirations           map[string]time.Time
	sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		Strings:               make(map[string]string),
        Lists:                 make(map[string][]string),
        Hashes:                make(map[string]map[string]string),
        Sets:                  make(map[string]map[string]struct{}),
        SortedSets:            make(map[string]map[string]float64),
        Expirations:           make(map[string]time.Time),
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
        // Sets
        "SADD":   s.handleSAdd,
        "SREM":   s.handleSRem,
        "SMEMBERS": s.handleSMembers,
        "SISMEMBER": s.handleSIsMember,
        // Sorted Sets
        "ZADD":   s.handleZAdd,
        "ZRANGE": s.handleZRange,
        "ZREM":   s.handleZRem,
        // Server and connection commands
        "EXPIRE": s.handleExpire,
        "TTL": s.handleTTL,
        "INFO": s.handleInfo,
        "FLUSHALL": s.handleFlushAll,
        "PING": s.handlePing,
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

func (s *Server) handleSAdd(args []string) string {
    if len(args) < 2 {
        return "ERROR 'SADD' command requires at least 2 arguments"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.Sets[key]; !exists {
        s.kvstore.Sets[key] = make(map[string]struct{})
    }

    addedCount := 0
    for _, member := range args[1:] {
        if _, exists := s.kvstore.Sets[key][member]; !exists {
            s.kvstore.Sets[key][member] = struct{}{}
            addedCount++
        }
    }
    return fmt.Sprintf("(integer) %d", addedCount)
}

func (s *Server) handleSRem(args []string) string {
    if len(args) < 2 {
        return "ERROR 'SREM' command requires at least 2 arguments"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.Sets[key]; !exists {
        return "(integer) 0"
    }

    removedCount := 0
    for _, member := range args[1:] {
        if _, exists := s.kvstore.Sets[key][member]; exists {
            delete(s.kvstore.Sets[key], member)
            removedCount++
        }
    }
    return fmt.Sprintf("(integer) %d", removedCount)
}

func (s *Server) handleSMembers(args []string) string {
    if len(args) != 1 {
        return "ERROR 'SMEMBERS' command requires 1 argument"
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if memberSet, exists := s.kvstore.Sets[key]; exists {
        result := make([]string, 0, len(memberSet))
        for member := range memberSet {
            result = append(result, member)
        }
        sort.Strings(result)  // Sort the slice
        return strings.Join(result, "\n")
    }
    return "(empty)"
}

func (s *Server) handleSIsMember(args []string) string {
    if len(args) != 2 {
        return "ERROR 'SISMEMBER' command requires 2 arguments"
    }
    key := args[0]
    member := args[1]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if set, exists := s.kvstore.Sets[key]; exists {
        if _, exists := set[member]; exists {
            return "(integer) 1"
        }
    }
    return "(integer) 0"
}

func (s *Server) handleZAdd(args []string) string {
    if len(args) < 3 || len(args)%2 != 1 {
        return "ERROR 'ZADD' command requires at least 3 arguments with score-member pairs"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.SortedSets[key]; !exists {
        s.kvstore.SortedSets[key] = make(map[string]float64)
    }

    addedCount := 0
    for i := 1; i < len(args)-1; i += 2 {
        score, err := strconv.ParseFloat(args[i], 64)
        if err != nil {
            return "ERROR score is not a valid number"
        }
        member := args[i+1]
        if _, exists := s.kvstore.SortedSets[key][member]; !exists {
            s.kvstore.SortedSets[key][member] = score
            addedCount++
        }
    }
    return fmt.Sprintf("(integer) %d", addedCount)
}

func (s *Server) handleZRange(args []string) string {
    if len(args) != 3 {
        return "ERROR 'ZRANGE' command requires 3 arguments"
    }
    key := args[0]
    start, err1 := strconv.Atoi(args[1])
    end, err2 := strconv.Atoi(args[2])
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if err1 != nil || err2 != nil {
        return "ERROR start or end is not a valid integer"
    }

    if sortedSet, exists := s.kvstore.SortedSets[key]; exists {
        // Create a slice to hold the members
        var members []string
        for member := range sortedSet {
            members = append(members, member)
        }

        // Sort the members based on their scores
        sort.Slice(members, func(i, j int) bool {
            return sortedSet[members[i]] < sortedSet[members[j]]
        })

        // Adjust start and end for negative indexing
        if start < 0 {
            start = len(members) + start
        }
        if end < 0 {
            end = len(members) + end
        }
        if start < 0 {
            start = 0
        }
        if end >= len(members) {
            end = len(members) - 1
        }
        if start > end {
            return "(empty)"
        }

        // Prepare the result
        result := make([]string, 0, end-start+1)
        for i := start; i <= end; i++ {
            result = append(result, fmt.Sprintf(`"%s"`, members[i]))
        }
        return strings.Join(result, "\n") // Return as separate lines
    }
    return "(empty)"
}

func (s *Server) handleZRem(args []string) string {
    if len(args) < 2 {
        return "ERROR 'ZREM' command requires at least 2 arguments"
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.SortedSets[key]; !exists {
        return "(integer) 0"
    }

    removedCount := 0
    for _, member := range args[1:] {
        if _, exists := s.kvstore.SortedSets[key][member]; exists {
            delete(s.kvstore.SortedSets[key], member)
            removedCount++
        }
    }
    return fmt.Sprintf("(integer) %d", removedCount)
}


func (s *Server) handleExpire(args []string) string {
    if len(args) != 2 {
        return "ERROR 'EXPIRE' command requires 2 arguments"
    }
    key := args[0]
    seconds, err := strconv.ParseInt(args[1], 10, 64)
    if err != nil {
        return "ERROR seconds must be a valid integer"
    }
    
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    
    if _, exists := s.kvstore.Strings[key]; exists {
        s.kvstore.Expirations[key] = time.Now().Add(time.Duration(seconds) * time.Second) // Set expiration time
        return "OK"
    }
    return "(nil)"
}

func (s *Server) handleTTL(args []string) string {
    if len(args) != 1 {
        return "ERROR 'TTL' command requires 1 argument"
    }
    key := args[0]

    s.kvstore.RLock()
    expiration, exists := s.kvstore.Expirations[key]
    s.kvstore.RUnlock()
    
    if exists {
        if time.Now().Before(expiration) {
            // Calculate remaining TTL
            ttl := int(time.Until(expiration).Seconds())
            return fmt.Sprintf("(integer) %d", ttl)
        }
        
        // Key has expired, clean up
        s.kvstore.Lock() // Acquire a write lock for cleanup
        defer s.kvstore.Unlock()
        
        delete(s.kvstore.Expirations, key)
        delete(s.kvstore.Strings, key)
        delete(s.kvstore.Lists, key)
        delete(s.kvstore.Hashes, key)
        delete(s.kvstore.Sets, key)
        delete(s.kvstore.SortedSets, key)
        
        return "(integer) -2" // Indicate the key existed but has expired
    }
    return "(integer) -1" // Key does not exist
}

func (s *Server) handleInfo(args []string) string {
    info := "Server Info:\n"
    info += fmt.Sprintf("Keys in store: %d\n", len(s.kvstore.Strings))
    info += fmt.Sprintf("Lists: %d\n", len(s.kvstore.Lists))
    info += fmt.Sprintf("Hashes: %d\n", len(s.kvstore.Hashes))
    info += fmt.Sprintf("Sets: %d\n", len(s.kvstore.Sets))
    info += fmt.Sprintf("Sorted Sets: %d\n", len(s.kvstore.SortedSets))
    return info
}

func (s *Server) handleFlushAll(args []string) string {
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    s.kvstore.Strings = make(map[string]string)
    s.kvstore.Lists = make(map[string][]string)
    s.kvstore.Hashes = make(map[string]map[string]string)
    s.kvstore.Sets = make(map[string]map[string]struct{})
    s.kvstore.SortedSets = make(map[string]map[string]float64)
    return "OK"
}

func (s *Server) handlePing(args []string) string {
    return "PONG"
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
