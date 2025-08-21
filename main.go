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

var pubsub = NewPubSub()
var persistence = NewPersistence("data.rdb")
type CommandFunc func([]string) redisprotocol.Value

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
        // Persistence commands
        "SAVE": s.handleSave,
        "BGSAVE": s.handleBgsave,
        //TODO: More commands will be added here
    }
}

func (s *Server) keyExistsUnlocked(key string) bool {
	_, sExists := s.kvstore.Strings[key]
	_, lExists := s.kvstore.Lists[key]
	_, hExists := s.kvstore.Hashes[key]
	_, setExists := s.kvstore.Sets[key]
	_, zExists := s.kvstore.SortedSets[key]
	return sExists || lExists || hExists || setExists || zExists
}

func (s *Server) handleCommandWithConn(cmd string, args []string, conn net.Conn) redisprotocol.Value {
    switch cmd {
    case "SUBSCRIBE":
        return s.handleSubscribe(args, conn)
    case "PUBLISH":
        return s.handlePublish(args)
    case "UNSUBSCRIBE":
        return s.handleUnsubscribe(args, conn)
    default:
        return redisprotocol.Value{Type: "error", Str: "ERR unknown command '" + cmd + "'"}
    }
}

func (s *Server) handleGet(args []string) redisprotocol.Value {
    if len(args) != 1 {
		return redisprotocol.Value{Type: "error", Str: "ERR 'GET' command requires 1 argument"}
	}
	key := args[0]
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()

	if value, ok := s.kvstore.Strings[key]; ok {
		return redisprotocol.Value{Type: "bulk", Bulk: value}
	}
	return redisprotocol.Value{Type: "nil"}
}

func (s *Server) handleSet(args []string) redisprotocol.Value {
    if len(args) != 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'SET' command requires 2 arguments"}
    }
    key, value := args[0], args[1]
    s.kvstore.Lock()
	defer s.kvstore.Unlock()

	s.kvstore.Strings[key] = value
	return redisprotocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleDel(args []string) redisprotocol.Value {
    if len(args) < 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'DEL' command requires at least 1 argument"}
    }
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    deletedCount := 0
    for _, key := range args {
		// First, check if the key exists in any data structure to correctly
		// increment the count of deleted keys.
		if s.keyExistsUnlocked(key) {
			deletedCount++
		}

		// Then, delete the key from all possible data stores.
		// This is safe because `delete` is a no-op if the key doesn't exist.
		delete(s.kvstore.Strings, key)
		delete(s.kvstore.Lists, key)
		delete(s.kvstore.Hashes, key)
		delete(s.kvstore.Sets, key)
		delete(s.kvstore.SortedSets, key)
		delete(s.kvstore.Expirations, key)
    }
    return redisprotocol.Value{Type: "integer", Num: deletedCount}
}

func (s *Server) handleExists(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'EXISTS' command requires 1 argument"}
    }
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()
    if s.keyExistsUnlocked(args[0]) {
        return redisprotocol.Value{Type: "integer", Num: 1}
    }
    return redisprotocol.Value{Type: "integer", Num: 0}
}

// applyDelta is a helper function that handles the core logic for all
// increment/decrement operations. It locks the store, gets the value, applies
// the delta, and saves the new value.
func (s *Server) applyDelta(key string, delta int64) redisprotocol.Value {
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    value, exists := s.kvstore.Strings[key]
    if !exists {
        value = "0"
    }

    intValue, err := strconv.ParseInt(value, 10, 64)
    if err != nil {
        return redisprotocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
    }

	intValue += delta
    s.kvstore.Strings[key] = strconv.FormatInt(intValue, 10)
    return redisprotocol.Value{Type: "integer", Num: int(intValue)}
}

func (s *Server) handleIncr(args []string) redisprotocol.Value {
	if len(args) != 1 {
		return redisprotocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'incr' command"}
	}
	return s.applyDelta(args[0], 1)
}

func (s *Server) handleDecr(args []string) redisprotocol.Value {
	if len(args) != 1 {
		return redisprotocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'decr' command"}
	}
	return s.applyDelta(args[0], -1)
}

func (s *Server) handleIncrBy(args []string) redisprotocol.Value {
	if len(args) != 2 {
		return redisprotocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'incrby' command"}
	}
	increment, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return redisprotocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	return s.applyDelta(args[0], increment)
}

func (s *Server) handleDecrBy(args []string) redisprotocol.Value {
	if len(args) != 2 {
		return redisprotocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'decrby' command"}
	}
	decrement, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return redisprotocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	return s.applyDelta(args[0], -decrement)
}

func (s *Server) handleMSet(args []string) redisprotocol.Value {
    if len(args)%2 != 0 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'MSET' command requires an even number of arguments"}
    }
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    for i := 0; i < len(args); i += 2 {
        s.kvstore.Strings[args[i]] = args[i+1]
    }
    return redisprotocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleMGet(args []string) redisprotocol.Value {
    if len(args) < 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'MGET' command requires at least 1 argument"}
    }
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()
    results := make([]redisprotocol.Value, len(args))
    for i, key := range args {
        if value, exists := s.kvstore.Strings[key]; exists {
            results[i] = redisprotocol.Value{Type: "bulk", Bulk: value}
        } else {
            results[i] = redisprotocol.Value{Type: "nil"}
        }
    }
    return redisprotocol.Value{Type: "array", Array: results}
}

func (s *Server) handleLPush(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'LPUSH' command requires at least 2 arguments"}
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
    return redisprotocol.Value{Type: "integer", Num: len(s.kvstore.Lists[key])}
}

func (s *Server) handleLPop(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'LPOP' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if list, exists := s.kvstore.Lists[key]; exists && len(list) > 0 {
        poppedValue := list[0]
        // Remove the first element
        s.kvstore.Lists[key] = s.kvstore.Lists[key][1:]
        return redisprotocol.Value{Type: "bulk", Bulk: poppedValue}
    }
    return redisprotocol.Value{Type: "nil"}
}

func (s *Server) handleLLen(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'LLEN' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if list, exists := s.kvstore.Lists[key]; exists {
        return redisprotocol.Value{Type: "integer", Num: len(list)}
    }
    return redisprotocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleRPush(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'RPUSH' command requires at least 2 arguments"}
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
    
    return redisprotocol.Value{Type: "integer", Num: len(s.kvstore.Lists[key])}
}

func (s *Server) handleRPop(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'RPOP' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if value, exists := s.kvstore.Lists[key]; exists && len(value) > 0 {
        poppedValue := value[len(value)-1] // Get the last element
        // Remove the last element
        s.kvstore.Lists[key] = value[:len(value)-1]
        return redisprotocol.Value{Type: "bulk", Bulk: poppedValue}
    }
    return redisprotocol.Value{Type: "nil"}
}

func (s *Server) handleHSet(args []string) redisprotocol.Value {
    if len(args) < 3 || len(args)%2 != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HSET' command requires at least 3 arguments with key-value pairs"}
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    // Initialize the hash if it doesn't exist
    if _, exists := s.kvstore.Hashes[key]; !exists {
        s.kvstore.Hashes[key] = make(map[string]string)
    }

    addedCount := 0
    for i := 1; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		if _, exists := s.kvstore.Hashes[key][field]; !exists {
			addedCount++
		}
        s.kvstore.Hashes[key][field] = value
    }

    return redisprotocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleHGet(args []string) redisprotocol.Value {
    if len(args) != 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HGET' command requires 2 arguments"}
    }
    key := args[0]
    field := args[1]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if value, exists := s.kvstore.Hashes[key][field]; exists {
        return redisprotocol.Value{Type: "bulk", Bulk: value}
    }
    return redisprotocol.Value{Type: "nil"}
}

func (s *Server) handleHDel(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HDEL' command requires at least 2 arguments"}
    }
    key := args[0]
    fields := args[1:]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.Hashes[key]; !exists {
        return redisprotocol.Value{Type: "integer", Num: 0}
    }

    count := 0
    for _, field := range fields {
        if _, exists := s.kvstore.Hashes[key][field]; exists {
            delete(s.kvstore.Hashes[key], field)
            count++
        }
    }

    return redisprotocol.Value{Type: "integer", Num: count}
}

func (s *Server) handleHLen(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HLEN' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if fields, exists := s.kvstore.Hashes[key]; exists {
        return redisprotocol.Value{Type: "integer", Num: len(fields)}
    }
    return redisprotocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleHMGet(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HMGET' command requires at least 2 arguments"}
    }
    key := args[0]
    fields := args[1:]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    results := make([]redisprotocol.Value, len(fields))
    hash, exists := s.kvstore.Hashes[key]
    if !exists {
		for i := range fields {
			results[i] = redisprotocol.Value{Type: "nil"}
		}
		return redisprotocol.Value{Type: "array", Array: results}
    }

	for i, field := range fields {
		if value, ok := hash[field]; ok {
			results[i] = redisprotocol.Value{Type: "bulk", Bulk: value}
		} else {
			results[i] = redisprotocol.Value{Type: "nil"}
		}
	}

    return redisprotocol.Value{Type: "array", Array: results}
}

func (s *Server) handleHGetAll(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'HGETALL' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if fields, exists := s.kvstore.Hashes[key]; exists {
		result := make([]redisprotocol.Value, 0, len(fields)*2)
        for field, value := range fields {
			result = append(result, redisprotocol.Value{Type: "bulk", Bulk: field})
			result = append(result, redisprotocol.Value{Type: "bulk", Bulk: value})
        }
        return redisprotocol.Value{Type: "array", Array: result}
    }
    return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{}}
}

func (s *Server) handleSAdd(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'SADD' command requires at least 2 arguments"}
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
    return redisprotocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleSRem(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'SREM' command requires at least 2 arguments"}
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.Sets[key]; !exists {
        return redisprotocol.Value{Type: "integer", Num: 0}
    }

    removedCount := 0
    for _, member := range args[1:] {
        if _, exists := s.kvstore.Sets[key][member]; exists {
            delete(s.kvstore.Sets[key], member)
            removedCount++
        }
    }
    return redisprotocol.Value{Type: "integer", Num: removedCount}
}

func (s *Server) handleSMembers(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'SMEMBERS' command requires 1 argument"}
    }
    key := args[0]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if memberSet, exists := s.kvstore.Sets[key]; exists {
		values := make([]redisprotocol.Value, 0, len(memberSet))
        for member := range memberSet {
			values = append(values, redisprotocol.Value{Type: "bulk", Bulk: member})
        }
        return redisprotocol.Value{Type: "array", Array: values}
    }
    return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{}}
}

func (s *Server) handleSIsMember(args []string) redisprotocol.Value {
    if len(args) != 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'SISMEMBER' command requires 2 arguments"}
    }
    key := args[0]
    member := args[1]
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if set, exists := s.kvstore.Sets[key]; exists {
        if _, exists := set[member]; exists {
            return redisprotocol.Value{Type: "integer", Num: 1}
        }
    }
    return redisprotocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleZAdd(args []string) redisprotocol.Value {
    if len(args) < 3 || len(args)%2 != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'ZADD' command requires at least 3 arguments with score-member pairs"}
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
            return redisprotocol.Value{Type: "error", Str: "ERR score is not a valid number"}
        }
        member := args[i+1]
        if _, exists := s.kvstore.SortedSets[key][member]; !exists {
            s.kvstore.SortedSets[key][member] = score
            addedCount++
        }
    }
    return redisprotocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleZRange(args []string) redisprotocol.Value {
    if len(args) != 3 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'ZRANGE' command requires 3 arguments"}
    }
    key := args[0]
    start, err1 := strconv.Atoi(args[1])
    end, err2 := strconv.Atoi(args[2])
    s.kvstore.RLock()
    defer s.kvstore.RUnlock()

    if err1 != nil || err2 != nil {
        return redisprotocol.Value{Type: "error", Str: "ERR start or end is not a valid integer"}
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
            return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{}}
        }

        // Prepare the result
        result := make([]redisprotocol.Value, 0, end-start+1)
        for i := start; i <= end; i++ {
            result = append(result, redisprotocol.Value{Type: "bulk", Bulk: members[i]})
        }
        return redisprotocol.Value{Type: "array", Array: result}
    }
    return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{}}
}

func (s *Server) handleZRem(args []string) redisprotocol.Value {
    if len(args) < 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'ZREM' command requires at least 2 arguments"}
    }
    key := args[0]
    s.kvstore.Lock()
    defer s.kvstore.Unlock()

    if _, exists := s.kvstore.SortedSets[key]; !exists {
        return redisprotocol.Value{Type: "integer", Num: 0}
    }

    removedCount := 0
    for _, member := range args[1:] {
        if _, exists := s.kvstore.SortedSets[key][member]; exists {
            delete(s.kvstore.SortedSets[key], member)
            removedCount++
        }
    }
    return redisprotocol.Value{Type: "integer", Num: removedCount}
}


func (s *Server) handleExpire(args []string) redisprotocol.Value {
    if len(args) != 2 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'EXPIRE' command requires 2 arguments"}
    }
    key := args[0]
    seconds, err := strconv.ParseInt(args[1], 10, 64)
    if err != nil {
        return redisprotocol.Value{Type: "error", Str: "ERR seconds must be a valid integer"}
    }
    
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    
    if s.keyExistsUnlocked(key) {
        s.kvstore.Expirations[key] = time.Now().Add(time.Duration(seconds) * time.Second) // Set expiration time
        return redisprotocol.Value{Type: "integer", Num: 1}
    }
    return redisprotocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleTTL(args []string) redisprotocol.Value {
    if len(args) != 1 {
        return redisprotocol.Value{Type: "error", Str: "ERR 'TTL' command requires 1 argument"}
    }
    key := args[0]

    s.kvstore.RLock()
    expiration, exists := s.kvstore.Expirations[key]
    s.kvstore.RUnlock()
    
    if exists {
        if time.Now().Before(expiration) {
            // Calculate remaining TTL
            ttl := int(time.Until(expiration).Seconds())
            return redisprotocol.Value{Type: "integer", Num: ttl}
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
        
        return redisprotocol.Value{Type: "integer", Num: -2} // Indicate the key existed but has expired
    }
    return redisprotocol.Value{Type: "integer", Num: -1} // Key does not exist
}

func (s *Server) handleInfo(args []string) redisprotocol.Value {
    info := "Server Info:\n"
    info += fmt.Sprintf("Keys in store: %d\n", len(s.kvstore.Strings))
    info += fmt.Sprintf("Lists: %d\n", len(s.kvstore.Lists))
    info += fmt.Sprintf("Hashes: %d\n", len(s.kvstore.Hashes))
    info += fmt.Sprintf("Sets: %d\n", len(s.kvstore.Sets))
    info += fmt.Sprintf("Sorted Sets: %d\n", len(s.kvstore.SortedSets))
    return redisprotocol.Value{Type: "bulk", Bulk: info}
}

func (s *Server) handleFlushAll(args []string) redisprotocol.Value {
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
    s.kvstore.Strings = make(map[string]string)
    s.kvstore.Lists = make(map[string][]string)
    s.kvstore.Hashes = make(map[string]map[string]string)
    s.kvstore.Sets = make(map[string]map[string]struct{})
    s.kvstore.SortedSets = make(map[string]map[string]float64)
    return redisprotocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handlePing(args []string) redisprotocol.Value {
    return redisprotocol.Value{Type: "string", Str: "PONG"}
}

func (s *Server) handleSubscribe(args []string, conn net.Conn) redisprotocol.Value {
    if len(args) != 1 {
		return redisprotocol.Value{Type: "error", Str: "ERR 'SUBSCRIBE' command requires 1 argument"}
	}
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
	channel := args[0]
	pubsub.Subscribe(channel, conn) // Subscribe the connection to the channel
	return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{
		{Type: "bulk", Bulk: "subscribe"},
		{Type: "bulk", Bulk: channel},
		{Type: "integer", Num: 1},
	}}
}

func (s *Server) handlePublish(args []string) redisprotocol.Value {
    if len(args) < 2 {
		return redisprotocol.Value{Type: "error", Str: "ERR 'PUBLISH' command requires at least 2 arguments"}
	}
	channel := args[0]
	message := strings.Join(args[1:], " ")
	count := pubsub.Publish(channel, message) // Publish the message to the channel
	return redisprotocol.Value{Type: "integer", Num: count}
}   

func (s *Server) handleUnsubscribe(args []string, conn net.Conn) redisprotocol.Value {
    if len(args) != 1 {
		return redisprotocol.Value{Type: "error", Str: "ERR 'UNSUBSCRIBE' command requires 1 argument"}
	}
    s.kvstore.Lock()
    defer s.kvstore.Unlock()
	channel := args[0]
	pubsub.Unsubscribe(channel, conn) // Unsubscribe the connection from the channel
	return redisprotocol.Value{Type: "array", Array: []redisprotocol.Value{
		{Type: "bulk", Bulk: "unsubscribe"},
		{Type: "bulk", Bulk: channel},
		{Type: "integer", Num: 0},
	}}
}

func (s *Server) handleSave(args []string) redisprotocol.Value {
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	err := persistence.Save(s.kvstore)
	if err != nil {
		return redisprotocol.Value{Type: "error", Str: "ERR " + err.Error()}
	}
	return redisprotocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleBgsave(args []string) redisprotocol.Value {
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	persistence.Bgsave(s.kvstore)
	return redisprotocol.Value{Type: "string", Str: "Background saving started"}
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

func (s *Server) processCommand(command []string, conn net.Conn) redisprotocol.Value {
    fmt.Println("Received command:", command) // yo
    if len(command) == 0 {
        return redisprotocol.Value{Type: "error", Str: "ERR empty command"}
    }

    cmd := strings.ToUpper(command[0])
    args := command[1:]

    if cmd == "SUBSCRIBE" || cmd == "PUBLISH" || cmd == "UNSUBSCRIBE" {
        return s.handleCommandWithConn(cmd, args, conn)
    }

    if handler, ok := s.commands[cmd]; ok {
		return handler(args)
	}

	return redisprotocol.Value{Type: "error", Str: "ERR unknown command '" + cmd + "'"}
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

        response := server.processCommand(command, conn)
        err = resp.Write(response)
        if err != nil {
            fmt.Println("Error writing response:", err)
            return
        }
    }
}

func initializePersistence(server *Server) {
	if err := persistence.Load(server.kvstore); err != nil {
		fmt.Println("Warning:", err)
	}
}

func main() {
	port := "6378"
	server := NewServer()

    // Load existing data on startup
	initializePersistence(server)

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
