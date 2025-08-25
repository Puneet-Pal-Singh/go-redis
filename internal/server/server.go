// internal/server/server.go
package server

import (
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Puneet-Pal-Singh/go-redis/internal/protocol"
	"github.com/Puneet-Pal-Singh/go-redis/internal/pubsub"
	"github.com/Puneet-Pal-Singh/go-redis/internal/storage"
	
	"go.uber.org/zap"
	"github.com/Puneet-Pal-Singh/go-redis/internal/metrics"
)

type CommandFunc func([]string) protocol.Value

type Server struct {
	kvstore     *storage.KeyValueStore
	pubsub      *pubsub.PubSub
	persistence *storage.Persistence
	commands    map[string]CommandFunc
	log         *zap.Logger 
}

func NewServer(persistencePath string, logger *zap.Logger) *Server {
	s := &Server{
		kvstore:     storage.NewKeyValueStore(),
		pubsub:      pubsub.NewPubSub(),
		persistence: storage.NewPersistence(persistencePath),
		commands:    make(map[string]CommandFunc),
		log:         logger,
	}
	s.registerCommands()
	s.initializePersistence()
	return s
}

func (s *Server) initializePersistence() {
	if err := s.persistence.Load(s.kvstore); err != nil {
		s.log.Warn("Failed to load data from persistence file", zap.Error(err))
	}
}

func (s *Server) registerCommands() {
	s.commands = map[string]CommandFunc{
		"GET":       s.handleGet,
		"SET":       s.handleSet,
		"DEL":       s.handleDel,
		"EXISTS":    s.handleExists,
		"INCR":      s.handleIncr,
		"DECR":      s.handleDecr,
		"INCRBY":    s.handleIncrBy,
		"DECRBY":    s.handleDecrBy,
		"MSET":      s.handleMSet,
		"MGET":      s.handleMGet,
		"LPUSH":     s.handleLPush,
		"LPOP":      s.handleLPop,
		"LLEN":      s.handleLLen,
		"RPUSH":     s.handleRPush,
		"RPOP":      s.handleRPop,
		"HSET":      s.handleHSet,
		"HGET":      s.handleHGet,
		"HDEL":      s.handleHDel,
		"HLEN":      s.handleHLen,
		"HMGET":     s.handleHMGet,
		"HGETALL":   s.handleHGetAll,
		"SADD":      s.handleSAdd,
		"SREM":      s.handleSRem,
		"SMEMBERS":  s.handleSMembers,
		"SISMEMBER": s.handleSIsMember,
		"ZADD":      s.handleZAdd,
		"ZRANGE":    s.handleZRange,
		"ZREM":      s.handleZRem,
		"EXPIRE":    s.handleExpire,
		"TTL":       s.handleTTL,
		"INFO":      s.handleInfo,
		"FLUSHALL":  s.handleFlushAll,
		"PING":      s.handlePing,
		"SAVE":      s.handleSave,
		"BGSAVE":    s.handleBgsave,
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	// Increment the gauge when a client connects
    metrics.ConnectedClients.Inc()
    // Use defer to ensure the gauge is decremented when the function returns
    defer metrics.ConnectedClients.Dec()

	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	// Add client address for context
	s.log.Info("Client connected", zap.String("remote_addr", clientAddr))

	resp := protocol.NewResp(conn, conn)

	for {
		command, err := s.readCommand(resp)
		if err != nil {
			if err == io.EOF {
				s.log.Info("Client disconnected", zap.String("remote_addr", conn.RemoteAddr().String()))
				return
			}
			s.log.Error("Error reading command", zap.Error(err))
			return
		}

		response := s.processCommand(command, conn)
		err = resp.Write(response)
		if err != nil {
			s.log.Error("Error writing response", zap.Error(err))
			return
		}
	}
}

func (s *Server) readCommand(resp *protocol.Resp) ([]string, error) {
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

func (s *Server) processCommand(command []string, conn net.Conn) protocol.Value {
	if len(command) > 0 {
		s.log.Debug("Processing command",
			zap.String("command", command[0]),
			zap.Strings("args", command[1:]),
			zap.String("client", conn.RemoteAddr().String()),
		)
	}

	if len(command) == 0 {
		return protocol.Value{Type: "error", Str: "ERR empty command"}
	}

	cmd := strings.ToUpper(command[0])
	args := command[1:]

	// Increment the counter for the specific command
    metrics.CommandsProcessed.WithLabelValues(cmd).Inc()

	if cmd == "SUBSCRIBE" || cmd == "PUBLISH" || cmd == "UNSUBSCRIBE" {
		return s.handleCommandWithConn(cmd, args, conn)
	}

	if handler, ok := s.commands[cmd]; ok {
		return handler(args)
	}

	return protocol.Value{Type: "error", Str: "ERR unknown command '" + cmd + "'"}
}

func (s *Server) keyExistsUnlocked(key string) bool {
	_, sExists := s.kvstore.Strings[key]
	_, lExists := s.kvstore.Lists[key]
	_, hExists := s.kvstore.Hashes[key]
	_, setExists := s.kvstore.Sets[key]
	_, zExists := s.kvstore.SortedSets[key]
	return sExists || lExists || hExists || setExists || zExists
}

// This private helper deletes a key from all possible data structures.
// It is "unlocked" because it assumes the caller has already acquired the necessary write lock on the KeyValueStore.
func (s *Server) deleteKeyUnlocked(key string) {
	delete(s.kvstore.Strings, key)
	delete(s.kvstore.Lists, key)
	delete(s.kvstore.Hashes, key)
	delete(s.kvstore.Sets, key)
	delete(s.kvstore.SortedSets, key)
	delete(s.kvstore.Expirations, key)
}

// This checks if a key has an active expiration and if the current
// time has passed it. If so, it acquires a write lock and deletes the key
// from all data stores. It returns true if the key was expired and deleted,
// otherwise false. This is the core of the "lazy expiration" mechanism.
func (s *Server) deleteIfExpired(key string) bool {
	// Use a read lock first for a quick check.
	s.kvstore.RLock()
	expiration, hasExpiry := s.kvstore.Expirations[key]
	s.kvstore.RUnlock()

	if hasExpiry && time.Now().After(expiration) {
		// If expired, now acquire a write lock to delete.
		s.kvstore.Lock()
		defer s.kvstore.Unlock()
		// Re-check the condition after acquiring the write lock to avoid race conditions.
		// The key might have been deleted or updated by another goroutine.
		expiration, hasExpiry = s.kvstore.Expirations[key]
		if hasExpiry && time.Now().After(expiration) {
			s.deleteKeyUnlocked(key)
			return true // Key was expired and deleted.
		}
	}
	return false // Key is not expired or has no expiry.
}

func (s *Server) handleCommandWithConn(cmd string, args []string, conn net.Conn) protocol.Value {
	switch cmd {
	case "SUBSCRIBE":
		return s.handleSubscribe(args, conn)
	case "PUBLISH":
		return s.handlePublish(args)
	case "UNSUBSCRIBE":
		return s.handleUnsubscribe(args, conn)
	default:
		return protocol.Value{Type: "error", Str: "ERR unknown command '" + cmd + "'"}
	}
}

func (s *Server) handleGet(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'GET' command requires 1 argument"}
	}
	key := args[0]

	// First, check for and handle expiration. This is the crucial bug fix.
	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "nil"} // The key was expired and deleted.
	}

	// Now we can safely read the value.
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()

	if value, ok := s.kvstore.Strings[key]; ok {
		return protocol.Value{Type: "bulk", Bulk: value}
	}
	return protocol.Value{Type: "nil"}
}

func (s *Server) handleSet(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR 'SET' command requires 2 arguments"}
	}
	key, value := args[0], args[1]
	s.kvstore.Lock()
	defer s.kvstore.Unlock()

	s.kvstore.Strings[key] = value
	return protocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleDel(args []string) protocol.Value {
	if len(args) < 1 {
		return protocol.Value{Type: "error", Str: "ERR 'DEL' command requires at least 1 argument"}
	}
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	deletedCount := 0
	for _, key := range args {
		if s.keyExistsUnlocked(key) {
			deletedCount++
		}
		s.deleteKeyUnlocked(key)
	}
	return protocol.Value{Type: "integer", Num: deletedCount}
}

func (s *Server) handleExists(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'EXISTS' command requires 1 argument"}
	}
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if s.keyExistsUnlocked(args[0]) {
		return protocol.Value{Type: "integer", Num: 1}
	}
	return protocol.Value{Type: "integer", Num: 0}
}

func (s *Server) applyDelta(key string, delta int64) protocol.Value {
	s.deleteIfExpired(key)
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	value, exists := s.kvstore.Strings[key]
	if !exists {
		value = "0"
	}
	intValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return protocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	intValue += delta
	s.kvstore.Strings[key] = strconv.FormatInt(intValue, 10)
	return protocol.Value{Type: "integer", Num: int(intValue)}
}

func (s *Server) handleIncr(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'incr' command"}
	}
	return s.applyDelta(args[0], 1)
}

func (s *Server) handleDecr(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'decr' command"}
	}
	return s.applyDelta(args[0], -1)
}

func (s *Server) handleIncrBy(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'incrby' command"}
	}
	increment, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	return s.applyDelta(args[0], increment)
}

func (s *Server) handleDecrBy(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR wrong number of arguments for 'decrby' command"}
	}
	decrement, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}
	return s.applyDelta(args[0], -decrement)
}

func (s *Server) handleMSet(args []string) protocol.Value {
	if len(args)%2 != 0 {
		return protocol.Value{Type: "error", Str: "ERR 'MSET' command requires an even number of arguments"}
	}
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	for i := 0; i < len(args); i += 2 {
		s.kvstore.Strings[args[i]] = args[i+1]
	}
	return protocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleMGet(args []string) protocol.Value {
	if len(args) < 1 {
		return protocol.Value{Type: "error", Str: "ERR 'MGET' command requires at least 1 argument"}
	}
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	results := make([]protocol.Value, len(args))
	for i, key := range args {
		if value, exists := s.kvstore.Strings[key]; exists {
			results[i] = protocol.Value{Type: "bulk", Bulk: value}
		} else {
			results[i] = protocol.Value{Type: "nil"}
		}
	}
	return protocol.Value{Type: "array", Array: results}
}

func (s *Server) handleLPush(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'LPUSH' command requires at least 2 arguments"}
	}
	key := args[0]
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if _, exists := s.kvstore.Lists[key]; !exists {
		s.kvstore.Lists[key] = make([]string, 0)
	}
	for _, value := range args[1:] {
		s.kvstore.Lists[key] = append([]string{value}, s.kvstore.Lists[key]...)
	}
	return protocol.Value{Type: "integer", Num: len(s.kvstore.Lists[key])}
}

func (s *Server) handleLPop(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'LPOP' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "nil"}
	}

	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if list, exists := s.kvstore.Lists[key]; exists && len(list) > 0 {
		poppedValue := list[0]
		s.kvstore.Lists[key] = s.kvstore.Lists[key][1:]
		return protocol.Value{Type: "bulk", Bulk: poppedValue}
	}
	return protocol.Value{Type: "nil"}
}

func (s *Server) handleLLen(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'LLEN' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if list, exists := s.kvstore.Lists[key]; exists {
		return protocol.Value{Type: "integer", Num: len(list)}
	}
	return protocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleRPush(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'RPUSH' command requires at least 2 arguments"}
	}
	key := args[0]
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if _, exists := s.kvstore.Lists[key]; !exists {
		s.kvstore.Lists[key] = make([]string, 0)
	}
	values := args[1:]
	s.kvstore.Lists[key] = append(s.kvstore.Lists[key], values...)
	return protocol.Value{Type: "integer", Num: len(s.kvstore.Lists[key])}
}

func (s *Server) handleRPop(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'RPOP' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "nil"}
	}

	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if value, exists := s.kvstore.Lists[key]; exists && len(value) > 0 {
		poppedValue := value[len(value)-1]
		s.kvstore.Lists[key] = value[:len(value)-1]
		return protocol.Value{Type: "bulk", Bulk: poppedValue}
	}
	return protocol.Value{Type: "nil"}
}

func (s *Server) handleHSet(args []string) protocol.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'HSET' command requires at least 3 arguments with key-value pairs"}
	}
	key := args[0]
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
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
	return protocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleHGet(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR 'HGET' command requires 2 arguments"}
	}
	key := args[0]
	field := args[1]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "nil"}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if value, exists := s.kvstore.Hashes[key][field]; exists {
		return protocol.Value{Type: "bulk", Bulk: value}
	}
	return protocol.Value{Type: "nil"}
}

func (s *Server) handleHDel(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'HDEL' command requires at least 2 arguments"}
	}
	key := args[0]
	fields := args[1:]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if _, exists := s.kvstore.Hashes[key]; !exists {
		return protocol.Value{Type: "integer", Num: 0}
	}
	count := 0
	for _, field := range fields {
		if _, exists := s.kvstore.Hashes[key][field]; exists {
			delete(s.kvstore.Hashes[key], field)
			count++
		}
	}
	return protocol.Value{Type: "integer", Num: count}
}

func (s *Server) handleHLen(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'HLEN' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if fields, exists := s.kvstore.Hashes[key]; exists {
		return protocol.Value{Type: "integer", Num: len(fields)}
	}
	return protocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleHMGet(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'HMGET' command requires at least 2 arguments"}
	}
	key := args[0]
	fields := args[1:]

	if s.deleteIfExpired(key) {
		results := make([]protocol.Value, len(fields))
		for i := range fields {
			results[i] = protocol.Value{Type: "nil"}
		}
		return protocol.Value{Type: "array", Array: results}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	results := make([]protocol.Value, len(fields))
	hash, exists := s.kvstore.Hashes[key]
	if !exists {
		for i := range fields {
			results[i] = protocol.Value{Type: "nil"}
		}
		return protocol.Value{Type: "array", Array: results}
	}
	for i, field := range fields {
		if value, ok := hash[field]; ok {
			results[i] = protocol.Value{Type: "bulk", Bulk: value}
		} else {
			results[i] = protocol.Value{Type: "nil"}
		}
	}
	return protocol.Value{Type: "array", Array: results}
}

func (s *Server) handleHGetAll(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'HGETALL' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "array", Array: []protocol.Value{}}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if fields, exists := s.kvstore.Hashes[key]; exists {
		result := make([]protocol.Value, 0, len(fields)*2)
		for field, value := range fields {
			result = append(result, protocol.Value{Type: "bulk", Bulk: field})
			result = append(result, protocol.Value{Type: "bulk", Bulk: value})
		}
		return protocol.Value{Type: "array", Array: result}
	}
	return protocol.Value{Type: "array", Array: []protocol.Value{}}
}

func (s *Server) handleSAdd(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'SADD' command requires at least 2 arguments"}
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
	return protocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleSRem(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'SREM' command requires at least 2 arguments"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if _, exists := s.kvstore.Sets[key]; !exists {
		return protocol.Value{Type: "integer", Num: 0}
	}
	removedCount := 0
	for _, member := range args[1:] {
		if _, exists := s.kvstore.Sets[key][member]; exists {
			delete(s.kvstore.Sets[key], member)
			removedCount++
		}
	}
	return protocol.Value{Type: "integer", Num: removedCount}
}

func (s *Server) handleSMembers(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'SMEMBERS' command requires 1 argument"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "array", Array: []protocol.Value{}}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if memberSet, exists := s.kvstore.Sets[key]; exists {
		values := make([]protocol.Value, 0, len(memberSet))
		for member := range memberSet {
			values = append(values, protocol.Value{Type: "bulk", Bulk: member})
		}
		return protocol.Value{Type: "array", Array: values}
	}
	return protocol.Value{Type: "array", Array: []protocol.Value{}}
}

func (s *Server) handleSIsMember(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR 'SISMEMBER' command requires 2 arguments"}
	}
	key := args[0]
	member := args[1]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if set, exists := s.kvstore.Sets[key]; exists {
		if _, exists := set[member]; exists {
			return protocol.Value{Type: "integer", Num: 1}
		}
	}
	return protocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleZAdd(args []string) protocol.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'ZADD' command requires at least 3 arguments with score-member pairs"}
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
			return protocol.Value{Type: "error", Str: "ERR score is not a valid number"}
		}
		member := args[i+1]
		if _, exists := s.kvstore.SortedSets[key][member]; !exists {
			s.kvstore.SortedSets[key][member] = score
			addedCount++
		}
	}
	return protocol.Value{Type: "integer", Num: addedCount}
}

func (s *Server) handleZRange(args []string) protocol.Value {
	if len(args) != 3 {
		return protocol.Value{Type: "error", Str: "ERR 'ZRANGE' command requires 3 arguments"}
	}
	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	end, err2 := strconv.Atoi(args[2])

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "array", Array: []protocol.Value{}}
	}

	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	if err1 != nil || err2 != nil {
		return protocol.Value{Type: "error", Str: "ERR start or end is not a valid integer"}
	}
	if sortedSet, exists := s.kvstore.SortedSets[key]; exists {
		var members []string
		for member := range sortedSet {
			members = append(members, member)
		}
		sort.Slice(members, func(i, j int) bool {
			return sortedSet[members[i]] < sortedSet[members[j]]
		})
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
			return protocol.Value{Type: "array", Array: []protocol.Value{}}
		}
		result := make([]protocol.Value, 0, end-start+1)
		for i := start; i <= end; i++ {
			result = append(result, protocol.Value{Type: "bulk", Bulk: members[i]})
		}
		return protocol.Value{Type: "array", Array: result}
	}
	return protocol.Value{Type: "array", Array: []protocol.Value{}}
}

func (s *Server) handleZRem(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'ZREM' command requires at least 2 arguments"}
	}
	key := args[0]

	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: 0}
	}

	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if _, exists := s.kvstore.SortedSets[key]; !exists {
		return protocol.Value{Type: "integer", Num: 0}
	}
	removedCount := 0
	for _, member := range args[1:] {
		if _, exists := s.kvstore.SortedSets[key][member]; exists {
			delete(s.kvstore.SortedSets[key], member)
			removedCount++
		}
	}
	return protocol.Value{Type: "integer", Num: removedCount}
}

func (s *Server) handleExpire(args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: "error", Str: "ERR 'EXPIRE' command requires 2 arguments"}
	}
	key := args[0]
	seconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.Value{Type: "error", Str: "ERR seconds must be a valid integer"}
	}
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	if s.keyExistsUnlocked(key) {
		s.kvstore.Expirations[key] = time.Now().Add(time.Duration(seconds) * time.Second)
		return protocol.Value{Type: "integer", Num: 1}
	}
	return protocol.Value{Type: "integer", Num: 0}
}

func (s *Server) handleTTL(args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'TTL' command requires 1 argument"}
	}
	key := args[0]

	// The helper now handles checking and deleting if expired.
	if s.deleteIfExpired(key) {
		return protocol.Value{Type: "integer", Num: -2} // Key existed but was expired.
	}

	// If we are here, the key is not expired. Check if it exists and has a TTL.
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()

	if !s.keyExistsUnlocked(key) {
		return protocol.Value{Type: "integer", Num: -2} // Key does not exist.
	}

	if expiration, hasExpiry := s.kvstore.Expirations[key]; hasExpiry {
		ttl := int(time.Until(expiration).Seconds())
		return protocol.Value{Type: "integer", Num: ttl}
	}

	return protocol.Value{Type: "integer", Num: -1} // Key exists but has no expiry.
}

func (s *Server) handleInfo(args []string) protocol.Value {
	info := "Server Info:\n"
	info += fmt.Sprintf("Keys in store: %d\n", len(s.kvstore.Strings))
	info += fmt.Sprintf("Lists: %d\n", len(s.kvstore.Lists))
	info += fmt.Sprintf("Hashes: %d\n", len(s.kvstore.Hashes))
	info += fmt.Sprintf("Sets: %d\n", len(s.kvstore.Sets))
	info += fmt.Sprintf("Sorted Sets: %d\n", len(s.kvstore.SortedSets))
	return protocol.Value{Type: "bulk", Bulk: info}
}

func (s *Server) handleFlushAll(args []string) protocol.Value {
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	s.kvstore.Strings = make(map[string]string)
	s.kvstore.Lists = make(map[string][]string)
	s.kvstore.Hashes = make(map[string]map[string]string)
	s.kvstore.Sets = make(map[string]map[string]struct{})
	s.kvstore.SortedSets = make(map[string]map[string]float64)
	return protocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handlePing(args []string) protocol.Value {
	return protocol.Value{Type: "string", Str: "PONG"}
}

func (s *Server) handleSubscribe(args []string, conn net.Conn) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'SUBSCRIBE' command requires 1 argument"}
	}
	channel := args[0]
	s.pubsub.Subscribe(channel, conn)
	return protocol.Value{Type: "array", Array: []protocol.Value{
		{Type: "bulk", Bulk: "subscribe"},
		{Type: "bulk", Bulk: channel},
		{Type: "integer", Num: 1},
	}}
}

func (s *Server) handlePublish(args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: "error", Str: "ERR 'PUBLISH' command requires at least 2 arguments"}
	}
	channel := args[0]
	message := strings.Join(args[1:], " ")
	count := s.pubsub.Publish(channel, message)
	return protocol.Value{Type: "integer", Num: count}
}

func (s *Server) handleUnsubscribe(args []string, conn net.Conn) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: "error", Str: "ERR 'UNSUBSCRIBE' command requires 1 argument"}
	}
	channel := args[0]
	s.pubsub.Unsubscribe(channel, conn)
	return protocol.Value{Type: "array", Array: []protocol.Value{
		{Type: "bulk", Bulk: "unsubscribe"},
		{Type: "bulk", Bulk: channel},
		{Type: "integer", Num: 0},
	}}
}

func (s *Server) handleSave(args []string) protocol.Value {
	s.kvstore.Lock()
	defer s.kvstore.Unlock()
	err := s.persistence.Save(s.kvstore)
	if err != nil {
		return protocol.Value{Type: "error", Str: "ERR " + err.Error()}
	}
	return protocol.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleBgsave(args []string) protocol.Value {
	s.kvstore.RLock()
	defer s.kvstore.RUnlock()
	s.persistence.Bgsave(s.kvstore, s.log)
	return protocol.Value{Type: "string", Str: "Background saving started"}
}
