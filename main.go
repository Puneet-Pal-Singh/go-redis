package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"io"

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

func (s *Server) ExecuteCommand(input string) string {
    args := strings.Fields(input)
    if len(args) == 0 {
        return "ERR empty command"
    }

    command := strings.ToUpper(args[0])
    if cmd, ok := s.commands[command]; ok {
        return cmd(args[1:])
    }
    return "ERR unknown command '" + command + "'"
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
