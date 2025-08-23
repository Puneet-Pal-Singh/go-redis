// cmd/redis-server/main.go
package main

import (
	"fmt"
	"net"

	"github.com/Puneet-Pal-Singh/go-redis/internal/server"
)

func main() {
	port := "6378"
	// Create the server, providing the persistence file path
	srv := server.NewServer("data.rdb")

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
		// Let the server handle the connection in a new goroutine
		go srv.HandleConnection(conn)
	}
}