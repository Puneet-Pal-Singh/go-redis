// cmd/redis-server/main.go
package main

import (
	"flag"
	"log" // Use the standard log for fatal errors before logger is set up
	"net"

	"github.com/Puneet-Pal-Singh/go-redis/internal/server"
	"github.com/Puneet-Pal-Singh/go-redis/pkg/logger"
	"go.uber.org/zap"                                  // The Zap library
)

func main() {
	// --- 1. Configuration Setup using Flags ---
	// Define command-line flags for port and database path.
	port := flag.String("port", "6378", "Port to listen on")
	dbPath := flag.String("dbpath", "data.rdb", "Path to the database file")
	flag.Parse() // Parse the flags provided by the user.

	// --- 2. Logger Initialization ---
	// Create a new structured logger instance using your logger package.
	appLogger, err := logger.NewDevelopment()
	if err != nil {
		// If logger fails, we can't use it, so fall back to standard log.
		log.Fatalf("failed to initialize logger: %v", err)
	}
	// Flushes any buffered log entries before the application exits. A good practice.
	defer appLogger.Sync()

	// --- 3. Server Startup ---
	appLogger.Info("Starting Go Redis server...",
		zap.String("port", *port),
		zap.String("db_path", *dbPath),
	)

	// Create the server instance, passing the configuration and the logger.
	srv := server.NewServer(*dbPath, appLogger)

	// Start listening for TCP connections.
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		appLogger.Fatal("Failed to start TCP listener", zap.Error(err))
	}
	defer listener.Close()

	appLogger.Info("Server is listening and ready to accept connections")

	// --- 4. Connection Handling Loop ---
	// Continuously accept and handle new client connections.
	for {
		conn, err := listener.Accept()
		if err != nil {
			appLogger.Error("Failed to accept new connection", zap.Error(err))
			continue // Don't crash the server, just log the error and continue.
		}
		// Handle each connection in a new goroutine.
		go srv.HandleConnection(conn)
	}
}