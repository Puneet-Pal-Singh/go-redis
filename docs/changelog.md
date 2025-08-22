## Milestone: 1

This project is a Go implementation of a Redis-like server. It's a custom-built, in-memory database that mimics the functionality of Redis, a popular open-source data store.

Here’s a comprehensive breakdown of what the project does and how it works:

### Core Functionality

1.  **In-Memory Data Store**: At its heart, the project is a key-value store that holds all its data in memory. This makes it very fast for read and write operations. It supports several of Redis's core data structures:
    *   **Strings**: Simple key-value pairs.
    *   **Lists**: Ordered collections of strings.
    *   **Hashes**: Maps of field-value pairs.
    *   **Sets**: Unordered collections of unique strings.
    *   **Sorted Sets**: Sets where each member has an associated score, allowing them to be sorted.

2.  **Redis Command Implementation**: The server understands and responds to a wide range of standard Redis commands. The code has specific handler functions for commands like `SET`, `GET`, `LPUSH`, `HSET`, `SADD`, `ZADD`, and many more.

3.  **Networking**: It runs a TCP server that listens for client connections on port `6378`. It can handle multiple clients concurrently, with each client connection being managed in its own goroutine.

4.  **Redis Protocol (RESP)**: The server communicates with clients using the Redis Serialization Protocol (RESP). The `redisprotocol/resp.go` file contains a custom parser and writer for this protocol, which is how commands and data are exchanged between the server and clients.

5.  **Data Persistence**: To prevent data loss when the server restarts, it has a persistence mechanism.
    *   The `SAVE` command synchronously saves the entire dataset to a file named `data.rdb` in JSON format.
    *   The `BGSAVE` command does the same but in a background goroutine, so it doesn't block the server from handling other requests.
    *   When the server starts, it loads the data from `data.rdb` back into memory.

6.  **Publish/Subscribe (Pub/Sub)**: The project includes a pub/sub messaging system. Clients can `SUBSCRIBE` to channels, and other clients can `PUBLISH` messages to those channels. This is useful for real-time applications and event-driven architectures.

### How the Code is Structured

*   `main.go`: This is the main file that ties everything together. It defines the `KeyValueStore` and `Server` structs, registers all the command handlers, and starts the TCP server.
*   `persistance.go`: This file contains the logic for saving the in-memory database to disk and loading it back.
*   `pubsub.go`: This file implements the logic for the publish/subscribe system, managing subscribers and message broadcasting.
*   `redisprotocol/resp.go`: This is a self-contained package for handling the RESP protocol, which is crucial for communicating with Redis clients.

### In Summary

This project is a from-scratch implementation of a Redis server in Go. It's a great example of how to build a networked, concurrent, in-memory database. It covers many important concepts in software engineering, including data structures, networking, concurrency, and protocol implementation.