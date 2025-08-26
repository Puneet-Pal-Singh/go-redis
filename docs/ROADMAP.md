# Project Roadmap & Feature Vision

This document outlines the current capabilities and future development direction for the Go Redis project. Our vision is to evolve this server from a robust implementation of core data structures into a more feature-complete, production-grade application, guided by the most common and powerful Redis use cases.

## ✅ Part 1: Current Capabilities (What We've Achieved)

Our server already provides a powerful foundation, supporting many of the most critical Redis use cases out of the box.

### 1. Caching & Session Storage
- **Use Case:** Storing database query results, web page responses, or user session data for fast retrieval.
- **Supported by:** `GET`, `SET`, `DEL`, `EXPIRE`, `TTL`. The implementation of key expiration is fundamental to this use case.

### 2. Distributed Atomic Counters
- **Use Case:** Counting events across multiple application instances, such as API hits, likes, or views.
- **Supported by:** `INCR`, `DECR`, `INCRBY`, `DECRBY`. The atomicity of these operations is guaranteed by our server's concurrency model.

### 3. Real-time Leaderboards
- **Use Case:** Maintaining ordered lists of scores for gaming or ranking systems.
- **Supported by:** `ZADD`, `ZRANGE`, `ZREM`. Our Sorted Set implementation provides the core functionality for this popular feature.

### 4. Simple Message Queues
- **Use Case:** Managing background jobs, asynchronous tasks, or ensuring reliable message processing.
- **Supported by:** `LPUSH`/`RPOP` and `RPUSH`/`LPOP`. Our List commands can be used to implement reliable FIFO (First-In, First-Out) queues.

### 5. Data Structures for Modeling (e.g., Shopping Carts)
- **Use Case:** Storing complex but related data under a single key, like a user's shopping cart.
- **Supported by:** `HSET`, `HGET`, `HGETALL`, `HLEN`. Hashes provide the perfect structure for this.

### 6. Real-time Messaging (Pub/Sub)
- **Use Case:** Distributing messages to multiple subscribers in real-time for chat applications, notifications, or live updates.
- **Supported by:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`.

---

## 🚀 Part 2: The Next Frontier (High-Impact Features)

These are the next logical features to implement. They build upon our existing data structures to unlock more advanced, production-critical use cases.

### 1. Enhanced `SET` for Distributed Locking
- **Goal:** Allow users to acquire a distributed lock, ensuring only one process can access a shared resource at a time.
- **Path Forward:** Enhance the `SET` command to support the standard Redis options:
    - `NX`: Set the key only if it does not already exist.
    - `XX`: Set the key only if it already exists.
    - `EX seconds`: Set an expiration time in seconds.
    - `PX milliseconds`: Set an expiration time in milliseconds.
- **Impact:** This is the cornerstone of distributed systems coordination and a major feature.

### 2. `MULTI`/`EXEC` for Atomic Transactions
- **Goal:** Allow clients to group multiple commands into a single, atomic operation. If one command fails, none are executed.
- **Path Forward:** Implement the `MULTI`, `EXEC`, `DISCARD`, and `WATCH` commands. This involves creating a transaction context for each client that queues commands and executes them in a single locked step.
- **Impact:** Unlocks robust patterns for things like rate limiting and "check-and-set" operations, significantly increasing the reliability of the server.

### 3. `BYSCORE` Commands for Job Scheduling
- **Goal:** Enable the creation of job schedulers, where tasks are processed based on a future timestamp.
- **Path Forward:** Implement the `BYSCORE` family of Sorted Set commands:
    - `ZRANGEBYSCORE`: Get all items within a score range (e.g., all jobs due between now and 1 minute from now).
    - `ZREMRANGEBYSCORE`: Atomically fetch and remove items in a score range.
- **Impact:** Transforms our Sorted Sets from a simple ranking tool into a powerful time-based task management system.

---

## 🔭 Part 3: The Long-Term Vision (Advanced Modules)

These features represent the pinnacle of Redis's capabilities and would require implementing entirely new, complex data structures. They are the "moonshot" goals for the project.

### 1. Probabilistic Data Structures
- **Goal:** Add memory-efficient data structures for large-scale data analysis.
- **Features:**
    - **HyperLogLog:** For approximating the cardinality (number of unique elements) of a very large set. Would require `PFADD`, `PFCOUNT`.
    - **Bloom Filter:** For efficiently checking if an element is part of a set, with a small chance of false positives. Great for "user has already seen this" checks. Would require `BF.ADD`, `BF.EXISTS`.

### 2. Geospatial Indexing
- **Goal:** Store and query points based on their longitude and latitude.
- **Path Forward:** Implement Geohashing algorithms on top of our Sorted Sets and add the `GEOADD`, `GEORADIUS`, and `GEODIST` commands.
- **Impact:** Turns the server into a location-aware database, capable of answering "find all restaurants within 5km".

### 3. Redis Module Emulation (The Ultimate Goal)
- **Goal:** Emulate the functionality of major Redis Modules, which provide features like Full-Text Search, JSON document storage, and Time Series data.
- **Path Forward:** This would be a massive undertaking, likely involving creating a pluggable architecture for the server itself, where new "modules" could be added with their own data types and commands.

This roadmap is a living document and will evolve as the project grows.