### Testing Your `go-redis` Server

Now that the server is fixed, here's how you can build, run, and test it.

#### Step 1: Build and Run the Server

Open a terminal in the project directory and run the following commands:

```bash
# Build the application
go build -o main .

# Run the server
./main
```

You should see the output Server listening on :6378. Leave this terminal running.

#### Step 2: Test with `redis-cli`
Open a new terminal window and use redis-cli to connect to your server. I've prepared a script with commands to test all the implemented features.

You can run these commands one by one in the `redis-cli` prompt:


```bash
# Connect to the server
redis-cli -p 6378

# --- Test Server/Connection Commands ---
PING
# Expected: PONG

INFO
# Expected: Server info...

# --- Test String Commands ---
SET mykey "hello world"
# Expected: OK
GET mykey
# Expected: "hello world"
GET non_existent_key
# Expected: (nil)
INCR counter
# Expected: (integer) 1
INCR counter
# Expected: (integer) 2
DECR counter
# Expected: (integer) 1
INCRBY counter 10
# Expected: (integer) 11
DECRBY counter 5
# Expected: (integer) 6
MSET key1 val1 key2 val2
# Expected: OK
MGET key1 key2 mykey
# Expected:
# 1) "val1"
# 2) "val2"
# 3) "hello world"
EXISTS counter
# Expected: (integer) 1
DEL counter
# Expected: (integer) 1
EXISTS counter
# Expected: (integer) 0

# --- Test List Commands ---
LPUSH mylist "world"
# Expected: (integer) 1
LPUSH mylist "hello"
# Expected: (integer) 2
RPUSH mylist "!"
# Expected: (integer) 3
LLEN mylist
# Expected: (integer) 3
LPOP mylist
# Expected: "hello"
RPOP mylist
# Expected: "!"
LLEN mylist
# Expected: (integer) 1
DEL mylist
# Expected: (integer) 1

# --- Test Hash Commands ---
HSET myhash field1 "Hello"
# Expected: (integer) 1
HSET myhash field2 "World"
# Expected: (integer) 1
HGET myhash field1
# Expected: "Hello"
HLEN myhash
# Expected: (integer) 2
HMGET myhash field1 field2 nofield
# Expected:
# 1) "Hello"
# 2) "World"
# 3) (nil)
HGETALL myhash
# Expected (order may vary):
# 1) "field1"
# 2) "Hello"
# 3) "field2"
# 4) "World"
HDEL myhash field1
# Expected: (integer) 1
HDEL myhash field1
# Expected: (integer) 0
DEL myhash
# Expected: (integer) 1

# --- Test Set Commands ---
SADD myset "a" "b" "c"
# Expected: (integer) 3
SADD myset "a"
# Expected: (integer) 0
SMEMBERS myset
# Expected (order may vary):
# 1) "a"
# 2) "b"
# 3) "c"
SISMEMBER myset "b"
# Expected: (integer) 1
SISMEMBER myset "d"
# Expected: (integer) 0
SREM myset "c"
# Expected: (integer) 1
DEL myset
# Expected: (integer) 1

# --- Test Sorted Set Commands ---
ZADD myzset 1 "one" 2 "two" 3 "three"
# Expected: (integer) 3
ZRANGE myzset 0 -1
# Expected:
# 1) "one"
# 2) "two"
# 3) "three"
ZREM myzset "two"
# Expected: (integer) 1
DEL myzset
# Expected: (integer) 1

# --- Test Expiration ---
SET tempkey "will expire"
# Expected: OK
EXPIRE tempkey 5
# Expected: (integer) 1
TTL tempkey
# Expected: (integer) 5 (or less)
# Wait 6 seconds...
GET tempkey
# Expected: (nil)
TTL tempkey
# Expected: (integer) -2

# --- Test Persistence ---
SET pkey "persistent"
# Expected: OK
SAVE
# Expected: OK
FLUSHALL
# Expected: OK
GET pkey
# Expected: (nil)

# Now, quit redis-cli (type QUIT) and restart your Go server.
# The pkey should be loaded from data.rdb
# Reconnect with `redis-cli -p 6378`
GET pkey
# Expected: "persistent"
FLUSHALL
# Expected: OK
SAVE
# Expected: OK

# --- Exit redis-cli ---
QUIT
```

#### Step 3: Test Pub/Sub
For Pub/Sub, you'll need three terminal windows.

- Terminal 1: Your go-redis server is running.
- Terminal 2: A subscriber client.
- Terminal 3: A publisher client.

#### In Terminal 2 (Subscriber):


```bash
redis-cli -p 6378
SUBSCRIBE mychannel
```

This terminal will now wait for messages on `mychannel`.

#### In Terminal 3 (Publisher):
```bash
redis-cli -p 6378
PUBLISH mychannel "Hello from the publisher!"
# Expected: (integer) 1
```

#### Back in Terminal 2 (Subscriber), you should see:

```plaintext
1) "subscribe"
2) "mychannel"
3) (integer) 1
1) "message"
2) "mychannel"
3) "Hello from the publisher!"
```

This confirms your Pub/Sub system is working correctly!

These changes and tests should give you a much more robust and compliant Redis server.