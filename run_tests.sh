#!/bin/bash

# --- Configuration ---
SERVER_PORT=6378
BINARY_PATH="bin/go-redis"
MAIN_PACKAGE_PATH="./cmd/redis-server/"
REPORT_FILE="test_results.md"
SERVER_PID=0

# --- Colors for better output ---
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Helper Functions ---

start_server() {
    echo -e "${BLUE}--- Starting Redis Server ---${NC}"
    ./$BINARY_PATH > server.log 2>&1 &
    SERVER_PID=$!
    sleep 1
    if ! ps -p $SERVER_PID > /dev/null; then
        echo -e "${RED}Server failed to start. Check server.log for details.${NC}"
        cat server.log
        exit 1
    fi
    echo -e "${GREEN}Server started with PID: $SERVER_PID${NC}"
}

stop_server() {
    if [[ $SERVER_PID -ne 0 ]]; then
        echo -e "${BLUE}--- Stopping Redis Server (PID: $SERVER_PID) ---${NC}"
        kill $SERVER_PID
        wait $SERVER_PID 2>/dev/null
        echo -e "${GREEN}Server stopped.${NC}"
        SERVER_PID=0
    fi
}

# CORRECTED test function
# Arguments:
# 1. Test Description
# 2. Expected result
# 3. Comparison mode
# 4+ The rest of the arguments are the command and its parts
run_test() {
    local description="$1"
    local expected="$2"
    local mode="$3"
    shift 3 # Discard the first 3 args, the rest is the command
    local command_parts=("$@")

    # The --raw flag is crucial to get clean output
    # Passing args as an array ("${command_parts[@]}") prevents word splitting
    local actual=$(redis-cli -p $SERVER_PORT --raw "${command_parts[@]}" 2>&1)
    local command_str="${command_parts[*]}"

    # Prepare for logging
    echo "#### Test: \`$command_str\`" >> $REPORT_FILE
    echo "**Expected:**" >> $REPORT_FILE
    echo '```' >> $REPORT_FILE
    echo -e "$expected" >> $REPORT_FILE
    echo '```' >> $REPORT_FILE
    echo "**Actual:**" >> $REPORT_FILE
    echo '```' >> $REPORT_FILE
    echo -e "$actual" >> $REPORT_FILE
    echo '```' >> $REPORT_FILE

    local passed=false
    case "$mode" in
        "exact")
            if [[ "$actual" == "$expected" ]]; then
                passed=true
            fi
            ;;
        "ttl")
            if [[ "$actual" -le "$expected" && "$actual" -ge 0 ]]; then
                passed=true
            fi
            ;;
        "sorted")
            local actual_sorted=$(echo -e "$actual" | sort)
            local expected_sorted=$(echo -e "$expected" | sort)
            if [[ "$actual_sorted" == "$expected_sorted" ]]; then
                passed=true
            fi
            ;;
    esac

    if $passed; then
        echo -e "✅  ${GREEN}PASS${NC}: $description"
        echo "**Result:** <font color='green'>PASS</font>" >> $REPORT_FILE
        return 0
    else
        echo -e "❌  ${RED}FAIL${NC}: $description"
        echo "**Result:** <font color='red'>FAIL</font>" >> $REPORT_FILE
        CURRENT_GROUP_PASSED=false
        return 1
    fi
}

run_test_group() {
    local group_name="$1"
    echo -e "\n${BLUE}--- Testing $group_name ---${NC}"
    echo -e "\n## $group_name" >> $REPORT_FILE
    CURRENT_GROUP_PASSED=true
    eval "$2"
    if $CURRENT_GROUP_PASSED; then
        echo -e "\n${GREEN}$group_name - true${NC}"
    else
        echo -e "\n${RED}$group_name - false${NC}"
    fi
}

# --- Test Definitions (Corrected Calls) ---

server_commands() {
    run_test "PING" "PONG" "exact" PING
    # NOTE: FLUSHALL is run at the start of string_commands to ensure count is 0
    run_test "INFO" $'Server Info:\nKeys in store: 0\nLists: 0\nHashes: 0\nSets: 0\nSorted Sets: 0' "exact" INFO
}

string_commands() {
    run_test "FLUSHALL before strings" "OK" "exact" FLUSHALL
    run_test "SET" "OK" "exact" SET mykey "hello world"
    run_test "GET" "hello world" "exact" GET mykey
    run_test "GET non-existent" "" "exact" GET non_existent_key
    run_test "INCR" "1" "exact" INCR counter
    run_test "INCR again" "2" "exact" INCR counter
    run_test "DECR" "1" "exact" DECR counter
    run_test "INCRBY" "11" "exact" INCRBY counter 10
    run_test "DECRBY" "6" "exact" DECRBY counter 5
    run_test "MSET" "OK" "exact" MSET key1 val1 key2 val2
    run_test "MGET" $'val1\nval2\nhello world' "exact" MGET key1 key2 mykey
    run_test "EXISTS" "1" "exact" EXISTS counter
    run_test "DEL" "1" "exact" DEL counter
    run_test "EXISTS after DEL" "0" "exact" EXISTS counter
}

list_commands() {
    run_test "LPUSH first" "1" "exact" LPUSH mylist "world"
    run_test "LPUSH second" "2" "exact" LPUSH mylist "hello"
    run_test "RPUSH" "3" "exact" RPUSH mylist "!"
    run_test "LLEN" "3" "exact" LLEN mylist
    run_test "LPOP" "hello" "exact" LPOP mylist
    run_test "RPOP" "!" "exact" RPOP mylist
    run_test "LLEN after pops" "1" "exact" LLEN mylist
    run_test "DEL list" "1" "exact" DEL mylist
}

hash_commands() {
    run_test "HSET first" "1" "exact" HSET myhash field1 "Hello"
    run_test "HSET second" "1" "exact" HSET myhash field2 "World"
    run_test "HGET" "Hello" "exact" HGET myhash field1
    run_test "HLEN" "2" "exact" HLEN myhash
    run_test "HMGET" $'Hello\nWorld' "exact" HMGET myhash field1 field2 nofield
    run_test "HGETALL" $'field1\nHello\nfield2\nWorld' "sorted" HGETALL myhash
    run_test "HDEL existing" "1" "exact" HDEL myhash field1
    run_test "HDEL non-existing" "0" "exact" HDEL myhash field1
    run_test "DEL hash" "1" "exact" DEL myhash
}

set_commands() {
    run_test "SADD" "3" "exact" SADD myset "a" "b" "c"
    run_test "SADD existing" "0" "exact" SADD myset "a"
    run_test "SMEMBERS" $'a\nb\nc' "sorted" SMEMBERS myset
    run_test "SISMEMBER existing" "1" "exact" SISMEMBER myset "b"
    run_test "SISMEMBER non-existing" "0" "exact" SISMEMBER myset "d"
    run_test "SREM" "1" "exact" SREM myset "c"
    run_test "DEL set" "1" "exact" DEL myset
}

sorted_set_commands() {
    run_test "ZADD" "3" "exact" ZADD myzset 1 "one" 2 "two" 3 "three"
    run_test "ZRANGE" $'one\ntwo\nthree' "exact" ZRANGE myzset 0 -1
    run_test "ZREM" "1" "exact" ZREM myzset "two"
    run_test "DEL zset" "1" "exact" DEL myzset
}

expiration_commands() {
    run_test "SET temp" "OK" "exact" SET tempkey "will expire"
    run_test "EXPIRE" "1" "exact" EXPIRE tempkey 5
    run_test "TTL" "5" "ttl" TTL tempkey
    echo -e "${BLUE}--- Waiting for key to expire (6 seconds) ---${NC}"
    echo -e "\n> *Waiting 6 seconds for key to expire...*" >> $REPORT_FILE
    sleep 6
    run_test "GET expired key" "" "exact" GET tempkey
    run_test "TTL of expired key" "-2" "exact" TTL tempkey
}

persistence_commands() {
    run_test "SET persistent key" "OK" "exact" SET pkey "persistent"
    run_test "SAVE" "OK" "exact" SAVE
    run_test "FLUSHALL" "OK" "exact" FLUSHALL
    run_test "GET after flush" "" "exact" GET pkey
    echo -e "\n${BLUE}--- Restarting server to test persistence ---${NC}"
    echo -e "\n> *Restarting server to test persistence...*" >> $REPORT_FILE
    stop_server
    start_server
    run_test "GET persistent key after restart" "persistent" "exact" GET pkey
    run_test "Final FLUSHALL" "OK" "exact" FLUSHALL
    run_test "Final SAVE" "OK" "exact" SAVE
}

# --- Main Script Execution ---
trap stop_server EXIT

echo "# Go Redis Test Report" > $REPORT_FILE
echo "Generated on: $(date)" >> $REPORT_FILE

echo -e "${BLUE}--- Building Project ---${NC}"
go build -o $BINARY_PATH $MAIN_PACKAGE_PATH
if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed. Aborting tests.${NC}"
    exit 1
fi
echo -e "${GREEN}Build successful.${NC}"

start_server

# Run all test groups
run_test_group "Server Commands" server_commands
run_test_group "String Commands" string_commands
run_test_group "List Commands" list_commands
run_test_group "Hash Commands" hash_commands
run_test_group "Set Commands" set_commands
run_test_group "Sorted Set Commands" sorted_set_commands
run_test_group "Expiration Commands" expiration_commands
run_test_group "Persistence Commands" persistence_commands

echo -e "\n${GREEN}--- All tests complete. Report generated at $REPORT_FILE ---${NC}"