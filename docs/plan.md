Awesome—let’s turn your Redis-like server into something you can confidently call “production-ish”. I’ll lay out a **step-by-step implementation plan** with (1) what to build, (2) why, (3) how to design it, (4) concrete “Do this” tasks, (5) gotchas, and (6) tests to write. Follow it in order; each phase is small and shippable.

---

# Phase 0 — Safety Net & Baseline

**Goal:** lock down correctness before adding power.

**Do this**

1. Add a **command registry** with metadata (arity, read/write, key positions). Enforce arity & normalize names centrally.
2. Wrap the TCP conn with **read/write deadlines**.
3. RESP parser: handle **Null Bulk** `$-1`, **Null Array** `*-1`, large bulk strings (streamed read), strict CRLF.
4. Add **table-driven tests** for every command you already support.

**Design sketch**

```go
type CmdSpec struct {
  Name string
  Arity int // -N means >= N
  IsWrite bool
  KeyIdxs []int
  Handler func(*Client, []Resp) Resp
}
var cmd = map[string]CmdSpec{ /* ... */ }

func dispatch(c *Client, arr []Resp) Resp {
  if len(arr)==0 { return RespError("ERR protocol error") }
  name := strings.ToUpper(arr[0].BulkString())
  spec, ok := cmd[name]; if !ok { return RespError("ERR unknown command") }
  if !validArity(spec.Arity, len(arr)) { return RespError("ERR wrong number of arguments") }
  return spec.Handler(c, arr)
}
```

**Gotchas**

* Never panic in parser; return `-ERR`.
* Don’t read full bulk into one giant buffer; stream then copy into final slice.

**Tests**

* Golden tests for RESP encode/decode (byte-exact).
* Wrong-arity tests auto-generated from registry.
* Fuzz `RESP` with invalid bytes (`go test -fuzz=FuzzRESP`).

---

# Phase 1 — Concurrency: Sharded Store

**Goal:** reduce write contention & clarify locking.

**Design**

* **64 shards** (configurable). Each shard owns its maps & its lock.
* For multi-key ops: lock involved shards in **ascending shard index** to avoid deadlocks.

**Do this**

1. Introduce `type shard struct { mu sync.RWMutex; strings map[string]string; lists ... }`
2. `type KV struct { shards []shard }`
3. Helper:

```go
func (kv *KV) shardFor(key string) *shard {
  h := fnv32a(key)
  return &kv.shards[int(h)%len(kv.shards)]
}
```

4. Migrate all single-key commands to `withShard(key, write, fn)`.
5. Implement a tiny **key grouper** for multi-key commands that returns shards sorted & deduped.

**Gotchas**

* Don’t hold multiple shard locks longer than needed.
* For read ops across shards, prefer RLocks; for writes, Lock.

**Tests**

* Race detector: `go test -race`.
* Benchmarks: write vs read throughput pre/post sharding.

---

# Phase 2 — Data Structure Correctness & Complexity

**Goal:** predictable big-O & no hidden O(n) surprises.

**Do this**

1. **Lists**: replace slice head-pops with a **deque**.

   * Simple impl: two slices (`front`, `back`) with amortized O(1) ends.
2. **Sets**: keep `map[string]struct{}`; add SPOP/SRANDMEMBER tests.
3. **ZSets**:

   * If you currently store a sorted slice, document O(n) insert.
   * Upgrade to **dict + skiplist** (classic Redis pattern) if time permits:

     * dict: member → score
     * skiplist: order by (score, member) for range queries.

**Gotchas**

* For ZAdd, define exactly how you treat equal scores & tie-breaks (use lexicographical member).

**Tests**

* Boundary cases (empty, 1 item, duplicates).
* ZRANGE with negative indexes; inclusive/exclusive score ranges.

---

# Phase 3 — Durable Persistence (AOF + Safer Snapshot)

**Goal:** crash safety with good latency.

## 3A) AOF (Append Only File)

**Design**

* Append every **write command** in RESP form to `appendonly.aof`.
* **Fsync policy**: `everysec`. Use a ticker to flush.
* On startup: **replay AOF** into an empty store.

**Do this**

1. Add an `AOFWriter` goroutine with a bounded channel of `[]byte`.
2. On every write command (via registry `IsWrite`), encode the original RESP and enqueue.
3. Ticker every 1s: `Sync()` file; also on graceful shutdown.

**Gotchas**

* Backpressure: if AOF channel is full, either block the command (documented) or disconnect after timeout.
* Use a **single writer goroutine**; no concurrent writes to the file handle.

**Tests**

* Crash during replay (inject kill); ensure up to last fsync is preserved.
* Corrupt tail handling: safely ignore partial last command.

## 3B) Safer BGSAVE Snapshot

**Design**

* Make a **point-in-time** view:

  * Acquire short **global RLock** just to take references to per-shard maps,
  * Then per-shard **deep copy** under each shard’s RLock in a goroutine.
* Serialize to **binary** (faster than JSON) later; keep JSON first for simplicity.

**Do this**

1. `BeginSnapshot()` returns a struct with deep-copied data.
2. Background goroutine writes snapshot to `data.rdb.tmp`, then `rename()` to `data.rdb`.

**Gotchas**

* Never mutate during iteration; all deep-copy happens while holding that shard’s RLock.
* Rename is atomic on the same filesystem.

**Tests**

* Snapshot under heavy write load (soak test).
* Ensure RDB load equals the map size captured, not current live size.

---

# Phase 4 — Key Expiration (TTL) & (Optional) Eviction

**Goal:** Redis-like behavior for TTL.

**Design**

* Store `expiresAt` per key (in the key’s shard).
* **Passive** expiration on key access.
* **Active** expiration: a **min-heap** of (expiresAt, key, shard), tick every 100ms–1s and remove a small batch.

**Do this**

1. Extend value wrapper: `type entry struct { v any; expiresAt int64 /* unix ms or 0 */ }`
2. Implement `EXPIRE`, `TTL`, `PERSIST`, `PEXPIRE`, `PTTL`.
3. Add a per-shard or global min-heap; on tick, pop expired until `now < top.expiresAt`.

**Gotchas**

* Clock source: use `time.Now()` once per tick; avoid excessive syscalls.
* On write to a key, update/clear any existing expiration.

**Tests**

* TTL of 1ms under load.
* `PERSIST` cancels expiration.
* Expired reads return nil; `TTL` returns negative for no TTL.

**(Optional)** **Eviction policy** once you introduce `maxmemory`: approximate LRU/LFU using samples.

---

# Phase 5 — Pub/Sub Hardening

**Goal:** slow subscribers don’t stall the server.

**Design**

* Each subscriber has a **bounded buffered channel** (e.g., 256 messages).
* Policy: on full buffer, either **drop oldest** (ring buffer) or **disconnect**.

**Do this**

1. `subscriber{ ch chan []byte, close func() }`
2. Publisher fans out to subscribers **non-blocking**:

   ```go
   select { case sub.ch <- payload: default: dropOrKick(sub) }
   ```
3. Add **ping/heartbeat**; clean up dead connections.

**Gotchas**

* Ensure goroutines exit when the client disconnects.
* Backpressure metrics (see next phase).

**Tests**

* One slow client, one fast client; fast must not be impacted.

---

# Phase 6 — Observability (Metrics + Logs)

**Goal:** measure, debug, and prove performance.

**Do this**

1. **Prometheus endpoint** `/metrics`:

   * `redis_like_commands_total{cmd}`
   * `redis_like_latency_ms_bucket{cmd,le}`
   * `redis_like_connections`
   * `redis_like_pubsub_queue_depth`
   * `redis_like_aof_queue_depth`
   * `redis_like_memory_bytes` (approx)
2. **Structured logs** (Go `slog` or `zap`):

   * one line per command: `{cmd, keys, duration_ms, err}`
   * lifecycle events: start, snapshot, aof fsync.

**Gotchas**

* Don’t log entire payloads (PII/size); log sizes & counts.

**Tests**

* Unit test metrics registration & increments.
* Manual check with Prometheus/Grafana.

---

# Phase 7 — Performance Pass

**Goal:** know your numbers and regressions.

**Do this**

1. Benchmarks:

   * `SET/GET` single shard vs cross shards
   * `LPUSH/LPOP` ends
   * `ZADD/ZRANGE` on various sizes
2. Flamegraphs:

   * expose `net/http/pprof`; run `go tool pprof`.
3. Tune shard count, buffer sizes, AOF flush interval.

**Gotchas**

* Don’t micro-optimize before measuring; keep changes minimal & justified.

---

# Phase 8 — Developer Experience & Docs

**Goal:** interviewer-ready & contributor-friendly.

**Do this**

1. Add `./scripts/dev.sh` to run server with sensible defaults.
2. `README` sections:

   * Architecture diagram
   * Consistency guarantees (per command)
   * Big-O per structure
   * Persistence model (AOF + RDB) & recovery steps
   * Limitations (e.g., multi-key cross-shard cost)
3. Provide a tiny **example client** and **redis-benchmark** snippet.

---

## Implementation Checklists (copy into TODO)

### ✅ Infra & Safety

* [ ] Command registry + uniform errors
* [ ] Conn read/write deadlines
* [ ] RESP: Nulls, CRLF, large bulks, fuzz

### ✅ Concurrency

* [ ] 64-shard KV, `withShard` helper
* [ ] Multi-key shard lock ordering
* [ ] Race-free iteration patterns

### ✅ Data Structures

* [ ] Deque for lists (O(1) ends)
* [ ] ZSet dict (+ optional skiplist)
* [ ] Clear complexity docs

### ✅ Durability

* [ ] AOF writer goroutine + `everysec` fsync
* [ ] AOF replay w/ tail truncation
* [ ] BGSAVE snapshot (deep copy per shard)
* [ ] Atomic rename

### ✅ TTL/Eviction

* [ ] expiresAt per key
* [ ] Passive + active expiration
* [ ] (Opt) LRU/LFU samples for maxmemory

### ✅ Pub/Sub

* [ ] Bounded subscriber buffers
* [ ] Drop-or-kick policy
* [ ] Heartbeats & cleanup

### ✅ Observability

* [ ] Prometheus metrics
* [ ] Structured logs
* [ ] pprof endpoint

### ✅ Testing/Bench

* [ ] Table-driven cmd tests
* [ ] Golden RESP tests
* [ ] Fuzz parser
* [ ] Benchmarks
* [ ] Soak tests (AOF, BGSAVE, TTL)

---

## “Teach me the code” snippets you’ll reuse

**Shard helper**

```go
func (kv *KV) withShard(key string, write bool, fn func(s *shard) Resp) Resp {
  s := kv.shardFor(key)
  if write { s.mu.Lock(); defer s.mu.Unlock() } else { s.mu.RLock(); defer s.mu.RUnlock() }
  return fn(s)
}
```

**Bounded fan-out (pub/sub)**

```go
for _, sub := range subs {
  select {
  case sub.ch <- msg:
  default:
    // policy: drop oldest
    <-sub.ch
    sub.ch <- msg
  }
}
```

**AOF writer**

```go
type AOF struct {
  f *os.File
  q chan []byte
  done chan struct{}
}

func (a *AOF) Run() {
  ticker := time.NewTicker(time.Second)
  defer ticker.Stop()
  for {
    select {
    case b := <-a.q:
      if _, err := a.f.Write(b); err != nil { /* log & decide policy */ }
    case <-ticker.C:
      _ = a.f.Sync()
    case <-a.done:
      _ = a.f.Sync()
      return
    }
  }
}
```

**Snapshot skeleton**

```go
func (kv *KV) SnapshotTo(path string) error {
  type snapShard struct{ /* deep-copied maps */ }
  snaps := make([]snapShard, len(kv.shards))

  var wg sync.WaitGroup
  for i := range kv.shards {
    i := i
    wg.Add(1)
    go func() {
      defer wg.Done()
      s := &kv.shards[i]
      s.mu.RLock()
      defer s.mu.RUnlock()
      snaps[i] = deepCopyShard(s) // copy maps/slices, not pointers
    }()
  }
  wg.Wait()
  return writeJSONAtomic(path, snaps) // write to .tmp then rename
}
```

**TTL active loop**

```go
func (kv *KV) startExpiryLoop() {
  t := time.NewTicker(200 * time.Millisecond)
  go func() {
    for range t.C {
      now := time.Now().UnixMilli()
      kv.expHeap.PopExpired(now, func(shardIdx int, key string) {
        s := &kv.shards[shardIdx]
        s.mu.Lock()
        defer s.mu.Unlock()
        if ent, ok := s.strings[key]; ok && ent.expiresAt > 0 && ent.expiresAt <= now {
          delete(s.strings, key)
        }
      })
    }
  }()
}
```

---

## Where to start today

1. **Phase 0 + Phase 1** (registry + sharding) — these unlock everything else and usually expose hidden assumptions.
2. Pick **AOF** next if you want “durable”, or **TTL** if you want “feature parity”.

If you paste your current `Server/KV` structs and one handler (e.g., `SET`), I’ll adapt the sharded version and wire the command registry for you, then we’ll proceed to AOF in the next pass.
