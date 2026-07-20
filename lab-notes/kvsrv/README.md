# Lab 2: Key/Value Server and Distributed Lock

This note records the implementation approach, pitfalls encountered, and key
lessons from building a versioned key/value server and a distributed lock on
top of it.

## Lab Goals

Lab 2 is split into four tasks:

1. **KV Server with version-based CAS** — implement a single-server key/value
   store where each key carries a version number and writes are conditional on
   the version matching (compare-and-swap).
2. **Clerk (RPC client)** — implement a client that talks to the KV server via
   RPC, with correct retry semantics for unreliable networks.
3. **At-most-once retry semantics** — handle the ambiguity when an RPC reply is
   lost: distinguish between "definitely not executed" and "might have been
   executed" (the `ErrMaybe` error).
4. **Distributed lock** — build a distributed lock using only `Get` and `Put`
   on the KV server, handling network failures and ownership verification.

---

## Implementation Approach

### 1. KV Server: version-based optimistic concurrency control

The server wraps a simple `map[string]*Value` behind a mutex. Each key stores
both a value and a version number:

```go
type Value struct {
    value   string
    version rpc.Tversion
}
```

**Get** returns the current value and version, or `ErrNoKey` if the key does
not exist.

**Put** implements a compare-and-swap:
- If the key does not exist, it is created only when `args.Version == 0`.
- If the key exists, the write succeeds only when `args.Version` matches the
  server's current version. On success, the version is incremented.
- If versions don't match, `ErrVersion` is returned.

The mutex ensures each `Get`/`Put` is atomic, but it does **not** make a
sequence of `Get → Put` atomic across two RPC calls — that's what the
version-based CAS is for.

### 2. Clerk: RPC client with infinite retry

**Get** — idempotent, so it retries forever on network failure:

```go
for {
    ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
    if ok {
        return reply.Value, reply.Version, reply.Err
    }
}
```

**Put** — not idempotent, so the retry logic must distinguish between:
- Network failure (request may or may not have reached the server) → retry
- `ErrVersion` on first attempt → definitely not executed → return `ErrVersion`
- `ErrVersion` on retry → ambiguous → return `ErrMaybe`

The `resend` flag tracks whether we are in a retry:

```go
resend := false
for {
    ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)
    if ok {
        if reply.Err == rpc.ErrVersion && resend {
            return rpc.ErrMaybe
        }
        return reply.Err
    }
    resend = true
}
```

### 3. ErrMaybe: the honest ambiguity

`ErrMaybe` is returned in exactly ONE scenario:

> The first RPC succeeded (the server executed the Put), but the reply was
> lost. The client retried with the same version, and the server rejected it
> with `ErrVersion`.

The client cannot tell this apart from "the first RPC never reached the
server and someone else changed the version." Both cases produce `ErrVersion`
on retry. `ErrMaybe` communicates this ambiguity to the application layer.

| First RPC | Retry RPC | Return |
|-----------|-----------|--------|
| OK (reply lost) | ErrVersion | **ErrMaybe** |
| Network failure (never reached) | OK | OK |
| Network failure (never reached) | ErrNoKey | ErrNoKey |
| Direct success | — | OK |
| First-attempt ErrVersion | — | ErrVersion |

### 4. Distributed lock with unique ownership

The lock uses the KV server as shared state. Each `MakeLock` call generates a
random unique ID (via `kvtest.RandValue`), and the lock writes this ID instead
of a generic "locked" string.

**Acquire:**
- Get the lock key. If it doesn't exist, `Put(myID, version=0)`.
- If it exists and is empty, CAS with `Put(myID, currentVersion)`.
- On `ErrMaybe`, verify ownership by calling `Get` and checking `val == myID`.
- If the lock is held by someone else, sleep and retry.

**Release:**
- Get the current version, then `Put("", currentVersion)`.
- **Only release if `val == myID`** — this prevents a stale Release from
  freeing a lock held by another client (analogous to double-free in C).
- Retry on network failure; treat `ErrMaybe` as success.

---

## Key Data Structures

```
KVServer
├── mu      sync.Mutex
└── data    map[string]*Value
                ├── value   string
                └── version rpc.Tversion

Clerk
├── clnt    *tester.Clnt    (RPC client)
└── server  string           (server address)

Lock
├── ck       kvtest.IKVClerk
├── lockname string           (KV key used for this lock)
└── id       string            (unique owner identifier)
```

```
Architecture:

┌──────────┐   ┌──────────┐   ┌──────────┐
│ Client A │   │ Client B │   │ Client C │
│          │   │          │   │          │
│  Clerk   │   │  Clerk   │   │  Clerk   │
│  Lock    │   │  Lock    │   │  Lock    │
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │              │              │
     │   Put/Get    │   Put/Get    │
     └──────────────┼──────────────┘
                    ▼
            ┌──────────────┐
            │  KV Server   │
            │              │
            │  mu          │
            │  data map    │
            └──────────────┘
```

---

## Main Takeaways

- **Version-based CAS is the minimal primitive** needed to turn a key/value
  store into a coordination service. It enables optimistic locking, which in
  turn enables distributed locks, leader election, and barriers.

- **ErrMaybe is not a bug — it's a feature.** It honestly communicates the
  inherent ambiguity of unreliable networks. Any distributed system must
  confront this ambiguity somewhere in its stack.

- **A distributed lock on top of KV** is the same pattern used by etcd,
  ZooKeeper, and Chubby. The KV store provides the shared truth; the lock
  is an application-level construct built with CAS and unique ownership IDs.

- **Ownership verification is essential** in both Acquire (after `ErrMaybe`)
  and Release (before freeing). Without it, a stale retry can corrupt the
  lock state — analogous to a double-free bug in memory management.

- **The single-server KV is not fault-tolerant.** It has no replication.
  Lab 3 (Raft) and Lab 4 (KV Raft) will add replication to make the KV
  service survive server crashes.

## Possible Improvements

- Add lease/timeout to locks so a crashed client doesn't hold the lock
  forever.
- Use exponential backoff instead of fixed sleep in Acquire to reduce
  contention under high load.
- Add a `Delete` operation to the KV server to avoid accumulating empty
  keys from released locks.
