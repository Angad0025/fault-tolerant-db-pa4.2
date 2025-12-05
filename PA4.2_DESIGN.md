# PA4.2 Design Document: Building a Fault-Tolerant Replicated Database with Zookeeper

## What is PA4.2?

PA4.2 extends PA4.1 by adding **fault tolerance** - the ability to survive server crashes and network failures. Using Apache Zookeeper as a coordination service, we build a system that:

- ✅ **Survives crashes** - If a server dies, its data is recovered from the log
- ✅ **Survives leader death** - If the leader dies, a new leader is automatically elected
- ✅ **Persists all writes** - Every write is logged before being applied
- ✅ **Maintains consistency** - All servers stay synchronized even across failures

---

## The Problem PA4.1 Had

From PA4.1, our leader-based replication system was simple but **fragile**:

```
❌ Server crashes → Data lost forever
❌ Leader crashes → No more writes can be ordered
❌ Network split → Inconsistent state across servers
❌ No recovery → Starting from scratch after restart
```

---

## How PA4.2 Fixes It: The Zookeeper Approach

### **The Big Picture**

```
┌──────────────────────────────────────────┐
│   APACHE ZOOKEEPER (localhost:2181)      │
│                                          │
│  - Persistent write log                 │
│  - Leader election                      │
│  - Server health monitoring             │
│  - Crash recovery                       │
└──────────────────────────────────────────┘
         ↑             ↑             ↑
      server0      server1      server2
   (Cassandra)   (Cassandra)   (Cassandra)
```

---

## Three Core Components

### **1. Persistent Write Log (Durability)**

**What it does:**
- Every write is logged to Zookeeper BEFORE being applied
- If a server crashes, it can replay all logged writes to recover

**How it works:**

```
Client: "Insert grade 123"
   ↓
Server (Leader): "Let me log this first"
   ↓
Zookeeper: "Write logged! You're #5"
   ↓
Zookeeper stores: /mydb/log/5 = "insert into grade..."
   ↓
Server applies write to Cassandra
   ↓
All servers see write #5 in log
   ↓
All servers apply it in order
   ↓
Result: Durable + consistent! ✅
```

**Key feature:** Writes are **atomic** - either logged+applied or not applied at all. No half-way states.

---

### **2. Automatic Leader Election (Liveness)**

**What it does:**
- At startup, servers negotiate who should be the leader
- If the current leader crashes, a new one is automatically elected
- **No manual intervention needed!**

**How it works:**

```
Step 1: Startup
  - Server0, Server1, Server2 all come online
  - Each registers itself in Zookeeper: /mydb/servers/server0, etc.
  - They compare names and pick: server0 (lexicographically smallest)
  - server0 becomes LEADER, others become FOLLOWERS

Step 2: Normal operation
  - Leader sequences all writes
  - Followers apply writes in order

Step 3: Leader crashes
  - Zookeeper detects server0 is dead (ephemeral node disappears)
  - Remaining servers: server1, server2
  - They re-elect: server1 becomes new LEADER
  - Writing resumes!
```

**Benefit:** **Zero downtime** - system recovers automatically in seconds!

---

### **3. Crash Recovery (Availability)**

**What it does:**
- When a crashed server restarts, it automatically catches up
- Replays all writes from the persistent log
- Rejoins the cluster as if nothing happened

**How it works:**

```
Timeline:
  12:00 - server1 is running, write #1-100 applied
  12:05 - server1 CRASHES ☠️
  12:10 - server2 continues, write #101-150 applied
  12:15 - server1 RESTARTS

server1's recovery:
  1. Read checkpoint: "Last recovered to write #100"
  2. Read all logs since #101
  3. Apply writes #101-150 in order
  4. Reconnect to cluster
  5. Done! Back in sync! ✅
```

**Checkpointing optimization:**
- To speed up recovery, we save checkpoints every 100 writes
- Instead of replaying all 1000 writes, just replay last 50
- Faster startup = less downtime

---

## The Architecture

### **Zookeeper Paths (The Filing Cabinet)**

```
/mydb                    (root directory)
├── /servers/            (online server registry)
│   ├── server0         (ephemeral - disappears if server dies)
│   ├── server1
│   └── server2
├── /log/                (persistent write log)
│   ├── 1 = "insert into grade (id, events) values (1, [1,2,3]);"
│   ├── 2 = "update grade set events = [1,2] where id = 1;"
│   ├── 3 = "delete from grade where id = 2;"
│   └── ...
├── /checkpoint/         (recovery checkpoints)
│   ├── server0 = 1000   (server0 recovered up to write #1000)
│   ├── server1 = 950    (server1 recovered up to write #950)
│   └── server2 = 1000
└── /leader              (ephemeral leader marker)
```

---

## Message Flow: How Writes Work

### **Scenario: Client sends write, all servers apply it**

```
                    ZOOKEEPER
                   (localhost:2181)
                         △
                         │
                    logs & coordinates

CLIENT              LEADER (server0)           FOLLOWER (server1)    FOLLOWER (server2)
  │                      │                          │                     │
  │─ write ─────────────→ │                          │                     │
  │                       │                          │                     │
  │      1. Assign seq#5  │                          │                     │
  │      2. Log to ZK: /mydb/log/5 = "insert..."    │                     │
  │      ───────────────→ │                          │                     │
  │                       │                          │                     │
  │                       │ 3. Broadcast to followers                       │
  │                       ├─ "apply write #5" ─────→ │                     │
  │                       ├─ "apply write #5" ──────────────────────────→  │
  │                       │                          │                     │
  │      4. Apply to cassandra                       │                     │
  │      ─────────────────→ cassandra               cassandra            cassandra
  │                       │                          │                     │
  │                       │ 5. Acknowledge           │ 6. Apply          7. Apply
  │ ← response ─────────── │ ← done ─────────────── ← ← done ──────────── ←
  │                       │                          │                     │
  
Result: Write #5 successfully applied to all 3 databases! ✅
```

---

## How Crash Recovery Works

### **Scenario: Server crashes and restarts**

```
Timeline:
─────────────────────────────────────────────────────────

12:00  Write #1-100 applied on all servers

12:05  server1 CRASHES (but server0, server2 continue)

12:10  Write #101-150 applied on server0 and server2
       (server1 can't participate - it's dead)

12:15  server1 RESTARTS

server1's recovery process:
  1. Read checkpoint from Zookeeper:
     "server1 last recovered to write #100"
  
  2. List all logs after #100:
     /mydb/log/101
     /mydb/log/102
     ...
     /mydb/log/150
  
  3. Read each log and apply:
     apply("update grade...")
     apply("insert into grade...")
     ...
  
  4. Update checkpoint:
     /mydb/checkpoint/server1 = 150
  
  5. Rejoin as FOLLOWER (or become LEADER if needed)
  
  6. Continue accepting writes

Result: server1 is back in sync! ✅
```

---

## Why This Design Works

### ✅ **Durability (All writes are persistent)**
- Write logged to Zookeeper BEFORE applied to Cassandra
- Even if all servers crash immediately after, write is safe
- Recovery reads log and reapplies

### ✅ **Consistency (Everyone agrees)**
- All writes have sequence numbers from leader
- All servers apply in same order
- No two servers disagree on state

### ✅ **Availability (System keeps running)**
- If leader dies → automatic new leader elected
- If follower dies → joins back when it restarts
- If network splits → Zookeeper ensures only one partition leads

### ✅ **Partition Tolerance (Network failures)**
- Zookeeper handles network partitions
- Prevents split-brain (two leaders)
- Ensures consistency over availability

---

## Key Optimizations

### **1. Checkpointing**
- Save snapshots every 100 writes
- Don't replay entire log (faster recovery)
- Trade-off: small disk space for much faster startup

### **2. Log Cleanup**
- After checkpoint, old logs can be deleted
- Keeps log size bounded (MAX_LOG_SIZE = 400)
- Prevents disk from filling up

### **3. Batching**
- Multiple writes batched together when possible
- Reduces Zookeeper round-trips
- Better throughput

---

## Test Scenarios PA4.2 Handles

✅ **test31_GracefulExecutionSingleRequest**
- Single request completes successfully
- Write logged and applied

✅ **test32_GracefulExecutionMultipleRequestsSingleServer**
- Multiple rapid requests handled correctly
- All logged and applied in order

✅ **test34_SingleServerCrash**
- One server crashes
- Others continue, write to log
- Crashed server recovers and catches up

✅ **test35_TwoServerCrash**
- Two servers crash simultaneously
- Remaining server continues
- Both recover when they restart

✅ **test36_OneServerRecoveryMultipleRequests**
- Server crashes mid-operation
- While down, other servers process writes
- Crashed server recovers and gets all missed writes

✅ **test41_CheckpointRecoveryTest**
- Recovery uses checkpoints for speed
- Large write logs don't slow recovery

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│              MyDBFaultTolerantServerZK                  │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │         Client Message Handler                   │ │
│  │  - Receive writes from clients                   │ │
│  │  - Route to leader or follower logic             │ │
│  └───────────────────────────────────────────────────┘ │
│                         │                               │
│        ┌────────────────┼────────────────┐             │
│        ▼                ▼                ▼             │
│   ┌─────────┐    ┌──────────┐    ┌──────────────┐    │
│   │  Leader │    │ Follower │    │ Zookeeper   │    │
│   │ Logic   │    │  Logic   │    │  Connector  │    │
│   │         │    │          │    │             │    │
│   │ -Order  │    │-Apply in │    │-Read logs   │    │
│   │  writes │    │ order    │    │-Leadership  │    │
│   │-Log to  │    │-Monitor  │    │-Coord       │    │
│   │ ZK      │    │ leader   │    │-Recovery    │    │
│   └─────────┘    └──────────┘    └──────────────┘    │
│        │                │                 │           │
│        └────────────────┼─────────────────┘           │
│                         ▼                             │
│  ┌──────────────────────────────────────────────────┐ │
│  │         Cassandra Interface                      │ │
│  │  - Execute CQL queries                          │ │
│  │  - Maintain persistent database state           │ │
│  │  - Keyspace: {myID} (server0, server1, etc.)    │ │
│  └──────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
         │                               │
         ▼                               ▼
    ┌─────────────────┐          ┌──────────────────┐
    │   Cassandra     │          │  Apache          │
    │                 │          │  Zookeeper       │
    │ Database        │          │                  │
    │ (persistence)   │          │ Log + Coord      │
    └─────────────────┘          │ (reliability)    │
                                 └──────────────────┘
```

---

## Summary: What Makes PA4.2 Different from PA4.1

| Feature | PA4.1 | PA4.2 |
|---------|-------|-------|
| **Crash Recovery** | ❌ Data lost | ✅ Recovered from log |
| **Leader Election** | ❌ Manual restart | ✅ Automatic |
| **Persistence** | ❌ In-memory only | ✅ Zookeeper log |
| **Multi-server failure** | ❌ System dies | ✅ Continues if majority online |
| **Downtime after crash** | ❌ Manual intervention | ✅ Automatic recovery |
| **Network partition** | ❌ Split-brain possible | ✅ Prevented by Zookeeper |

---

## How This Maps to Real Systems

**Facebook Memcache + MySQL:**
- Memcache = our servers
- MySQL binlog = our Zookeeper log
- MySQL replication = our crash recovery

**Google Spanner:**
- Multi-region Paxos = our Zookeeper leader election
- Write-ahead logging = our persistent log
- Checkpoint & recovery = our restore mechanism

**Amazon DynamoDB:**
- Quorum reads/writes = our leader-based coordination
- Consistent hashing = our server registry
- Versioning & tombstones = our write logging

---

## Conclusion

PA4.2 transforms the simple PA4.1 leader-based system into a **production-grade fault-tolerant database** by:

1. **Persisting all writes** - Zookeeper log survives crashes
2. **Electing leaders automatically** - No manual intervention
3. **Recovering automatically** - Replay log to restore state
4. **Handling failures** - System continues despite crashes

This is the foundation of **real distributed databases** like Cassandra, HBase, and Spanner!

```
**Key Insight**: Fault tolerance = Durability + Availability + Consistency
                 = Persistent log + Leader election + Crash recovery
```
