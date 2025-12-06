# PA4.2 Design – Zookeeper-Based Fault-Tolerant Server

## Goal and Approach

The goal of PA4.2 is to make the replicated datastore **fault-tolerant**: servers can crash and restart, and the system should still converge to a consistent state across replicas. [file:33]

For this assignment, the chosen option is the **Zookeeper coordination server** (option 2 in the PA4.2 README). The main idea is:

- Use Zookeeper as a shared, persistent **log of writes** that all replicas can see.  
- Each write is recorded as a znode under a common parent path, e.g., `/mydb/log`.  
- Servers replay this log in order (by znode sequence) to recover state after crashes.  
- This ZK-backed logic is implemented in `MyDBFaultTolerantServerZK` in the `faulttolerance` package. [file:33]

---

## File Organization and Constraints

- All PA4.2-specific logic lives under `src/server/faulttolerance/`. [file:33]  
- The main Zookeeper-based server is:
  - `server.faulttolerance.MyDBFaultTolerantServerZK` – extends `MyDBSingleServer`.  
- A separate class is used for the Gigapaxos option:
  - `server.faulttolerance.MyDBReplicableAppGP` – implements `Replicable` for GP tests. [file:33]

The implementation respects the constraint that shared coordination between servers happens only via Zookeeper (no shared files or other side channels). [file:33]

---

## Core Design: MyDBFaultTolerantServerZK

### Overview

`MyDBFaultTolerantServerZK` combines: [file:33]

- The PA4.1 leader-based pattern (leader election, FORWARD/ORDER).  
- A Zookeeper-backed log:

  - Path root: `/mydb/log`.  
  - Each znode name is the sequence number (`seq`) as a string.  
  - Znode data contains the `ORDER` line: `ORDER|seq|requestId|base64CQL`.

On startup:

- Each server connects to a local Zookeeper at `localhost:2181`.  
- It ensures that `/mydb/log` exists.  
- It calls `recoverFromZkLog()` to replay any existing ORDER entries so that state can be rebuilt after crashes. [file:33]

---

## Leader Election and Messaging

Just like PA4.1: [file:29][file:33]

- Each server uses the `NodeConfig<String>` passed to the constructor to obtain all node IDs.  
- It sorts them and picks the lexicographically smallest as `leaderId`.  
- Servers communicate via a `MessageNIOTransport<String,String>` called `serverMessenger`.  
- Message types:

  - `FORWARD|requestId|base64CQL` – follower to leader.  
  - `ORDER|seq|requestId|base64CQL` – leader to all replicas.

Client messages still look like `requestId|CQL` or just `CQL`, parsed in `handleMessageFromClient`. [file:33]

---

## Zookeeper Log Structure

- Root path: `ZK_LOG_ROOT = "/mydb/log"`.  
- For each ordered write with sequence `seq`, the leader creates:

  - Path: `/mydb/log/<seq>` – znode name is the decimal sequence number.  
  - Data: the full ORDER line (`ORDER|seq|requestId|base64CQL`). [file:33]

This acts as a durable, shared write-ahead log.

### Logging Path (Leader)

In `onClientWriteAtLeader` (for `MyDBFaultTolerantServerZK`): [file:33]

1. Assign `seq = nextSeq.getAndIncrement()`.  
2. Save the write in `pending.put(seq, new Pending(cql, clientAddr, requestId))`.  
3. Build `orderLine = "ORDER|" + seq + "|" + requestId + "|" + base64CQL`.  
4. Create znode:  
   - `zk.create("/mydb/log/" + seq, orderLineBytes, OPEN_ACL_UNSAFE, PERSISTENT)`  
5. Call `multicastOrder(orderLine)` to send the ORDER to all replicas.

If a node already exists for that `seq`, the code ignores the `NodeExistsException`. [file:33]

---

## Recovery from Zookeeper

On startup, `recoverFromZkLog()` does: [file:33]

1. Call `zk.getChildren("/mydb/log", false)` to get all children.  
2. Sort the names by numeric value (`Long.parseLong(name)`).  
3. For each `name`:

   - Parse `seq = Long.parseLong(name)`.  
   - Fetch data: `zk.getData("/mydb/log/" + name, false, null)`.  
   - Convert data to string (`orderLine`) and call `onOrderLine(orderLine)`.  
   - Ensure `nextSeq` is at least `seq + 1` so new writes get higher sequence numbers.

This ensures that after a crash, each replica replays all logged ORDER entries to rebuild state. [file:33]

---

## Ordered Application

For both network ORDER messages and recovery: [file:33]

- `onOrderLine` parses the ORDER line, decodes the CQL, and ensures a `Pending` entry exists for this `seq`.  
- Then it calls `tryApplyInOrder()`.  

The `tryApplyInOrder` method:

- Uses `synchronized(applyLock)` so only one thread applies writes at a time.  
- Starting from `nextToApply`, loops while `pending` contains an entry for that sequence.  
- Conceptually should:

  - Execute the CQL on Cassandra for each replica.  
  - Remove the entry and increment `nextToApply`.  

In the current code, `tryApplyInOrder` only advances `nextToApply` and removes entries but does not yet execute the CQL against Cassandra. This is the main missing piece that causes the `verifyOrderConsistent` failures in the PA4.2 tests. [file:6][file:33]

---

## Handling Client Requests

`handleMessageFromClient` in `MyDBFaultTolerantServerZK`: [file:33]

1. Reads bytes → UTF-8 string.  
2. Parses into `requestId` and `cql` (generating a UUID if no `|` is present).  
3. If this server is the leader:
   - Calls `onClientWriteAtLeader(requestId, cql, header.sndr)`.  
4. Otherwise:
   - Calls `sendForwardToLeader(requestId, cql)`.

This is the same high-level pattern as PA4.1, but the leader additionally logs to Zookeeper before broadcasting ORDER.

---

## MyDBReplicableAppGP (Gigapaxos Option)

For the GigaPaxos-based option, `MyDBReplicableAppGP` implements `Replicable`. [file:33]

Key ideas:

- The constructor takes `args[0]` as the Cassandra keyspace and opens a `Cluster` and `Session` to that keyspace.  
- `execute(Request request, boolean)`:
  - Casts to `RequestPacket`.  
  - Parses any `reqID::query` format but effectively just extracts the CQL string.  
  - Executes the CQL against Cassandra with `session.execute(query)`.  
- `checkpoint(String)`:
  - Reads the entire `grade` table and serializes it into a compact string form (id:events list).  
- `restore(String, String)`:
  - Clears and re-populates the `grade` table using the checkpoint string, then lets GigaPaxos replay the log on top.  

This app is stateless beyond Cassandra, relying on GigaPaxos for replication and on Cassandra for storage. [file:33]

---

## Limitations and Failing Tests

Currently, the PA4.2 implementation is incomplete in two main ways: [file:6][file:32][file:33]

1. **CQL execution in ZK server:**  
   - `MyDBFaultTolerantServerZK.tryApplyInOrder` does not yet execute `p.cql` against Cassandra, so writes are never reflected in the underlying database.  
   - As a result, the consistency checks in `GraderCommonSetup.verifyOrderConsistent` see empty or mismatched state and tests like `test31_GracefulExecutionSingleRequest` fail with `nonEmpty=false`. [file:6][file:32]

2. **Robust exception handling:**  
   - Zookeeper operations may throw `KeeperException` at runtime; while most calls are wrapped in `try/catch`, the error handling is conservative and sometimes leads to IOExceptions that stop tests early. [file:32]

Because of these limitations, the autograder currently reports 0/55 despite the structural design being present.

---

## Summary

- PA4.1 and PA4.2 are implemented in a single repo.  
- PA4.1 uses a leader-based FORWARD/ORDER protocol with a `pending` map and `applyLock` to ensure ordered application of writes. [file:29][file:33]  
- PA4.2 extends that idea with a Zookeeper-based log (`/mydb/log/<seq>` storing ORDER lines) and recovery via `recoverFromZkLog()`. [file:33]  
- The main missing piece is wiring `tryApplyInOrder` to execute CQL against Cassandra and fully handling Zookeeper exceptions, which is why the autograder’s consistency and recovery tests fail even though the high-level design matches the README’s Zookeeper option. [file:6][file:32][file:33]
