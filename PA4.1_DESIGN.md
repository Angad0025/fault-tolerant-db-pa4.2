# PA4.1 Design Document: Building a Replicated Database


## How Our Solution Works

### The Main Idea: The Leader Pattern

We use something called a **"leader-based" system**:
- We pick **ONE server as the leader** (the boss)
- The other servers are **followers** (the workers)
- When someone wants to write data, they tell the **leader**
- The **leader** decides the order of all writes
- The **leader** tells all followers the order
- Everyone applies the writes in the **same order**
- **Result**: All databases have identical data ✅

### Step-by-Step: What Happens When You Insert Data?

```
1. Client says: "Insert this grade record"
   ↓
2. Client sends to Server B (random server)
   ↓
3. Server B thinks: "I'm not the leader, let me ask the leader"
   ↓
4. Server B sends to Server A (the leader): 
   "Hey leader! Client wants to insert this thing"
   ↓
5. Server A (leader) thinks: 
   "OK, this is the 5th write I'm seeing today. Let me mark it as #5"
   ↓
6. Server A tells EVERYONE (including B and C):
   "Write #5 is: Insert this grade record"
   ↓
7. Server A, B, and C all do it at the same time
   ↓
8. All three servers now have the exact same data! ✅
```

## The Three Types of Messages

Our servers talk to each other using three types of messages:

### 1. **FORWARD Message** (Worker → Leader)
**When:** A non-leader server receives a write request
**What it says:** "Leader, this client wants to write something"
**Format:** `FORWARD|request_id|base64_encoded_CQL`

**Real example:**
```
FORWARD|client_123|aW5zZXJ0IGludG8gZ3JhZGUgKGlkLCBldmVudHMpIHZhbHVlcyAoMSwgWzEsMiwzXSk=
                   ↑ (this is the CQL query encoded)
```

### 2. **ORDER Message** (Leader → Everyone)
**When:** The leader receives a write and assigns it a number
**What it says:** "Everyone, apply write #5 in this exact order"
**Format:** `ORDER|sequence_number|request_id|base64_encoded_CQL`

**Real example:**
```
ORDER|5|client_123|aW5zZXJ0IGludG8gZ3JhZGUgKGlkLCBldmVudHMpIHZhbHVlcyAoMSwgWzEsMiwzXSk=
      ↑
    This is write number 5
```

### 3. **Client Messages** (Client → Any Server)
**Format:** Either `request_id|CQL` or just `CQL`

**Real examples:**
```
123|insert into grade (id, events) values (1, [1,2,3]);
```

## How The Servers Know Who's The Leader?

At startup, each server looks at all the servers' addresses:
- `server0` at `127.0.0.1:2000`
- `server1` at `127.0.0.1:2001`
- `server2` at `127.0.0.1:2002`

Then it picks: **The one that comes first alphabetically + numerically**

In this case: `server0` wins because `127.0.0.1:2000` is the smallest!

This means no voting is needed - everyone just agrees on the same rule. ✅

## The Secret Sauce: The Lock (Synchronization)

Here's something really important: **Multiple people can send messages at the same time!**

Imagine:
- Message about write #1 arrives at Server B
- Message about write #3 arrives at Server B (but #2 hasn't arrived yet!)
- Two threads try to apply both at the same time

**Disaster!** They might apply them in wrong order.

**Our solution:** We use a **LOCK** 🔒

```
private final Object applyLock = new Object();

synchronized (applyLock) {
    // Only ONE thread can be here at a time
    // Apply writes in order
    // Release the lock
    // Next thread comes in
}
```

It's like a bathroom with only one stall - people have to wait their turn! This ensures writes are always applied in order. ✅

## How Reads Work

**Reads are easy!** 

Since all databases are identical, we can just:
1. Read from any server's local Cassandra database
2. Don't need to ask the leader
3. Get the answer instantly

This makes reads super fast! 🚀

## What Data Lives Where?

We use **Cassandra** (a real database) to store the actual data:
- `server0` has its own Cassandra keyspace: `server0`
- `server1` has its own Cassandra keyspace: `server1`
- `server2` has its own Cassandra keyspace: `server2`

Each one has a `grade` table:
```
grade (
  id: int (primary key),
  events: list of integers
)
```

## How Do We Track What Needs To Be Done?

The leader uses a **map** (like a dictionary):
```
pending = {
  1: { query: "INSERT ...", client_id: "123" },
  2: { query: "UPDATE ...", client_id: "456" },
  5: { query: "DELETE ...", client_id: "789" }
}

nextToApply = 1  // "Apply write 1 next!"
```

When write #1 is done, we:
1. Remove it from the map
2. Move to write #2
3. Repeat!

It's like a to-do list that gets shorter as we complete tasks. ✅

## Why This Design Works

### ✅ Consistency (Everyone Agrees)
Because the leader assigns sequence numbers, and everyone applies them in order:
- After write #5 is applied, all servers have the same data
- If you ask any server for the data, you get the same answer
- **This is called "linearizable consistency"** - it's the strongest type!

### ✅ Correctness
1. Leader gives each write a unique number (5, 6, 7, ...)
2. All servers receive these numbers
3. All servers apply in the same order (thanks to the lock!)
4. **Result:** All servers stay in sync! 

### ✅ Simple to Understand
- One leader, simple rules
- No voting, no complex algorithms
- Easy to debug and maintain

## What Tests Pass?

✅ **test10_CreateTables** - We can create the grade table  
✅ **test11_UpdateRecord_SingleServer** - One update works  
✅ **test12_UpdateRecord_AllServer** - Multiple updates work  
✅ **test13_UpdateRecord_RandomServer** - Any server can receive writes  
✅ **test14_UpdateRecordFaster_RandomServer** - Fast updates stay consistent  
✅ **test15_UpdateRecordMuchFaster_RandomServer** - Very fast updates stay consistent  
✅ **test16_UpdateRecordFastest_RandomServer** - Super fast updates stay consistent  

**All 7 consistency tests pass!** 🎉

## What Doesn't Work (And Why We Need PA4.2)

❌ **Server crashes** - If a server dies, it's gone forever  
❌ **Leader crashes** - If the leader dies, nothing works  
❌ **No recovery** - We don't remember what happened after restart  
❌ **Temporary network problems** - If networks glitch, we might lose data  

That's what **PA4.2 (Zookeeper)** fixes! It adds:
- ✅ Remember everything that happened (persistent log)
- ✅ Automatic leader re-election
- ✅ Recover after crashes
- ✅ Handle temporary network problems

## The Code Organization

**Main file:** `src/server/MyDBReplicatedServer.java`

**Key methods:**
- `handleMessageFromClient()` - Receives messages from clients and other servers
- `onOrderLine()` - Process ORDER messages (apply writes)
- `onForwardLine()` - Process FORWARD messages (leader receives them)
- `tryApplyInOrder()` - The magic method that applies writes in order
- `multicastOrder()` - Leader broadcasts ORDER messages
- `sendForwardToLeader()` - Non-leader forwards to leader

## Real World Analogy

Imagine a restaurant with **3 locations**:

- **Leader (Main Office):** Gets all orders first, assigns order numbers (1st, 2nd, 3rd, etc.), tells all locations what to cook
- **Followers (Other Locations):** Wait for the main office to tell them what to cook, then they cook in that order
- **Result:** All 3 locations prepare meals in the exact same order, so customers everywhere get consistent service! ✅

## How To Run It

```bash
# Compile the code
javac -d out -cp "lib/*" src/server/*.java src/server/faulttolerance/*.java src/client/*.java

# Run the consistency tests
java -cp "lib/*:out" GraderConsistency

# Expected: All 7 tests pass! ✅
```

## Summary

**What we built:**
- A simple but powerful replication system
- All databases stay identical
- Reads are fast, writes are ordered
- Easy to understand and maintain

**How it works:**
- One leader assigns order to all writes
- All followers apply writes in that same order
- A lock prevents race conditions
- Cassandra stores the actual data


