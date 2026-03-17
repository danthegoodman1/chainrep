# Architecture

This repository currently has two main subsystems:

- `coordinator`
- `storage`

They are intentionally separated so placement and reconfiguration logic stay independent from replica execution and networking.

## Roles

### Coordinator

The coordinator is the control-plane planner.

It is responsible for:

- initial chain placement
- deterministic reconfiguration planning
- tracking desired chain membership and replica ordering
- deciding when replicas should join, become active, leave, or be removed

It is not responsible for:

- copying data between replicas
- serving client requests
- executing chain replication I/O

The key packages are:

- `coordinator`: pure planning and state transitions
- `coordinator/runtime`: optional durable wrapper with WAL, checkpoints, replay, and idempotent command handling
- `coordserver`: synchronous coordinator service that dispatches storage-node commands, records heartbeats, and exposes routing snapshots to clients

### Storage

The storage subsystem is the execution side.

It is responsible for:

- hosting many slot replicas on one physical node
- creating and deleting local replicas
- fetching and installing snapshots for joining replicas
- executing steady-state chain replication writes between active replicas
- serving direct client requests when the local replica is the correct active head or tail
- tracking local replica lifecycle state
- reporting readiness, removal, and heartbeat information back to the coordinator

It is not responsible for:

- deciding global placement
- deciding which chain should change next

## Boundary Between Them

The coordinator computes desired changes.

The storage node executes those changes.

The intended connection is coordinator-driven:

1. coordinator/runtime accepts a durable command
2. coordinator computes the next safe chain step
3. some coordinator server layer turns that step into storage-node commands
4. storage nodes execute the commands locally
5. storage nodes report progress back to the coordinator
6. coordinator applies that progress and plans the next safe step
7. clients periodically fetch routing snapshots from the coordinator server and talk directly to storage nodes

## Important Interfaces

### In `storage`

`Backend`

- local per-node storage backend
- owns per-slot replica data
- current implementation is in-memory KV
- later this can be replaced with a durable backend without changing node lifecycle logic

Methods:

- `CreateReplica`
- `DeleteReplica`
- `Snapshot`
- `InstallSnapshot`
- `SetHighestCommittedSequence`
- `StagePut`
- `StageDelete`
- `CommitSequence`
- `GetCommitted`

`CoordinatorClient`

- control-plane callback interface from a storage node back to the coordinator side

Methods:

- `ReportReplicaReady`
- `ReportReplicaRemoved`
- `ReportNodeHeartbeat`

`ReplicationTransport`

- node-to-node data-plane interface used for catch-up and steady-state replication

Methods:

- `FetchSnapshot`
- `FetchCommittedSequence`
- `ForwardWrite`
- `CommitWrite`

### In `coordserver`

`RoutingSnapshot`

- read-only snapshot of current slot routes for clients
- exposes per-slot chain version plus the current active head and tail
- marks a slot non-writable while a `joining` replica exists, because the current implementation does not stream live writes into catching-up replicas

### In `client`

`Router`

- caches a coordinator routing snapshot
- hashes keys into dense logical slots
- routes writes to the active head and reads to the active tail
- refreshes once on a typed stale-routing error from storage and retries once

## Current Storage Node Model

The public storage abstraction is a physical node service, not a single replica process.

One `storage.Node` owns:

- one physical node identity
- one backend
- one coordinator client
- one replication transport
- many hosted slot replicas

Each hosted replica has:

- a slot
- chain metadata
- predecessor/successor information
- a local lifecycle state

Current replica lifecycle states:

- `pending`
- `catching_up`
- `active`
- `leaving`
- `removed`

## Current Integration Flow

### Tail add / join

1. coordinator decides a node should join a chain as a new tail
2. coordinator-facing server sends `AddReplicaAsTail` to that storage node
3. storage node creates the local replica
4. storage node fetches a snapshot from its predecessor through `ReplicationTransport`
5. storage node installs the snapshot into `Backend`
6. storage node enters `catching_up`
7. coordinator later sends `ActivateReplica`
8. storage node marks it active and calls `ReportReplicaReady`

### Graceful drain / removal

1. coordinator gets a replacement tail active
2. coordinator sends `MarkReplicaLeaving` to the old replica owner
3. coordinator later sends `RemoveReplica`
4. storage node deletes local data and calls `ReportReplicaRemoved`

### Heartbeats

Storage nodes can summarize local lifecycle state through `ReportNodeHeartbeat`.

This is intended for future coordinator service integration and liveness monitoring.

### Client routing

1. a client fetches a `RoutingSnapshot` from `coordserver`
2. the client hashes the key to a logical slot
3. writes go to the active head for that slot and reads go to the active tail
4. storage nodes reject stale slot ownership, stale chain versions, wrong roles, and inactive replicas with a typed routing-mismatch error
5. the client refreshes the routing snapshot once and retries once

Writes are intentionally blocked while a slot contains a `joining` replica. Reads continue from the active tail during that phase.

## What Is Still Missing

The code now has the core interfaces and in-memory implementations, but it does not yet have:

- a real network transport
- a durable storage backend
- storage restart/recovery semantics
- heartbeat-driven liveness and automatic dead-node actions

So the current shape is:

- coordinator decides
- storage executes
- clients route using coordinator snapshots
- interfaces sit between them so tests can run entirely in memory and production transports can be added later
