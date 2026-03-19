# Coordinator HA Store

This document describes the shared coordinator HA store used by the active/standby
coordinator mode.

It is intended for developers implementing a new HA store backend and for reviewers
who want to understand the safety boundary of coordinator failover.

## Purpose

The HA store is the primary safety boundary for coordinator failover.

It owns, atomically:

- the current coordinator materialized state
- the current active lease holder
- the current monotonic coordinator `epoch`
- pending coordinator progress expectations
- the durable outbox of storage-node control commands

The key safety rule is:

- a coordinator may only persist authoritative state changes while it still holds the
  current lease for the current epoch

Storage-node epoch rejection is a second line of defense, not the primary one.

## Why This Exists

Without a shared epoch-gated store, an old coordinator instance can continue
creating authoritative work after a new coordinator has already taken over.

The HA store prevents that by requiring state changes and outbox updates to be
conditional on the current lease epoch.

That means:

- stale leaders cannot commit new coordinator state
- stale leaders cannot durably create new storage-node control work
- a new leader can resume pending outbox work after failover

## Contract

The interface lives in [coordserver/hastore.go](/Users/dangoodman/code/chainrep/coordserver/hastore.go).

The core interface is:

```go
type HAStore interface {
    CurrentLease(ctx context.Context) (LeaderLease, bool, error)
    AcquireOrRenew(ctx context.Context, holderID string, holderEndpoint string, now time.Time, ttl time.Duration) (LeaderLease, bool, error)
    LoadSnapshot(ctx context.Context) (HASnapshot, error)
    SaveSnapshot(ctx context.Context, lease LeaderLease, now time.Time, expectedSnapshotVersion uint64, snapshot HASnapshot) (uint64, error)
    Close() error
}
```

### `AcquireOrRenew`

Required semantics:

- exactly one holder may be leader for the current unexpired lease
- the first successful acquisition returns `epoch = 1`
- takeover after expiry increments the epoch exactly once
- renew by the current holder keeps the same epoch
- if another holder currently owns an unexpired lease, the caller must not become leader

### `LoadSnapshot`

Required semantics:

- returns the current durable coordinator HA snapshot
- if no snapshot exists yet, it must behave like an empty initialized snapshot
- returned data must be safe for callers to mutate without corrupting store-owned state

### `SaveSnapshot`

Required semantics:

- succeeds only if the supplied `lease` is still the current active lease
- fails with `ErrNotLeader` if the caller no longer holds the active epoch
- fails with `ErrHASnapshotConflict` if `expectedSnapshotVersion` is stale
- persists the entire new snapshot atomically
- returns the new incremented snapshot version on success

This is the critical write path. If this operation is weak, coordinator HA is weak.

## Data Model

The persisted snapshot type is `coordserver.HASnapshot`.

It includes:

- `State`: the materialized coordinator runtime state
- `Pending`: expected replica progress still waiting to arrive
- `LastPolicy`: current reconfiguration policy
- `UnavailableReplicas`: replicas currently considered unavailable during repair
- `LastRecoveryReports`: last known recovered-node report state
- `Outbox`: durable storage-node control commands not yet fully acknowledged

The lease is tracked separately as `coordserver.LeaderLease`:

- `HolderID`
- `HolderEndpoint`
- `Epoch`
- `ExpiresAtUnixNano`

## Implementation Guidance

An HA store backend should be implemented around strong semantics, not around the
conveniences of a particular database.

What you need:

- exclusive lease acquisition/renewal
- monotonic epoch increment on takeover
- atomic snapshot persistence
- compare-and-swap style snapshot version checking

### Good Backend Shapes

- Postgres or MySQL transactionally managing:
  - a singleton lease row
  - a singleton snapshot row
- etcd or another KV with:
  - linearizable lease ownership
  - conditional writes
  - durable compare-and-swap state updates

### Bad Backend Shapes

- anything where lease ownership is only eventually consistent
- anything where epoch increments are not strictly monotonic
- anything where snapshot save cannot be conditioned on current leadership

### Practical Rules

1. `AcquireOrRenew` and `SaveSnapshot` must observe the same notion of current
   leadership.
2. `SaveSnapshot` must reject writes from stale leaders even if the process is still
   running.
3. Returned snapshots and leases should be cloned or otherwise isolated from caller
   mutation.
4. Empty snapshots should be normalized so callers do not have to deal with nil map
   hazards.

## Current Backends

### In-Memory

The deterministic reference backend is in
[coordserver/hastore_inmemory.go](/Users/dangoodman/code/chainrep/coordserver/hastore_inmemory.go).

Use it for:

- unit tests
- deterministic failover logic testing
- understanding the minimal required semantics

### Postgres via `pgx`

The production-oriented backend is in
[coordserver/hastore_postgres.go](/Users/dangoodman/code/chainrep/coordserver/hastore_postgres.go).

It uses:

- `database/sql`
- `pgx` stdlib adapter
- a singleton lease row
- a singleton snapshot row with JSON payload
- transactional `SELECT ... FOR UPDATE` coordination

It is intentionally narrow and semantic-first.

## Conformance Testing

The reusable conformance suite is in
[coordserver/hastoretest/suite.go](/Users/dangoodman/code/chainrep/coordserver/hastoretest/suite.go).

Every HA store backend should pass this suite before being trusted.

The suite currently checks:

- initial leader acquisition
- same-holder lease renewal without epoch change
- single-winner behavior while a lease is active
- epoch increment on takeover after expiry
- steady-state snapshot save/load behavior
- rejection of stale-leader snapshot writes after takeover
- outbox continuity across takeover

### How To Add a Backend to the Conformance Suite

Add a package test that calls `hastoretest.Run(...)`.

Example shape:

```go
func TestMyHAStoreConformance(t *testing.T) {
    hastoretest.Run(t, func(t *testing.T) coordserver.HAStore {
        t.Helper()
        store := openMyStoreForTest(t)
        return store
    })
}
```

If the backend needs external infrastructure, env-gate it the same way the Postgres
test does.

See:

- [coordserver/hastore_inmemory_test.go](/Users/dangoodman/code/chainrep/coordserver/hastore_inmemory_test.go)
- [coordserver/hastore_postgres_test.go](/Users/dangoodman/code/chainrep/coordserver/hastore_postgres_test.go)

## Testing a New Backend

Minimum bar:

1. pass the HA-store conformance suite
2. run `go test ./coordserver ./transport/grpcx`
3. verify coordinator failover still works with gRPC clients

Recommended additional checks:

- force lease expiry and ensure a stale leader cannot save
- ensure outbox entries survive process restart
- verify repeated takeover histories converge deterministically

## Relationship to Storage Epoch Enforcement

Storage nodes persist the highest accepted coordinator epoch and reject regressing
control commands.

That is important, but it is not enough by itself.

The HA store is still the main correctness boundary because:

- a storage node may not have seen the new epoch yet
- a stale leader must be prevented from durably creating new work before that point

So the intended defense model is:

1. shared HA store blocks stale-leader state mutation
2. storage nodes reject regressing epochs

Both matter, but the store is the primary one.
