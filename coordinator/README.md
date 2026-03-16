# Coordinator

This package builds and evolves chain placement for a fixed set of logical slots.

## Model

The coordinator maintains:

- `slot -> chain`
- `chain -> ordered replicas`
- `replica -> node ID + replica state`
- `node -> failure domains + global health`

The key exported state types are:

- `ClusterState`
- `Chain`
- `Replica`
- `Node`

Replica state is per-chain:

- `joining`
- `active`
- `leaving`

Node health is global:

- `alive`
- `dead`

The current implementation treats only `active` replicas as continuity anchors during reconfiguration.

## Initial Chain Creation

Bootstrap placement is done by `BuildInitialPlacement`.

It requires:

- `SlotCount > 0`
- `ReplicationFactor > 0`
- enough nodes to satisfy the replication factor
- unique node IDs
- valid failure-domain maps

Bootstrap does not allow partial startup. If a full valid placement cannot be built, the call fails.

### Placement algorithm

Chains are built in deterministic slot order from `0..SlotCount-1`.

For each slot, replicas are placed left to right until the replication factor is reached.

For each replica position:

1. Scan every physical node once.
2. Filter to eligible nodes:
   - the node is not already in the chain
   - the node does not conflict with any existing chain member on failure-domain key/value pairs
3. Pick the eligible node with the lexicographically smallest:
   - `(tail_count, head_count, replica_count, node_id)`

The counts are updated dynamically while the full set of chains is being built:

- the first replica in a chain increments head, tail, and replica counts
- appending a later replica removes the prior tail assignment and gives tail to the new last replica
- head remains attached to the first replica in the chain

This spreads roles deterministically across the cluster instead of sorting the full node list for every placement.

### Failure-domain rules

A candidate node is ineligible for a chain if it shares the same value for the same failure-domain key with any node already in that chain.

Example:

- if a chain already contains `rack=r2`, another node with `rack=r2` cannot join that chain
- missing keys do not conflict

## Reconfiguration Overview

Reconfiguration is driven by:

- `PlanReconfiguration`
- `ApplyProgress`

`PlanReconfiguration` accepts an existing `ClusterState`, an ordered list of events, and a `ReconfigurationPolicy`.

The main event types are:

- `add_node`
- `begin_drain_node`
- `mark_node_dead`
- `replica_became_active`
- `replica_removed`

The planner returns a `ReconfigurationPlan` with:

- updated cluster state
- changed slots
- explicit per-slot steps
- unassigned live nodes that could not be used yet

## Reconfiguration Invariants

The current implementation tries to preserve these rules:

- no forced mode
- no full bootstrap until a complete valid placement exists
- dead nodes are never selected as candidates
- draining nodes are not selected as new candidates
- repairs append a new tail
- healthy join rebalance changes at most one member per affected chain in one planning round
- a chain must not lose all `active` replicas during repair
- surviving replicas keep their relative order through staged transitions

If the planner cannot repair a chain without violating those rules, it returns an error.

## Repair And Removal

### Dead node repair

When a node is marked `dead`:

1. It is removed from effective membership.
2. Any dead replicas are dropped from affected chains.
3. If the chain has no `active` replica left, the chain is considered broken and planning fails.
4. The coordinator appends new `joining` tails until the chain returns to the configured replication factor.

This is a mandatory repair path, so it is not limited by `MaxChangedChains`.

### Graceful drain

When a node begins draining:

1. The node is marked as draining globally.
2. For each affected chain, the planner appends a replacement `joining` tail.
3. Once that new replica becomes `active`, the draining replica is converted to `leaving`.
4. A later `replica_removed` event removes the leaving replica from that chain.

This keeps the replacement flow uniform: replacement always enters at the tail, and the old member leaves only after the new one is active.

## Healthy Join Rebalance

Adding a node has two phases:

1. the node is added to live membership
2. the planner tries to move some chains toward the ideal steady-state placement

### Target placement

The planner recomputes the ideal steady-state chains over the current live, non-draining node set using the same scoring function as bootstrap:

- `(tail_count, head_count, replica_count, node_id)`

This means new nodes are not permanently biased toward the tail in the final steady state. They may end up as head, middle, or tail if that is what the target placement requires.

### Safe migration shape

A healthy join rebalance is staged:

1. append the new node as a `joining` tail on a selected chain
2. wait for `replica_became_active`
3. reorder the chain into the new active target shape and mark the displaced old member as `leaving`
4. wait for `replica_removed`

In one planning round, a join-driven chain change is only allowed if the target differs by one member and keeps the survivors in the same relative order. This is the mechanism that avoids fully disjoint remaps.

### Disruption budget

`ReconfigurationPolicy.MaxChangedChains` limits optional join-driven rebalance only.

If adding a node would require changing more chains than the budget allows, the node remains in membership but can be left unassigned for now. Those nodes are returned in `ReconfigurationPlan.UnassignedNodeIDs`.

## Progress Events

`ApplyProgress` is the mechanism for moving staged plans forward.

Supported progress events:

- `replica_became_active`
- `replica_removed`

The coordinator does not assume progress happened automatically. The caller must report it. After progress is applied, `PlanReconfiguration` can be called again to compute the next safe step.

## What This Implementation Does Not Do

The current package is still intentionally narrow. It does not implement:

- request routing
- data transfer / catch-up mechanics
- forceful unsafe reconfiguration
- per-chain progress timestamps or orchestration metadata
- automatic background execution of reconfiguration steps

It is a deterministic planner and state transition helper for chain placement, recovery, and staged membership movement.
