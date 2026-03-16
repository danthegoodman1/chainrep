# Coordinator Correctness Spec

This document defines the current behavioral contract for the `coordinator` package.

It is intentionally scoped to the implementation that exists in this repository today. The corresponding Go tests are expected to enforce this contract directly.

## State Model

The coordinator manages:

- a fixed set of logical slots
- one ordered chain per slot
- one physical-node catalog with failure-domain metadata
- one global health state per node
- one per-chain replica state per replica assignment

### Replica states

- `joining`
- `active`
- `leaving`

### Node health

- `alive`
- `dead`

### Draining

Draining is modeled separately from node health. A draining node is still globally `alive`, but it is ineligible as a new placement candidate and should be replaced out of chains over time.

## Bootstrap Contract

`BuildInitialPlacement` must:

- reject invalid config or invalid node metadata
- reject bootstrap if the replication factor cannot be satisfied
- build exactly one chain per slot
- build exactly `ReplicationFactor` replicas per chain
- produce only `active` replicas
- produce deterministic output for the same effective input

### Bootstrap placement invariants

For every chain:

- no duplicate node ID may appear
- all referenced nodes must exist in the node catalog
- failure-domain constraints must hold pairwise across replicas
- the chain order is deterministic

## Reconfiguration Contract

`PlanReconfiguration` accepts:

- a prior `ClusterState`
- an ordered list of events
- a reconfiguration policy

It returns:

- an updated state
- changed slots with explicit steps
- unassigned live nodes if optional rebalance could not use them yet

`ApplyProgress` advances staged transitions after the caller reports progress.

## Global Reconfiguration Invariants

For all produced states:

- each chain slot index must match its position in the slice
- every replica must reference a known node
- every known node in the produced state must have a node-health entry
- no chain may contain duplicate node IDs
- no chain may retain a `dead` node
- failure-domain constraints must still hold for all replicas present in a chain
- every chain must retain at least one `active` replica
- no chain may exceed `ReplicationFactor + 1` replicas
- every chain must have at least `ReplicationFactor` replicas

### Replica-state shape

Produced chains are constrained to one of these forms:

- active-only
- active prefix followed by joining suffix
- active prefix followed by leaving suffix

Produced chains must not mix `joining` and `leaving` replicas in the same chain state.

## Event Semantics

### `add_node`

- adds a new live node to the catalog
- does not remove or directly invalidate any existing replica
- may leave the new node unassigned if the disruption budget is too small
- optional rebalance must stay continuity-safe

### `begin_drain_node`

- marks a node as draining
- the node remains `alive`
- affected chains may only replace it through staged tail append, activation, then leaving/removal

### `mark_node_dead`

- marks the node globally `dead`
- dead replicas must be removed from affected chains immediately in the planned state
- affected chains must be repaired by appending replacement tails if possible
- if no active anchor survives, planning must fail

### `replica_became_active`

- valid only for a `joining` replica already present in the addressed slot
- converts that replica to `active`

### `replica_removed`

- valid only for a `leaving` replica or a replica on a `dead` node
- removes that replica from the addressed slot

## Continuity Rules

The current implementation is intentionally conservative.

### General continuity

- a successful changed chain must share at least one replica with its prior version
- the planner must not emit a fully disjoint remap in one step
- active-anchor continuity must be preserved across successful planned transitions

### Append-tail steps

For a slot plan whose steps are all `append_tail`:

- all surviving replicas from the prior chain must keep their relative order
- any newly introduced replicas must appear only as a tail suffix
- all newly introduced replicas must be `joining`

This covers:

- dead-node repair
- drain replacement append
- join-driven first-step append

### Mark-leaving steps

For a slot plan whose steps are all `mark_leaving`:

- no new node may be introduced
- the produced chain must contain the same node-ID set as the prior chain
- leaving replicas must appear only as a tail suffix
- the active prefix must be exactly `ReplicationFactor`

This covers:

- drain finalization
- join finalization

## Disruption Budget

`ReconfigurationPolicy.MaxChangedChains` applies only to optional join-driven rebalance.

It does not limit mandatory repair after dead-node removal.

If optional rebalance cannot proceed within this budget, the node may remain live but unassigned.

## Forbidden Transformations

The coordinator must reject or avoid:

- bootstrap without enough eligible replicas
- unsupported events
- illegal progress transitions
- planning a changed chain with no active anchor
- fully disjoint single-step remaps
- use of dead nodes as new placement candidates
- use of draining nodes as new placement candidates
- optional join-driven chain mutations that violate the one-step continuity shape

## Determinism

For identical:

- input state
- event list
- policy

the planner must produce identical results.

For identical bootstrap inputs, placement must also be identical.
