# TODO

- [x] Build deterministic coordinator initial placement.
- [x] Build deterministic coordinator reconfiguration and recovery planning.
- [x] Add coordinator spec, hardening, simulation, and benchmark coverage.
- [x] Add durable coordinator runtime with WAL, checkpointing, and recovery.
- [x] Define the storage node API and state model.
- [x] Add a coordinator transport interface for control-plane communication.
- [x] Add a storage replication transport interface for node-to-node communication.
- [x] Build in-memory reference implementations for both transport layers.
- [x] Add end-to-end tests that run a coordinator plus several storage nodes without real networking.
- [ ] Add a durable or interface-stabilized local storage backend beyond the in-memory KV model.
- [x] Add the coordinator server that issues storage-node commands and consumes storage-node progress.
- [ ] Add node liveness detection and heartbeat-driven reconfiguration policy, including suspect/dead timeouts and automatic coordinator actions.
- [ ] Add real network transports for coordinator control-plane traffic and storage-node replication traffic.
- [ ] Audit and harden behavior that currently assumes synchronous in-memory transport delivery, especially dispatch ordering, progress timing, and write ack propagation.
- [x] Implement storage-node persistence and restart/recovery semantics.
- [x] Implement the steady-state chain replication protocol between storage nodes.
- [x] Implement client-facing read/write execution and routing.
- [ ] Stored objects have associated metadata, enabling conditional writes (e.g. monotonic fencing token per-object, auto-incremented on mutation)
