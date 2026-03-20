# chainrep - Chain Replication in Go

`chainrep` is a Go implementation of chain replication with a coordinator
control plane, multi-replica storage nodes, gRPC transport, and both non-HA and
HA coordinator modes.

Highlights:

- gRPC transport between clients, coordinator, and storage nodes
- epoch-gated coordinator HA failover
- durable local Badger-backed storage backend
- dynamic storage-node auto-registration and ready gating
- durable non-HA coordinator dispatch retry and restart recovery
- flapping-node eviction through coordinator liveness policy
- conditional writes with per-object metadata
- optional TLS/mTLS for gRPC transports
- read-only HTTP admin/health endpoints plus Prometheus metrics

Documentation:

- [Architecture](./ARCHITECTURE.md)
- [Coordinator HA Store](./HA_STORE.md)
- [Coordinator](./coordinator/README.md)
- [Observability](./OBSERVABILITY.md)
- [Quickstart](./QUICKSTART.md)
- [Security](./SECURITY.md)

## Ops Surfaces

Observability is documented in [OBSERVABILITY.md](./OBSERVABILITY.md).

At a high level, each coordinator or storage process can optionally expose a
separate read-only HTTP admin listener with:

- `/livez`
- `/readyz`
- `/metrics`
- `/admin/v1/state`

The admin listener is unauthenticated in v1 and is intended for loopback or a
trusted network only.

## Performance Notes

The repo includes an end-to-end localhost gRPC benchmark in
[`transport/grpcx/grpc_benchmark_test.go`](./transport/grpcx/grpc_benchmark_test.go).
It uses:

- a coordinator gRPC server
- a client router
- storage-node gRPC servers
- gRPC replication between nodes
- Badger-backed storage on local temp directories

These numbers are end-to-end client latencies on localhost through a
setup: router -> coordinator snapshot -> storage-node gRPC server(s) -> storage,
with replication over gRPC where applicable.

Benchmark command:

```bash
go test ./transport/grpcx -run '^$' -bench BenchmarkClientLatencyGRPC_Localhost -benchmem -benchtime=3s -count=5 -cpu=1
```

Average results from 5 localhost benchmark runs on an Apple M3 Max, using the command above:

- `single_replica_get`: `0.044 ms/op`, `10,656 B/op`, `186 allocs/op`
- `single_replica_put`: `0.100 ms/op`, `13,976 B/op`, `291 allocs/op`
- `three_replica_get`: `0.045 ms/op`, `10,657 B/op`, `186 allocs/op`
- `three_replica_put`: `0.325 ms/op`, `58,215 B/op`, `1,118 allocs/op`

These are localhost benchmark numbers, not SLOs or cross-machine production guarantees.
They include gRPC and durable local storage costs, but not network latency, TLS, or
multi-host deployment effects. The read path likely reflects cache-warm access through
Badger and the OS page cache rather than cold disk-read latency.
