# chainrep - Chain Replication in Go

- Real gRPC transport between client, coordinator, and storage nodes
- Coordinator HA via epoch-gated active/standby failover
- Durable local Badger-backed storage backend
- Conditional writes with per-object metadata
- Optional TLS/mTLS security for gRPC transports
- Read-only HTTP admin/health endpoints plus Prometheus metrics

- [Architecture](./ARCHITECTURE.md)
- [Coordinator HA Store](./HA_STORE.md)
- [Coordinator](./coordinator/README.md)
- [Observability](./OBSERVABILITY.md)
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

The repo includes a real end-to-end localhost gRPC benchmark in
[`transport/grpcx/grpc_benchmark_test.go`](./transport/grpcx/grpc_benchmark_test.go).
It uses:

- a real coordinator gRPC server
- a real client router
- real storage-node gRPC servers
- real gRPC replication between nodes
- real Badger-backed storage on local temp directories

These numbers are end-to-end client latencies on localhost through a production-style
setup: router -> coordinator snapshot -> storage-node gRPC server(s) -> Badger-backed
storage, with replication over gRPC where applicable.

Benchmark command:

```bash
go test ./transport/grpcx -run '^$' -bench BenchmarkClientLatencyGRPC_Localhost -benchmem -benchtime=3s -count=5 -cpu=1
```

Average results from 5 localhost benchmark runs on an Apple M3 Max on March 18, 2026, using the command above:

- `single_replica_get`: `0.044 ms/op`, `10,656 B/op`, `186 allocs/op`
- `single_replica_put`: `0.100 ms/op`, `13,976 B/op`, `291 allocs/op`
- `three_replica_get`: `0.045 ms/op`, `10,657 B/op`, `186 allocs/op`
- `three_replica_put`: `0.325 ms/op`, `58,215 B/op`, `1,118 allocs/op`

These are reference localhost numbers for the current implementation, not SLOs or
cross-machine production guarantees. They include real gRPC and durable local storage
costs, but not network latency, TLS, or multi-host deployment effects. The benchmark
uses real Badger-backed storage, but the measured read path is likely hot-cache behavior
through Badger and the OS page cache rather than cold disk-read latency.
