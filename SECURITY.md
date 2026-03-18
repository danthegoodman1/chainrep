# Security

`chainrep` now supports optional gRPC transport security.

The default transport remains insecure so local development and existing tests keep
working without extra setup. For any non-trivial deployment, enable TLS.

## Model

V1 uses a simple cluster-CA model:

- one CA signs coordinator and storage-node certificates
- internal coordinator/storage RPCs use mTLS
- client-facing RPCs use TLS, with optional client certificate verification
- internal authorization is identity-bound, not CA-only trust

That means:

- a valid cluster-CA certificate is required to connect on internal planes
- the authenticated peer identity must also be authorized for the RPC being called
- client-facing RPCs may allow anonymous TLS clients or require client certs, depending
  on server configuration

## Identity Rules

The transport layer extracts peer identity from the certificate in this order:

1. first DNS SAN
2. first IP SAN
3. first URI SAN
4. Common Name

For predictable behavior, issue certificates with one logical DNS SAN first, for example:

- coordinator: `coordinator`
- storage node `a`: `a`
- storage node `b`: `b`

If the RPC endpoint is reached by IP or hostname, also include the actual host/IP in SANs
 so normal TLS server verification succeeds.

## RPC Authorization

Internal authorization is enforced by RPC plane:

- coordinator report RPCs accept authorized storage-node identities only
- report payload `node_id` must match the authenticated storage-node identity
- storage control RPCs accept authorized coordinator identities only
- storage replication RPCs accept authorized storage-node identities only
- replication `from_node_id` must match the authenticated storage-node identity
- client/admin RPCs are client-facing and do not require an internal coordinator/storage identity

## Configuration

The gRPC transport layer exposes optional TLS config in `transport/grpcx`.

Client-side config:

- `CAFile`
- optional `CertFile` and `KeyFile`
- optional `ServerName`

Server-side config:

- `CAFile`
- `CertFile`
- `KeyFile`
- `ClientAuth`
  - `disabled`
  - `verify-if-given`
  - `require-and-verify`
- `CoordinatorIdentities`
- `StorageNodeIdentities`

Recommended defaults:

- coordinator server: `verify-if-given`
- storage-node server: `verify-if-given`
- internal clients: mTLS with a node/coordinator certificate
- external clients: TLS with CA verification, optionally mTLS

`verify-if-given` is the practical default because one port serves both client-facing and
internal RPCs. It allows anonymous TLS clients for public RPCs while still requiring a
valid authenticated identity on internal RPCs.

## Certificate Rotation

Certificate material is loaded when the process starts.

Current behavior:

- server cert, key, and trusted CA files are read at startup
- client cert, key, and trusted CA files are read when the client connection pool is created
- changing cert or CA files on disk does not affect a running process
- rotating certs therefore requires restarting the affected coordinator, storage node, or client process

This is an intentional v1 tradeoff to keep the security implementation small and predictable.
`chainrep` is already designed to tolerate node restarts, so rolling restarts are the expected
rotation mechanism.

## Minimal Setup

Create a CA:

```bash
openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
  -keyout ca.key \
  -out ca.crt \
  -days 365 \
  -subj "/CN=chainrep-cluster-ca"
```

Create a coordinator certificate config, for example `coordinator.cnf`:

```ini
[req]
distinguished_name = dn
req_extensions = req_ext
prompt = no

[dn]
CN = coordinator

[req_ext]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth,clientAuth

[alt_names]
DNS.1 = coordinator
IP.1 = 127.0.0.1
```

Issue the coordinator cert:

```bash
openssl req -new -newkey rsa:4096 -nodes \
  -keyout coordinator.key \
  -out coordinator.csr \
  -config coordinator.cnf

openssl x509 -req \
  -in coordinator.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out coordinator.crt \
  -days 365 \
  -sha256 \
  -extensions req_ext \
  -extfile coordinator.cnf
```

Issue storage-node certs the same way, changing the logical DNS SAN to the node identity,
for example `a`, `b`, or `c`, and including the actual host/IP SAN used for TLS server
verification.

## Example Usage

Server:

```go
service, err := grpcx.NewStorageGRPCServerWithTLS(node, &grpcx.ServerTLSConfig{
    CAFile:                "ca.crt",
    CertFile:              "storage-a.crt",
    KeyFile:               "storage-a.key",
    ClientAuth:            grpcx.ClientAuthModeVerifyIfGiven,
    CoordinatorIdentities: []string{"coordinator"},
    StorageNodeIdentities: []string{"a", "b", "c"},
})
```

Internal client:

```go
pool, err := grpcx.NewConnPoolWithTLS(grpcx.ClientTLSConfig{
    CAFile:   "ca.crt",
    CertFile: "coordinator.crt",
    KeyFile:  "coordinator.key",
})
```

External client with TLS only:

```go
pool, err := grpcx.NewConnPoolWithTLS(grpcx.ClientTLSConfig{
    CAFile: "ca.crt",
})
```

## Testing

The transport package includes real TLS integration coverage:

- TLS-only client access
- optional client mTLS
- required-client-cert rejection
- internal authorization denial for wrong identities
- end-to-end secure coordinator/storage/router flows
- restart-based certificate handling

Run:

```bash
go test ./transport/grpcx
```
