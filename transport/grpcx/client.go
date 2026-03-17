package grpcx

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/storage"
	grpcproto "github.com/danthegoodman1/chainrep/proto/chainrep/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: map[string]*grpc.ClientConn{}}
}

func (p *ConnPool) DialContext(ctx context.Context, target string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	if conn, ok := p.conns[target]; ok {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	}
	conn, err := grpc.DialContext(
		ctx,
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.conns[target]; ok {
		_ = conn.Close()
		return existing, nil
	}
	p.conns[target] = conn
	return conn, nil
}

func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for target, conn := range p.conns {
		errs = append(errs, conn.Close())
		delete(p.conns, target)
	}
	return joinErrors(errs...)
}

type CoordinatorAdminClient struct {
	target string
	pool   *ConnPool
}

func NewCoordinatorAdminClient(target string, pool *ConnPool) *CoordinatorAdminClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &CoordinatorAdminClient{target: target, pool: pool}
}

func (c *CoordinatorAdminClient) RoutingSnapshot(ctx context.Context) (coordserver.RoutingSnapshot, error) {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return coordserver.RoutingSnapshot{}, err
	}
	client := grpcproto.NewCoordinatorServiceClient(conn)
	resp, err := client.RoutingSnapshot(ctx, &grpcproto.RoutingSnapshotRequest{})
	if err != nil {
		return coordserver.RoutingSnapshot{}, decodeError(err)
	}
	return fromProtoRoutingSnapshot(resp), nil
}

func (c *CoordinatorAdminClient) Bootstrap(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return coordruntime.State{}, err
	}
	client := grpcproto.NewCoordinatorServiceClient(conn)
	req := &grpcproto.BootstrapRequest{
		CommandId:       cmd.ID,
		ExpectedVersion: cmd.ExpectedVersion,
		SlotCount:       int32(cmd.Bootstrap.Config.SlotCount),
		ReplicationFactor: int32(cmd.Bootstrap.Config.ReplicationFactor),
	}
	for _, node := range cmd.Bootstrap.Nodes {
		req.Nodes = append(req.Nodes, protoNode(node))
	}
	resp, err := client.Bootstrap(ctx, req)
	if err != nil {
		return coordruntime.State{}, decodeError(err)
	}
	return coordruntime.State{Version: resp.Version}, nil
}

func (c *CoordinatorAdminClient) AddNode(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, grpcproto.NewCoordinatorServiceClient, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.AddNode(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) BeginDrainNode(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, grpcproto.NewCoordinatorServiceClient, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.BeginDrainNode(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) MarkNodeDead(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, grpcproto.NewCoordinatorServiceClient, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.MarkNodeDead(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) EvaluateLiveness(ctx context.Context) error {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return err
	}
	client := grpcproto.NewCoordinatorServiceClient(conn)
	_, err = client.EvaluateLiveness(ctx, &grpcproto.Empty{})
	return decodeError(err)
}

func (c *CoordinatorAdminClient) mutateMembership(
	ctx context.Context,
	newClient func(grpc.ClientConnInterface) grpcproto.CoordinatorServiceClient,
	call func(grpcproto.CoordinatorServiceClient, context.Context, *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error),
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return coordruntime.State{}, err
	}
	req := &grpcproto.MembershipMutationRequest{
		CommandId:       cmd.ID,
		ExpectedVersion: cmd.ExpectedVersion,
		MaxChangedChains: int32(cmd.Reconfigure.Policy.MaxChangedChains),
	}
	if len(cmd.Reconfigure.Events) > 0 {
		event := cmd.Reconfigure.Events[0]
		req.Node = protoNode(event.Node)
		req.NodeId = event.NodeID
	}
	resp, err := call(newClient(conn), ctx, req)
	if err != nil {
		return coordruntime.State{}, decodeError(err)
	}
	return coordruntime.State{Version: resp.Version}, nil
}

type CoordinatorReporterClient struct {
	target string
	pool   *ConnPool
	nodeID string
}

func NewCoordinatorReporterClient(nodeID string, target string, pool *ConnPool) *CoordinatorReporterClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &CoordinatorReporterClient{nodeID: nodeID, target: target, pool: pool}
}

func (c *CoordinatorReporterClient) ReportReplicaReady(ctx context.Context, slot int) error {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewCoordinatorServiceClient(conn).ReportReplicaReady(ctx, &grpcproto.ReplicaReadyReport{
		NodeId: c.nodeID,
		Slot:   int32(slot),
	})
	return decodeError(err)
}

func (c *CoordinatorReporterClient) ReportReplicaRemoved(ctx context.Context, slot int) error {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewCoordinatorServiceClient(conn).ReportReplicaRemoved(ctx, &grpcproto.ReplicaRemovedReport{
		NodeId: c.nodeID,
		Slot:   int32(slot),
	})
	return decodeError(err)
}

func (c *CoordinatorReporterClient) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewCoordinatorServiceClient(conn).ReportNodeRecovered(ctx, protoNodeRecovery(report))
	return decodeError(err)
}

func (c *CoordinatorReporterClient) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewCoordinatorServiceClient(conn).ReportNodeHeartbeat(ctx, protoNodeStatus(status))
	return decodeError(err)
}

type StorageNodeClient struct {
	target string
	pool   *ConnPool
}

func NewStorageNodeClient(target string, pool *ConnPool) *StorageNodeClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &StorageNodeClient{target: target, pool: pool}
}

func (c *StorageNodeClient) storageClient(ctx context.Context) (grpcproto.StorageServiceClient, error) {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return nil, err
	}
	return grpcproto.NewStorageServiceClient(conn), nil
}

func (c *StorageNodeClient) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.AddReplicaAsTail(ctx, &grpcproto.AddReplicaAsTailCommand{Assignment: protoAssignment(cmd.Assignment)})
	return decodeError(err)
}

func (c *StorageNodeClient) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ActivateReplica(ctx, &grpcproto.ActivateReplicaCommand{Slot: int32(cmd.Slot)})
	return decodeError(err)
}

func (c *StorageNodeClient) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.MarkReplicaLeaving(ctx, &grpcproto.MarkReplicaLeavingCommand{Slot: int32(cmd.Slot)})
	return decodeError(err)
}

func (c *StorageNodeClient) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.RemoveReplica(ctx, &grpcproto.RemoveReplicaCommand{Slot: int32(cmd.Slot)})
	return decodeError(err)
}

func (c *StorageNodeClient) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.UpdateChainPeers(ctx, &grpcproto.UpdateChainPeersCommand{Assignment: protoAssignment(cmd.Assignment)})
	return decodeError(err)
}

func (c *StorageNodeClient) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ResumeRecoveredReplica(ctx, &grpcproto.ResumeRecoveredReplicaCommand{Assignment: protoAssignment(cmd.Assignment)})
	return decodeError(err)
}

func (c *StorageNodeClient) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.RecoverReplica(ctx, &grpcproto.RecoverReplicaCommand{
		Assignment:   protoAssignment(cmd.Assignment),
		SourceNodeId: cmd.SourceNodeID,
	})
	return decodeError(err)
}

func (c *StorageNodeClient) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.DropRecoveredReplica(ctx, &grpcproto.DropRecoveredReplicaCommand{Slot: int32(cmd.Slot)})
	return decodeError(err)
}

type DynamicNodeClientFactory struct {
	pool *ConnPool
}

func NewDynamicNodeClientFactory(pool *ConnPool) *DynamicNodeClientFactory {
	if pool == nil {
		pool = NewConnPool()
	}
	return &DynamicNodeClientFactory{pool: pool}
}

func (f *DynamicNodeClientFactory) ClientForNode(node coordinator.Node) (coordserver.StorageNodeClient, error) {
	if node.RPCAddress == "" {
		return nil, fmt.Errorf("%w: node %q has empty rpc address", coordserver.ErrUnknownNode, node.ID)
	}
	return NewStorageNodeClient(node.RPCAddress, f.pool), nil
}

type ReplicationTransport struct {
	pool *ConnPool
}

func NewReplicationTransport(pool *ConnPool) *ReplicationTransport {
	if pool == nil {
		pool = NewConnPool()
	}
	return &ReplicationTransport{pool: pool}
}

func (t *ReplicationTransport) FetchSnapshot(ctx context.Context, fromTarget string, slot int) (storage.Snapshot, error) {
	conn, err := t.pool.DialContext(ctx, fromTarget)
	if err != nil {
		return nil, err
	}
	stream, err := grpcproto.NewStorageServiceClient(conn).FetchSnapshot(ctx, &grpcproto.FetchSnapshotRequest{Slot: int32(slot)})
	if err != nil {
		return nil, decodeError(err)
	}
	var entries []*grpcproto.SnapshotEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, decodeError(err)
		}
		entries = append(entries, entry)
	}
	return snapshotFromProtoEntries(entries)
}

func (t *ReplicationTransport) FetchCommittedSequence(ctx context.Context, fromTarget string, slot int) (uint64, error) {
	conn, err := t.pool.DialContext(ctx, fromTarget)
	if err != nil {
		return 0, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).FetchCommittedSequence(ctx, &grpcproto.FetchCommittedSequenceRequest{Slot: int32(slot)})
	if err != nil {
		return 0, decodeError(err)
	}
	return resp.Sequence, nil
}

func (t *ReplicationTransport) ForwardWrite(ctx context.Context, toTarget string, req storage.ForwardWriteRequest) error {
	conn, err := t.pool.DialContext(ctx, toTarget)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewStorageServiceClient(conn).ForwardWrite(ctx, &grpcproto.ForwardWriteRequest{
		Operation: &grpcproto.WriteOperation{
			Slot:     int32(req.Operation.Slot),
			Sequence: req.Operation.Sequence,
			Kind:     string(req.Operation.Kind),
			Key:      req.Operation.Key,
			Value:    req.Operation.Value,
		},
		FromNodeId: req.FromNodeID,
	})
	return decodeError(err)
}

func (t *ReplicationTransport) CommitWrite(ctx context.Context, toTarget string, req storage.CommitWriteRequest) error {
	conn, err := t.pool.DialContext(ctx, toTarget)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewStorageServiceClient(conn).CommitWrite(ctx, &grpcproto.CommitWriteRequest{
		Slot:       int32(req.Slot),
		Sequence:   req.Sequence,
		FromNodeId: req.FromNodeID,
	})
	return decodeError(err)
}

type ClientTransport struct {
	pool *ConnPool
}

func NewClientTransport(pool *ConnPool) *ClientTransport {
	if pool == nil {
		pool = NewConnPool()
	}
	return &ClientTransport{pool: pool}
}

func (t *ClientTransport) Get(ctx context.Context, target string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.ReadResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Get(ctx, &grpcproto.ClientGetRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
	})
	if err != nil {
		return storage.ReadResult{}, decodeError(err)
	}
	return fromProtoReadResult(resp), nil
}

func (t *ClientTransport) Put(ctx context.Context, target string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.CommitResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Put(ctx, &grpcproto.ClientPutRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		Value:                req.Value,
		ExpectedChainVersion: req.ExpectedChainVersion,
	})
	if err != nil {
		return storage.CommitResult{}, decodeError(err)
	}
	return fromProtoCommitResult(resp), nil
}

func (t *ClientTransport) Delete(ctx context.Context, target string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.CommitResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Delete(ctx, &grpcproto.ClientDeleteRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
	})
	if err != nil {
		return storage.CommitResult{}, decodeError(err)
	}
	return fromProtoCommitResult(resp), nil
}
