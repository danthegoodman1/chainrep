package coordserver

import (
	"context"
	"fmt"

	"github.com/danthegoodman1/chainrep/storage"
)

type InMemoryNodeAdapter struct {
	nodeID string
	node   *storage.Node
	sink   *Server
	local  storage.LocalStateStore
}

func NewInMemoryNodeAdapter(nodeID string, backend storage.Backend, repl storage.ReplicationTransport) (*InMemoryNodeAdapter, error) {
	return OpenInMemoryNodeAdapter(nodeID, backend, storage.NewInMemoryLocalStateStore(), repl)
}

func OpenInMemoryNodeAdapter(
	nodeID string,
	backend storage.Backend,
	local storage.LocalStateStore,
	repl storage.ReplicationTransport,
) (*InMemoryNodeAdapter, error) {
	adapter := &InMemoryNodeAdapter{nodeID: nodeID, local: local}
	node, err := storage.OpenNode(
		storage.Config{NodeID: nodeID},
		backend,
		local,
		adapter,
		repl,
	)
	if err != nil {
		return nil, fmt.Errorf("err in storage.OpenNode: %w", err)
	}
	adapter.node = node
	return adapter, nil
}

func (a *InMemoryNodeAdapter) BindServer(server *Server) {
	a.sink = server
}

func (a *InMemoryNodeAdapter) Node() *storage.Node {
	return a.node
}

func (a *InMemoryNodeAdapter) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	return a.node.AddReplicaAsTail(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	return a.node.ActivateReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	return a.node.MarkReplicaLeaving(ctx, cmd)
}

func (a *InMemoryNodeAdapter) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	return a.node.RemoveReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	return a.node.UpdateChainPeers(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	return a.node.ResumeRecoveredReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	return a.node.RecoverReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	return a.node.DropRecoveredReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ReportReplicaReady(ctx context.Context, slot int) error {
	if a.sink == nil {
		return nil
	}
	_, err := a.sink.ReportReplicaReady(ctx, a.nodeID, slot, "")
	return err
}

func (a *InMemoryNodeAdapter) ReportReplicaRemoved(ctx context.Context, slot int) error {
	if a.sink == nil {
		return nil
	}
	_, err := a.sink.ReportReplicaRemoved(ctx, a.nodeID, slot, "")
	return err
}

func (a *InMemoryNodeAdapter) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if a.sink == nil {
		return nil
	}
	return a.sink.ReportNodeHeartbeat(ctx, status)
}

func (a *InMemoryNodeAdapter) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if a.sink == nil {
		return nil
	}
	return a.sink.ReportNodeRecovered(ctx, report)
}
