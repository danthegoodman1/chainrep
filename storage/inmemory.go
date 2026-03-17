package storage

import (
	"context"
	"fmt"
	"sort"
)

type stagedOperation struct {
	kind  OperationKind
	key   string
	value string
}

type replicaData struct {
	committed        Snapshot
	staged           map[uint64]stagedOperation
	highestCommitted uint64
}

type InMemoryBackend struct {
	replicas map[int]*replicaData
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		replicas: make(map[int]*replicaData),
	}
}

func (b *InMemoryBackend) CreateReplica(slot int) error {
	if _, exists := b.replicas[slot]; exists {
		return fmt.Errorf("%w: slot %d", ErrReplicaExists, slot)
	}
	b.replicas[slot] = &replicaData{
		committed: Snapshot{},
		staged:    map[uint64]stagedOperation{},
	}
	return nil
}

func (b *InMemoryBackend) DeleteReplica(slot int) error {
	if _, exists := b.replicas[slot]; !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	delete(b.replicas, slot)
	return nil
}

func (b *InMemoryBackend) Snapshot(slot int) (Snapshot, error) {
	replica, exists := b.replicas[slot]
	if !exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return cloneSnapshot(replica.committed), nil
}

func (b *InMemoryBackend) InstallSnapshot(slot int, snap Snapshot) error {
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	replica.committed = cloneSnapshot(snap)
	replica.staged = map[uint64]stagedOperation{}
	return nil
}

func (b *InMemoryBackend) Put(slot int, key string, value string) error {
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	replica.committed[key] = value
	return nil
}

func (b *InMemoryBackend) ReplicaData(slot int) (Snapshot, error) {
	return b.CommittedSnapshot(slot)
}

func (b *InMemoryBackend) StagePut(slot int, sequence uint64, key string, value string) error {
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if _, exists := replica.staged[sequence]; exists {
		return fmt.Errorf("%w: slot %d sequence %d already staged", ErrSequenceMismatch, slot, sequence)
	}
	replica.staged[sequence] = stagedOperation{kind: OperationKindPut, key: key, value: value}
	return nil
}

func (b *InMemoryBackend) StageDelete(slot int, sequence uint64, key string) error {
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if _, exists := replica.staged[sequence]; exists {
		return fmt.Errorf("%w: slot %d sequence %d already staged", ErrSequenceMismatch, slot, sequence)
	}
	replica.staged[sequence] = stagedOperation{kind: OperationKindDelete, key: key}
	return nil
}

func (b *InMemoryBackend) CommitSequence(slot int, sequence uint64) error {
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if sequence != replica.highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			ErrSequenceMismatch,
			slot,
			replica.highestCommitted+1,
			sequence,
		)
	}
	operation, exists := replica.staged[sequence]
	if !exists {
		return fmt.Errorf("%w: slot %d sequence %d is not staged", ErrSequenceMismatch, slot, sequence)
	}
	switch operation.kind {
	case OperationKindPut:
		replica.committed[operation.key] = operation.value
	case OperationKindDelete:
		delete(replica.committed, operation.key)
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, operation.kind)
	}
	delete(replica.staged, sequence)
	replica.highestCommitted = sequence
	return nil
}

func (b *InMemoryBackend) CommittedSnapshot(slot int) (Snapshot, error) {
	return b.Snapshot(slot)
}

func (b *InMemoryBackend) StagedSequences(slot int) ([]uint64, error) {
	replica, exists := b.replicas[slot]
	if !exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	sequences := make([]uint64, 0, len(replica.staged))
	for sequence := range replica.staged {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool {
		return sequences[i] < sequences[j]
	})
	return sequences, nil
}

type InMemoryCoordinatorClient struct {
	ReadySlots   []int
	RemovedSlots []int
	Heartbeats   []NodeStatus
	ReadyErr     error
	RemovedErr   error
	HeartbeatErr error
}

func NewInMemoryCoordinatorClient() *InMemoryCoordinatorClient {
	return &InMemoryCoordinatorClient{}
}

func (c *InMemoryCoordinatorClient) ReportReplicaReady(_ context.Context, slot int) error {
	if c.ReadyErr != nil {
		return c.ReadyErr
	}
	c.ReadySlots = append(c.ReadySlots, slot)
	return nil
}

func (c *InMemoryCoordinatorClient) ReportReplicaRemoved(_ context.Context, slot int) error {
	if c.RemovedErr != nil {
		return c.RemovedErr
	}
	c.RemovedSlots = append(c.RemovedSlots, slot)
	return nil
}

func (c *InMemoryCoordinatorClient) ReportNodeHeartbeat(_ context.Context, status NodeStatus) error {
	if c.HeartbeatErr != nil {
		return c.HeartbeatErr
	}
	c.Heartbeats = append(c.Heartbeats, status)
	return nil
}

type InMemoryReplicationTransport struct {
	backends map[string]Backend
	nodes    map[string]replicationHandler
}

func NewInMemoryReplicationTransport() *InMemoryReplicationTransport {
	return &InMemoryReplicationTransport{
		backends: make(map[string]Backend),
		nodes:    make(map[string]replicationHandler),
	}
}

func (t *InMemoryReplicationTransport) Register(nodeID string, backend Backend) {
	t.backends[nodeID] = backend
}

func (t *InMemoryReplicationTransport) RegisterNode(nodeID string, node replicationHandler) {
	t.nodes[nodeID] = node
}

func (t *InMemoryReplicationTransport) FetchSnapshot(_ context.Context, fromNodeID string, slot int) (Snapshot, error) {
	backend, ok := t.backends[fromNodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, fromNodeID)
	}

	snapshot, err := backend.Snapshot(slot)
	if err != nil {
		return nil, fmt.Errorf("err in backend.Snapshot: %w", err)
	}
	return cloneSnapshot(snapshot), nil
}

func (t *InMemoryReplicationTransport) ForwardWrite(ctx context.Context, toNodeID string, req ForwardWriteRequest) error {
	node, ok := t.nodes[toNodeID]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if err := node.HandleForwardWrite(ctx, req); err != nil {
		return fmt.Errorf("err in node.HandleForwardWrite: %w", err)
	}
	return nil
}

func (t *InMemoryReplicationTransport) CommitWrite(ctx context.Context, toNodeID string, req CommitWriteRequest) error {
	node, ok := t.nodes[toNodeID]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if err := node.HandleCommitWrite(ctx, req); err != nil {
		return fmt.Errorf("err in node.HandleCommitWrite: %w", err)
	}
	return nil
}

type replicationHandler interface {
	HandleForwardWrite(ctx context.Context, req ForwardWriteRequest) error
	HandleCommitWrite(ctx context.Context, req CommitWriteRequest) error
}
