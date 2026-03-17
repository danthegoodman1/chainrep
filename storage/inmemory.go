package storage

import (
	"context"
	"fmt"
)

type InMemoryBackend struct {
	replicas map[int]Snapshot
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		replicas: make(map[int]Snapshot),
	}
}

func (b *InMemoryBackend) CreateReplica(slot int) error {
	if _, exists := b.replicas[slot]; exists {
		return fmt.Errorf("%w: slot %d", ErrReplicaExists, slot)
	}
	b.replicas[slot] = Snapshot{}
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
	snapshot, exists := b.replicas[slot]
	if !exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return cloneSnapshot(snapshot), nil
}

func (b *InMemoryBackend) InstallSnapshot(slot int, snap Snapshot) error {
	if _, exists := b.replicas[slot]; !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	b.replicas[slot] = cloneSnapshot(snap)
	return nil
}

func (b *InMemoryBackend) Put(slot int, key string, value string) error {
	snapshot, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	snapshot[key] = value
	return nil
}

func (b *InMemoryBackend) ReplicaData(slot int) (Snapshot, error) {
	return b.Snapshot(slot)
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
}

func NewInMemoryReplicationTransport() *InMemoryReplicationTransport {
	return &InMemoryReplicationTransport{
		backends: make(map[string]Backend),
	}
}

func (t *InMemoryReplicationTransport) Register(nodeID string, backend Backend) {
	t.backends[nodeID] = backend
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
