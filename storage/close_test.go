package storage

import (
	"context"
	"testing"
)

func TestNodeCloseClosesSharedOwnerOnlyOnce(t *testing.T) {
	ctx := context.Background()
	owner := &countingOwner{
		backend: NewInMemoryBackend(),
		local:   NewInMemoryLocalStateStore(),
	}
	node, err := OpenNode(ctx, Config{NodeID: "node-a"}, &countingBackend{owner: owner}, &countingLocal{owner: owner}, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if got, want := owner.closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
}

type countingOwner struct {
	backend    *InMemoryBackend
	local      *InMemoryLocalStateStore
	closeCalls int
	closed     bool
}

func (o *countingOwner) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.closeCalls++
	return nil
}

type countingBackend struct {
	owner *countingOwner
}

func (b *countingBackend) CreateReplica(slot int) error {
	return b.owner.backend.CreateReplica(slot)
}

func (b *countingBackend) DeleteReplica(slot int) error {
	return b.owner.backend.DeleteReplica(slot)
}

func (b *countingBackend) Snapshot(slot int) (Snapshot, error) {
	return b.owner.backend.Snapshot(slot)
}

func (b *countingBackend) InstallSnapshot(slot int, snap Snapshot) error {
	return b.owner.backend.InstallSnapshot(slot, snap)
}

func (b *countingBackend) SetHighestCommittedSequence(slot int, sequence uint64) error {
	return b.owner.backend.SetHighestCommittedSequence(slot, sequence)
}

func (b *countingBackend) StagePut(slot int, sequence uint64, key string, value string) error {
	return b.owner.backend.StagePut(slot, sequence, key, value)
}

func (b *countingBackend) StageDelete(slot int, sequence uint64, key string) error {
	return b.owner.backend.StageDelete(slot, sequence, key)
}

func (b *countingBackend) CommitSequence(slot int, sequence uint64) error {
	return b.owner.backend.CommitSequence(slot, sequence)
}

func (b *countingBackend) CommittedSnapshot(slot int) (Snapshot, error) {
	return b.owner.backend.CommittedSnapshot(slot)
}

func (b *countingBackend) GetCommitted(slot int, key string) (string, bool, error) {
	return b.owner.backend.GetCommitted(slot, key)
}

func (b *countingBackend) HighestCommittedSequence(slot int) (uint64, error) {
	return b.owner.backend.HighestCommittedSequence(slot)
}

func (b *countingBackend) StagedSequences(slot int) ([]uint64, error) {
	return b.owner.backend.StagedSequences(slot)
}

func (b *countingBackend) Close() error {
	return b.owner.Close()
}

type countingLocal struct {
	owner *countingOwner
}

func (l *countingLocal) LoadNode(ctx context.Context, nodeID string) (PersistedNodeState, error) {
	return l.owner.local.LoadNode(ctx, nodeID)
}

func (l *countingLocal) UpsertReplica(ctx context.Context, nodeID string, replica PersistedReplica) error {
	return l.owner.local.UpsertReplica(ctx, nodeID, replica)
}

func (l *countingLocal) DeleteReplica(ctx context.Context, nodeID string, slot int) error {
	return l.owner.local.DeleteReplica(ctx, nodeID, slot)
}

func (l *countingLocal) Close() error {
	return l.owner.Close()
}
