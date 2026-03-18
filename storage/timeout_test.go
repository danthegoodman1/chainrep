package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSubmitPutTimesOutAndPreservesInFlightState(t *testing.T) {
	ctx := context.Background()
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Nanosecond,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}

	if got, want := mustNodeStagedSequences(t, node, 7), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 7), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustHighestCommitted(t, node, 7), uint64(0); got != want {
		t.Fatalf("highest committed = %d, want %d", got, want)
	}
	record := node.replicas[7]
	if _, ok := record.pendingWrites[1]; !ok {
		t.Fatal("pending write missing after timeout")
	}
}

func TestSubmitPutRespectsCallerCancellationBeforeDefaultTimeout(t *testing.T) {
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, context.Background(), Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Hour,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want canceled", err)
		}
	}
}

func TestSubmitPutUsesTighterCallerDeadline(t *testing.T) {
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, context.Background(), Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Hour,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}
}

func TestQueuedAwaitWriteCommitHonorsCanceledContext(t *testing.T) {
	transport := NewQueuedInMemoryReplicationTransport()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := transport.AwaitWriteCommit(ctx, func() bool { return false }); err == nil {
		t.Fatal("AwaitWriteCommit unexpectedly succeeded")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
}

type blockingWriteTransport struct{}

func (t *blockingWriteTransport) FetchSnapshot(context.Context, string, int) (Snapshot, error) {
	return Snapshot{}, nil
}

func (t *blockingWriteTransport) FetchCommittedSequence(context.Context, string, int) (uint64, error) {
	return 0, nil
}

func (t *blockingWriteTransport) ForwardWrite(context.Context, string, ForwardWriteRequest) error {
	return nil
}

func (t *blockingWriteTransport) CommitWrite(context.Context, string, CommitWriteRequest) error {
	return nil
}

func (t *blockingWriteTransport) AwaitWriteCommit(ctx context.Context, check func() bool) error {
	if check() {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
