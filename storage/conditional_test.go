package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConditionalWritesAndMetadataLifecycleSingleReplica(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(100, 0).UTC()}
	node := mustNewNode(t, ctx, Config{
		NodeID: "single",
		Clock:  clock,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})

	createOnly := false
	putResult, err := node.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 1,
		Key:                  "k",
		Value:                "v1",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Exists: &createOnly,
		},
	})
	if err != nil {
		t.Fatalf("HandleClientPut(create) returned error: %v", err)
	}
	assertMetadata(t, putResult.Metadata, ObjectMetadata{
		Version:   1,
		CreatedAt: clock.now,
		UpdatedAt: clock.now,
	})

	read, err := node.HandleClientGet(ctx, ClientGetRequest{
		Slot:                 1,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet returned error: %v", err)
	}
	assertMetadata(t, read.Metadata, *putResult.Metadata)

	exists := true
	clock.Advance(time.Second)
	updateResult, err := node.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 1,
		Key:                  "k",
		Value:                "v2",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Exists: &exists,
			Version: &VersionComparison{
				Operator: ComparisonOperatorEqual,
				Value:    1,
			},
			UpdatedAt: &TimeComparison{
				Operator: ComparisonOperatorLessThan,
				Value:    clock.now,
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleClientPut(update) returned error: %v", err)
	}
	assertMetadata(t, updateResult.Metadata, ObjectMetadata{
		Version:   2,
		CreatedAt: putResult.Metadata.CreatedAt,
		UpdatedAt: clock.now,
	})

	clock.Advance(time.Second)
	deleteResult, err := node.HandleClientDelete(ctx, ClientDeleteRequest{
		Slot:                 1,
		Key:                  "k",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Exists: &exists,
			Version: &VersionComparison{
				Operator: ComparisonOperatorEqual,
				Value:    2,
			},
			UpdatedAt: &TimeComparison{
				Operator: ComparisonOperatorLessThan,
				Value:    clock.now,
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleClientDelete returned error: %v", err)
	}
	assertMetadata(t, deleteResult.Metadata, ObjectMetadata{
		Version:   3,
		CreatedAt: putResult.Metadata.CreatedAt,
		UpdatedAt: clock.now,
	})

	read, err = node.HandleClientGet(ctx, ClientGetRequest{
		Slot:                 1,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet after delete returned error: %v", err)
	}
	if read.Found || read.Metadata != nil {
		t.Fatalf("read after delete = %#v, want not found without metadata", read)
	}

	clock.Advance(time.Second)
	recreateResult, err := node.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 1,
		Key:                  "k",
		Value:                "v3",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientPut(recreate) returned error: %v", err)
	}
	assertMetadata(t, recreateResult.Metadata, ObjectMetadata{
		Version:   1,
		CreatedAt: clock.now,
		UpdatedAt: clock.now,
	})
}

func TestConditionFailureDoesNotConsumeSequenceOrReplicate(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(200, 0).UTC()}
	transport := NewInMemoryReplicationTransport()
	headBackend := NewInMemoryBackend()
	tailBackend := NewInMemoryBackend()
	head := mustNewNode(t, ctx, Config{NodeID: "head", Clock: clock}, headBackend, NewInMemoryCoordinatorClient(), transport)
	tail := mustNewNode(t, ctx, Config{NodeID: "tail", Clock: clock}, tailBackend, NewInMemoryCoordinatorClient(), transport)
	transport.Register("head", headBackend)
	transport.Register("tail", tailBackend)
	transport.RegisterNode("head", head)
	transport.RegisterNode("tail", tail)

	mustActivateReplica(t, head, 4, ReplicaAssignment{
		Slot:         4,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})
	mustActivateReplica(t, tail, 4, ReplicaAssignment{
		Slot:         4,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "head"},
	})

	first, err := head.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 4,
		Key:                  "k",
		Value:                "v1",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("first HandleClientPut returned error: %v", err)
	}
	if got, want := first.Sequence, uint64(1); got != want {
		t.Fatalf("first sequence = %d, want %d", got, want)
	}

	clock.Advance(time.Second)
	_, err = head.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 4,
		Key:                  "k",
		Value:                "v2",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Version: &VersionComparison{
				Operator: ComparisonOperatorEqual,
				Value:    99,
			},
		},
	})
	if err == nil {
		t.Fatal("conditional HandleClientPut unexpectedly succeeded")
	}
	var failed *ConditionFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("error = %v, want condition failed", err)
	}
	if !failed.CurrentExists || failed.CurrentMetadata == nil || failed.CurrentMetadata.Version != 1 {
		t.Fatalf("condition failure = %#v, want current metadata version 1", failed)
	}
	if got, want := mustHighestCommitted(t, head, 4), uint64(1); got != want {
		t.Fatalf("head highest committed = %d, want %d", got, want)
	}
	if got, want := mustHighestCommitted(t, tail, 4), uint64(1); got != want {
		t.Fatalf("tail highest committed = %d, want %d", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, tail, 4)["k"], "v1"; got != want {
		t.Fatalf("tail value = %q, want %q", got, want)
	}

	next, err := head.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 4,
		Key:                  "k",
		Value:                "v2",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Version: &VersionComparison{
				Operator: ComparisonOperatorEqual,
				Value:    1,
			},
		},
	})
	if err != nil {
		t.Fatalf("next HandleClientPut returned error: %v", err)
	}
	if got, want := next.Sequence, uint64(2); got != want {
		t.Fatalf("next sequence = %d, want %d", got, want)
	}
}

func TestDeleteAbsentNoOpAndConditionSemantics(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "single", Clock: &fakeClock{now: time.Unix(300, 0).UTC()}}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 2, ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle})

	result, err := node.HandleClientDelete(ctx, ClientDeleteRequest{
		Slot:                 2,
		Key:                  "missing",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientDelete(absent) returned error: %v", err)
	}
	if result.Applied || result.Sequence != 0 || result.Metadata != nil {
		t.Fatalf("delete absent result = %#v, want unapplied no-op", result)
	}

	createOnly := false
	result, err = node.HandleClientDelete(ctx, ClientDeleteRequest{
		Slot:                 2,
		Key:                  "missing",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{Exists: &createOnly},
	})
	if err != nil {
		t.Fatalf("HandleClientDelete(absent, exists=false) returned error: %v", err)
	}
	if result.Applied || result.Metadata != nil {
		t.Fatalf("delete absent with exists=false = %#v, want unapplied no-op", result)
	}

	_, err = node.HandleClientDelete(ctx, ClientDeleteRequest{
		Slot:                 2,
		Key:                  "missing",
		ExpectedChainVersion: 1,
		Conditions: WriteConditions{
			Version: &VersionComparison{
				Operator: ComparisonOperatorEqual,
				Value:    1,
			},
		},
	})
	if err == nil {
		t.Fatal("HandleClientDelete(absent, version) unexpectedly succeeded")
	}
	var failed *ConditionFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("error = %v, want condition failed", err)
	}
	if failed.CurrentExists || failed.CurrentMetadata != nil {
		t.Fatalf("condition failure = %#v, want absent current object", failed)
	}
}

func TestCatchupSnapshotPreservesObjectMetadata(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(400, 0).UTC()}
	transport := NewInMemoryReplicationTransport()
	sourceBackend := NewInMemoryBackend()
	targetBackend := NewInMemoryBackend()
	source := mustNewNode(t, ctx, Config{NodeID: "source", Clock: clock}, sourceBackend, NewInMemoryCoordinatorClient(), transport)
	target := mustNewNode(t, ctx, Config{NodeID: "target", Clock: clock}, targetBackend, NewInMemoryCoordinatorClient(), transport)
	transport.Register("source", sourceBackend)
	transport.Register("target", targetBackend)
	transport.RegisterNode("source", source)
	transport.RegisterNode("target", target)

	mustActivateReplica(t, source, 3, ReplicaAssignment{Slot: 3, ChainVersion: 1, Role: ReplicaRoleSingle})
	putResult, err := source.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 3,
		Key:                  "k",
		Value:                "v1",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("source HandleClientPut returned error: %v", err)
	}

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         3,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 3}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}

	snapshot, err := target.CommittedSnapshot(3)
	if err != nil {
		t.Fatalf("target CommittedSnapshot returned error: %v", err)
	}
	object := snapshot["k"]
	if object.Value != "v1" {
		t.Fatalf("target value = %q, want %q", object.Value, "v1")
	}
	assertMetadata(t, &object.Metadata, *putResult.Metadata)
}

func assertMetadata(t *testing.T, got *ObjectMetadata, want ObjectMetadata) {
	t.Helper()
	if got == nil {
		t.Fatalf("metadata = nil, want %#v", want)
	}
	if *got != want {
		t.Fatalf("metadata = %#v, want %#v", *got, want)
	}
}
