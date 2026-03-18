package storage

import (
	"context"
	"testing"
	"time"
)

func mustOpenNode(t *testing.T, ctx context.Context, cfg Config, backend Backend, local LocalStateStore, coord CoordinatorClient, repl ReplicationTransport) *Node {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = &fakeClock{now: time.Unix(0, 0).UTC()}
	}
	node, err := OpenNode(ctx, cfg, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	return node
}

func TestAddReplicaAsTailReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	cmd := AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
		Epoch:      5,
	}
	if err := node.AddReplicaAsTail(ctx, cmd); err != nil {
		t.Fatalf("first AddReplicaAsTail returned error: %v", err)
	}
	replay := cmd
	replay.Epoch = 6
	if err := node.AddReplicaAsTail(ctx, replay); err != nil {
		t.Fatalf("replayed AddReplicaAsTail returned error: %v", err)
	}
	replica := node.State().Replicas[1]
	if got, want := replica.State, ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if got, want := node.HighestAcceptedCoordinatorEpoch(), uint64(6); got != want {
		t.Fatalf("highest accepted epoch = %d, want %d", got, want)
	}
}

func TestMarkReplicaLeavingReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
		Epoch:      5,
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1, Epoch: 5}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1, Epoch: 5}); err != nil {
		t.Fatalf("first MarkReplicaLeaving returned error: %v", err)
	}
	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1, Epoch: 6}); err != nil {
		t.Fatalf("replayed MarkReplicaLeaving returned error: %v", err)
	}
	replica := node.State().Replicas[1]
	if got, want := replica.State, ReplicaStateLeaving; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if got, want := node.HighestAcceptedCoordinatorEpoch(), uint64(6); got != want {
		t.Fatalf("highest accepted epoch = %d, want %d", got, want)
	}
}

func TestResumeRecoveredReplicaReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repl := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	local := NewInMemoryLocalStateStore()
	node := mustOpenNode(t, ctx, Config{NodeID: "node-a"}, backend, local, NewInMemoryCoordinatorClient(), repl)

	assignment := ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: ReplicaRoleSingle}
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment, Epoch: 5}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1, Epoch: 5}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if _, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	reopened := mustOpenNode(t, ctx, Config{NodeID: "node-a"}, backend, local, NewInMemoryCoordinatorClient(), repl)
	if got, want := reopened.State().Replicas[1].State, ReplicaStateRecovered; got != want {
		t.Fatalf("reopened state = %q, want %q", got, want)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, ResumeRecoveredReplicaCommand{Assignment: assignment, Epoch: 6}); err != nil {
		t.Fatalf("first ResumeRecoveredReplica returned error: %v", err)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, ResumeRecoveredReplicaCommand{Assignment: assignment, Epoch: 7}); err != nil {
		t.Fatalf("replayed ResumeRecoveredReplica returned error: %v", err)
	}
	if got, want := reopened.State().Replicas[1].State, ReplicaStateActive; got != want {
		t.Fatalf("reopened state after replay = %q, want %q", got, want)
	}
}

func TestRecoverReplicaReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repl := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, NewInMemoryCoordinatorClient(), repl)
	repl.Register("source", sourceBackend)
	repl.RegisterNode("source", source)
	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 2, Role: ReplicaRoleSingle},
		Epoch:      5,
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2, Epoch: 5}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if _, err := source.SubmitPut(ctx, 2, "k", "v"); err != nil {
		t.Fatalf("source SubmitPut returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	target := mustNewNode(t, ctx, Config{NodeID: "target"}, targetBackend, NewInMemoryCoordinatorClient(), repl)
	repl.Register("target", targetBackend)
	repl.RegisterNode("target", target)

	cmd := RecoverReplicaCommand{
		Assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 5,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
		SourceNodeID: "source",
		Epoch:        6,
	}
	if err := target.RecoverReplica(ctx, cmd); err != nil {
		t.Fatalf("first RecoverReplica returned error: %v", err)
	}
	cmd.Epoch = 7
	if err := target.RecoverReplica(ctx, cmd); err != nil {
		t.Fatalf("replayed RecoverReplica returned error: %v", err)
	}
	replica := target.State().Replicas[2]
	if got, want := replica.State, ReplicaStateActive; got != want {
		t.Fatalf("target state after replay = %q, want %q", got, want)
	}
}
