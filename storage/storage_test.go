package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestInMemoryBackendSnapshotIsDeepCopy(t *testing.T) {
	backend := NewInMemoryBackend()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := backend.Put(1, "k1", "v1", testObjectMetadata(1)); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	snapshot, err := backend.Snapshot(1)
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	snapshot["k1"] = CommittedObject{Value: "mutated", Metadata: testObjectMetadata(9)}

	data, err := backend.ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData returned error: %v", err)
	}
	if got, want := data["k1"].Value, "v1"; got != want {
		t.Fatalf("stored value = %q, want %q", got, want)
	}
}

func TestNodeAddReplicaAsTailCopiesSnapshotAndActivates(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	sourceNode := mustNewNode(t, ctx, Config{NodeID: "node-a"}, sourceBackend, sourceCoord, transport)
	transport.Register("node-a", sourceBackend)

	if err := sourceNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := sourceNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(1, "alpha", "one", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	targetNode := mustNewNode(t, ctx, Config{NodeID: "node-b"}, targetBackend, targetCoord, transport)
	transport.Register("node-b", targetBackend)

	if err := targetNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "node-a"},
		},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}

	state := targetNode.State()
	if got, want := state.Replicas[1].State, ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if len(targetCoord.ReadySlots) != 0 {
		t.Fatalf("ready slots before activate = %v, want none", targetCoord.ReadySlots)
	}

	data, err := targetBackend.ReplicaData(1)
	if err != nil {
		t.Fatalf("target ReplicaData returned error: %v", err)
	}
	if got, want := snapshotValues(data), map[string]string{"alpha": "one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replica data = %v, want %v", got, want)
	}

	if err := targetNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}
	if got, want := targetCoord.ReadySlots, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready slots = %v, want %v", got, want)
	}
	if got, want := targetNode.State().Replicas[1].State, ReplicaStateActive; got != want {
		t.Fatalf("replica state after activate = %q, want %q", got, want)
	}
}

func TestActivateReplicaPreservesAssignmentUpdatesFromReadyCallback(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	callback := &updatingCoordinatorClient{}
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, callback, transport)
	callback.node = node

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}

	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	replica := node.State().Replicas[1]
	if got, want := replica.State, ReplicaStateActive; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if got, want := replica.Assignment.ChainVersion, uint64(4); got != want {
		t.Fatalf("chain version = %d, want %d", got, want)
	}
	if got, want := replica.Assignment.Role, ReplicaRoleHead; got != want {
		t.Fatalf("role = %q, want %q", got, want)
	}
}

func TestNodeAddReplicaAsTailFailsCleanlyWhenSourceUnavailable(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-b"}, backend, coord, transport)

	err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "missing"},
		},
	})
	if err == nil {
		t.Fatal("AddReplicaAsTail unexpectedly succeeded")
	}
	if !errors.Is(err, ErrSnapshotSourceUnavailable) {
		t.Fatalf("error = %v, want snapshot source unavailable", err)
	}
	if _, exists := node.State().Replicas[1]; exists {
		t.Fatal("replica still present after failed add")
	}
	if _, err := backend.ReplicaData(1); err == nil {
		t.Fatal("backend slot still present after failed add")
	}
}

func TestNodeInvalidLifecycleTransitionsFail(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err == nil {
		t.Fatal("ActivateReplica unexpectedly succeeded")
	} else if !errors.Is(err, ErrUnknownReplica) {
		t.Fatalf("error = %v, want unknown replica", err)
	}

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1}); err == nil {
		t.Fatal("MarkReplicaLeaving unexpectedly succeeded from catching_up")
	} else if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("error = %v, want invalid transition", err)
	}
}

func TestNodeDrainAndRemoveLifecycle(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := backend.Put(1, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if got, want := node.State().Replicas[1].State, ReplicaStateLeaving; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}

	if err := node.RemoveReplica(ctx, RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if got, want := coord.RemovedSlots, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removed slots = %v, want %v", got, want)
	}
	if _, exists := node.State().Replicas[1]; exists {
		t.Fatal("replica still present after remove")
	}
	if _, err := backend.ReplicaData(1); err == nil {
		t.Fatal("backend data still present after remove")
	}
}

func TestNodeUpdateChainPeersTargetsOnlyOneSlot(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	for slot := 1; slot <= 2; slot++ {
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{Slot: slot, ChainVersion: 1, Role: ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(slot=%d) returned error: %v", slot, err)
		}
		if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(slot=%d) returned error: %v", slot, err)
		}
	}

	if err := node.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "node-b"},
		},
	}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}

	state := node.State()
	if got, want := state.Replicas[1].Assignment.Peers.SuccessorNodeID, "node-b"; got != want {
		t.Fatalf("slot 1 successor = %q, want %q", got, want)
	}
	if got, want := state.Replicas[2].Assignment.Peers.SuccessorNodeID, ""; got != want {
		t.Fatalf("slot 2 successor = %q, want %q", got, want)
	}
}

func TestNodeReportHeartbeatSummarizesReplicas(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleTail},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(second) returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := node.ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	if got, want := len(coord.Heartbeats), 1; got != want {
		t.Fatalf("heartbeat count = %d, want %d", got, want)
	}
	if got, want := coord.Heartbeats[0], (NodeStatus{
		NodeID:          "node-a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
}

type updatingCoordinatorClient struct {
	node *Node
}

func (c *updatingCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int) error {
	return c.node.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 4,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "node-b"},
		},
	})
}

func (c *updatingCoordinatorClient) ReportReplicaRemoved(context.Context, int) error {
	return nil
}

func (c *updatingCoordinatorClient) ReportNodeRecovered(context.Context, NodeRecoveryReport) error {
	return nil
}

func (c *updatingCoordinatorClient) ReportNodeHeartbeat(context.Context, NodeStatus) error {
	return nil
}

func TestEndToEndDrainFlowAcrossNodesWithoutNetworking(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	headBackend := NewInMemoryBackend()
	headCoord := NewInMemoryCoordinatorClient()
	headNode := mustNewNode(t, ctx, Config{NodeID: "head"}, headBackend, headCoord, transport)
	transport.Register("head", headBackend)

	tailBackend := NewInMemoryBackend()
	tailCoord := NewInMemoryCoordinatorClient()
	tailNode := mustNewNode(t, ctx, Config{NodeID: "tail"}, tailBackend, tailCoord, transport)
	transport.Register("tail", tailBackend)

	if err := headNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 7, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("head AddReplicaAsTail returned error: %v", err)
	}
	if err := headNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("head ActivateReplica returned error: %v", err)
	}
	if err := headBackend.Put(7, "order-1", "committed", testObjectMetadata(1)); err != nil {
		t.Fatalf("head Put returned error: %v", err)
	}

	if err := tailNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         7,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "head"},
		},
	}); err != nil {
		t.Fatalf("tail AddReplicaAsTail returned error: %v", err)
	}
	if err := tailNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("tail ActivateReplica returned error: %v", err)
	}
	if err := headNode.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         7,
			ChainVersion: 2,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		},
	}); err != nil {
		t.Fatalf("head UpdateChainPeers returned error: %v", err)
	}
	if err := headNode.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 7}); err != nil {
		t.Fatalf("head MarkReplicaLeaving returned error: %v", err)
	}
	if err := headNode.RemoveReplica(ctx, RemoveReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("head RemoveReplica returned error: %v", err)
	}

	tailData, err := tailBackend.ReplicaData(7)
	if err != nil {
		t.Fatalf("tail ReplicaData returned error: %v", err)
	}
	if got, want := snapshotValues(tailData), map[string]string{"order-1": "committed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail data = %v, want %v", got, want)
	}
	if got, want := tailCoord.ReadySlots, []int{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail ready slots = %v, want %v", got, want)
	}
	if got, want := headCoord.RemovedSlots, []int{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("head removed slots = %v, want %v", got, want)
	}
}

func TestMultiSlotFailureIsolation(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, sourceCoord, transport)
	transport.Register("source", sourceBackend)

	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(2, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	target := mustNewNode(t, ctx, Config{NodeID: "target"}, targetBackend, targetCoord, transport)

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "missing"},
		},
	}); err == nil {
		t.Fatal("AddReplicaAsTail(slot=1) unexpectedly succeeded")
	}

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(slot=2) returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("ActivateReplica(slot=2) returned error: %v", err)
	}

	state := target.State()
	if _, exists := state.Replicas[1]; exists {
		t.Fatal("failed slot 1 should not remain present")
	}
	if got, want := state.Replicas[2].State, ReplicaStateActive; got != want {
		t.Fatalf("slot 2 state = %q, want %q", got, want)
	}
}

func TestDeterministicRepeatedCommandStream(t *testing.T) {
	leftState, leftReady, leftRemoved, leftData := runDeterministicFlow(t)
	rightState, rightReady, rightRemoved, rightData := runDeterministicFlow(t)

	if !reflect.DeepEqual(leftState, rightState) {
		t.Fatalf("node state mismatch\nleft=%#v\nright=%#v", leftState, rightState)
	}
	if !reflect.DeepEqual(leftReady, rightReady) {
		t.Fatalf("ready reports mismatch\nleft=%v\nright=%v", leftReady, rightReady)
	}
	if !reflect.DeepEqual(leftRemoved, rightRemoved) {
		t.Fatalf("removed reports mismatch\nleft=%v\nright=%v", leftRemoved, rightRemoved)
	}
	if !reflect.DeepEqual(leftData, rightData) {
		t.Fatalf("backend data mismatch\nleft=%v\nright=%v", leftData, rightData)
	}
}

func runDeterministicFlow(t *testing.T) (NodeState, []int, []int, Snapshot) {
	t.Helper()

	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, sourceCoord, transport)
	transport.Register("source", sourceBackend)

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	target := mustNewNode(t, ctx, Config{NodeID: "target"}, targetBackend, targetCoord, transport)
	transport.Register("target", targetBackend)

	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 9, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(9, "a", "1", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         9,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}
	if err := target.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         9,
			ChainVersion: 3,
			Role:         ReplicaRoleSingle,
		},
	}); err != nil {
		t.Fatalf("target UpdateChainPeers returned error: %v", err)
	}

	data, err := targetBackend.ReplicaData(9)
	if err != nil {
		t.Fatalf("target ReplicaData returned error: %v", err)
	}
	return target.State(), append([]int(nil), targetCoord.ReadySlots...), append([]int(nil), targetCoord.RemovedSlots...), data
}

func mustNewNode(t *testing.T, ctx context.Context, cfg Config, backend Backend, coord CoordinatorClient, repl ReplicationTransport) *Node {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = &fakeClock{now: time.Unix(0, 0).UTC()}
	}
	node, err := NewNode(ctx, cfg, backend, coord, repl)
	if err != nil {
		t.Fatalf("NewNode returned error: %v", err)
	}
	return node
}
