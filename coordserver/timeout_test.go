package coordserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestAddNodeDispatchTimeoutReturnsBoundedErrorAndNoPendingWork(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["d"].blockAddTail = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{DispatchTimeout: time.Nanosecond}, 8, 3, "a", "b", "c")

	_, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if got := server.Pending(); len(got) != 0 {
		t.Fatalf("pending work = %#v, want empty", got)
	}
}

func TestBeginDrainUpdatePeersTimeoutDoesNotFabricateCoordinatorAdvancement(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["a"].blockUpdatePeers = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{DispatchTimeout: time.Nanosecond}, 1, 3, "a", "b", "c", "d")

	_, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindBeginDrainNode,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{}))
	if err == nil {
		t.Fatal("BeginDrainNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending work = %#v, want %#v", got, want)
	}
	for _, replica := range server.Current().Cluster.Chains[0].Replicas {
		if replica.State == coordinator.ReplicaStateLeaving {
			t.Fatalf("coordinator state unexpectedly advanced replica to leaving: %v", replicaNodeStates(server.Current().Cluster.Chains[0]))
		}
	}
}

func TestRecoveryCommandTimeoutLeavesReplicaUnavailable(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
	}
	server := mustOpenServerWithConfig(t, store, mapBlockingToClient(nodes), ServerConfig{
		RecoveryCommandTimeout: time.Nanosecond,
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	current := server.Current()
	assignment, ok := currentAssignmentForNode(current, current.Cluster.Chains[0].Replicas[0].NodeID, 0)
	if !ok {
		t.Fatal("failed to find current assignment for recovered timeout test")
	}
	recoveringNodeID := current.Cluster.Chains[0].Replicas[0].NodeID
	nodes[recoveringNodeID].blockResume = true

	err := server.ReportNodeRecovered(ctx, storage.NodeRecoveryReport{
		NodeID: recoveringNodeID,
		Replicas: []storage.RecoveredReplica{{
			Assignment:               assignment,
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 0,
			HasCommittedData:         true,
		}},
	})
	if err == nil {
		t.Fatal("ReportNodeRecovered unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	snapshot, snapErr := server.RoutingSnapshot(ctx)
	if snapErr != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", snapErr)
	}
	if snapshot.Slots[0].Writable || snapshot.Slots[0].Readable {
		t.Fatalf("routing slot = %#v, want unavailable while recovery timed out", snapshot.Slots[0])
	}
}

func TestLivenessTriggeredDeadRepairRespectsDispatchTimeout(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["d"].blockAddTail = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{
		Clock:           clock,
		DispatchTimeout: time.Nanosecond,
		LivenessPolicy:  LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
	}, 1, 3, "a", "b", "c", "d")

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err == nil {
		t.Fatal("EvaluateLiveness unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrDispatchTimeout) {
			t.Fatalf("error = %v, want dispatch timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got := server.Pending(); len(got) != 0 {
		t.Fatalf("pending work = %#v, want empty", got)
	}
}

type blockingNodeClient struct {
	nodeID           string
	blockAddTail     bool
	blockUpdatePeers bool
	blockMarkLeaving bool
	blockResume      bool
	blockRecover     bool
	blockDrop        bool
}

func newBlockingNodeClient(nodeID string) *blockingNodeClient {
	return &blockingNodeClient{nodeID: nodeID}
}

func (c *blockingNodeClient) AddReplicaAsTail(ctx context.Context, _ storage.AddReplicaAsTailCommand) error {
	if c.blockAddTail {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) ActivateReplica(context.Context, storage.ActivateReplicaCommand) error { return nil }

func (c *blockingNodeClient) MarkReplicaLeaving(ctx context.Context, _ storage.MarkReplicaLeavingCommand) error {
	if c.blockMarkLeaving {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) RemoveReplica(context.Context, storage.RemoveReplicaCommand) error { return nil }

func (c *blockingNodeClient) UpdateChainPeers(ctx context.Context, _ storage.UpdateChainPeersCommand) error {
	if c.blockUpdatePeers {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) ResumeRecoveredReplica(ctx context.Context, _ storage.ResumeRecoveredReplicaCommand) error {
	if c.blockResume {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) RecoverReplica(ctx context.Context, _ storage.RecoverReplicaCommand) error {
	if c.blockRecover {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) DropRecoveredReplica(ctx context.Context, _ storage.DropRecoveredReplicaCommand) error {
	if c.blockDrop {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func mapBlockingToClient(nodes map[string]*blockingNodeClient) map[string]StorageNodeClient {
	cloned := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		cloned[nodeID] = node
	}
	return cloned
}

func mustBootstrappedBlockingServer(
	t *testing.T,
	ctx context.Context,
	nodes map[string]*blockingNodeClient,
	cfg ServerConfig,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), mapBlockingToClient(nodes), cfg)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}
