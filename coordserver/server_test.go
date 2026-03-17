package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestBootstrapCreatesCoordinatorStateAndDispatchesNothing(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}
	server := mustOpenServer(t, mapToClient(nodes))

	state, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := state.Cluster.SlotCount, 8; got != want {
		t.Fatalf("slot count = %d, want %d", got, want)
	}
	for nodeID, node := range nodes {
		if len(node.calls) != 0 {
			t.Fatalf("node %q received commands during bootstrap: %v", nodeID, node.calls)
		}
	}
}

func TestBootstrapFailsCleanlyOnInvalidConfig(t *testing.T) {
	server := mustOpenServer(t, nil)
	_, err := server.Bootstrap(context.Background(), coordruntime.Command{
		ID:              "bootstrap-bad",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 0, ReplicationFactor: 1},
			Nodes:  uniqueNodes("a"),
		},
	})
	if err == nil {
		t.Fatal("Bootstrap unexpectedly succeeded")
	}
	if !errors.Is(err, coordinator.ErrInvalidConfig) {
		t.Fatalf("error = %v, want invalid config", err)
	}
}

func TestAddNodeDispatchesAddReplicaAsTailToExpectedNode(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	state, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if got, want := state.Version, uint64(2); got != want {
		t.Fatalf("version = %d, want %d", got, want)
	}
	if got, want := server.Pending()[1], (PendingWork{Slot: 1, NodeID: "d", Kind: pendingKindReady}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending[1] = %#v, want %#v", got, want)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
	if got, want := nodes["c"].calls, []string{"update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node c calls = %v, want %v", got, want)
	}
}

func TestBeginDrainDispatchesReplacementTailBeforeRemoval(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 3, "a", "b", "c", "d")

	_, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindBeginDrainNode,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{}))
	if err != nil {
		t.Fatalf("BeginDrainNode returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
	if containsCall(nodes["b"].calls, "mark_leaving:0") {
		t.Fatalf("node b should not be marked leaving before replacement is ready: %v", nodes["b"].calls)
	}
}

func TestMarkNodeDeadDispatchesMandatoryRepairEvenWithZeroBudget(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 3, "a", "b", "c", "d")

	_, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 0}))
	if err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
}

func TestValidReadyAndRemovedProgressAdvanceStateAndDispatchNextStep(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if _, err := server.ReportReplicaReady(ctx, "d", 1, "ready-1"); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:1", "update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls after ready = %v, want %v", got, want)
	}
	if got, want := nodes["c"].calls, []string{"update_peers:1", "mark_leaving:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node c calls after ready = %v, want %v", got, want)
	}
	if got, want := server.Pending()[1], (PendingWork{Slot: 1, NodeID: "c", Kind: pendingKindRemoved}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending[1] after ready = %#v, want %#v", got, want)
	}

	if _, err := server.ReportReplicaRemoved(ctx, "c", 1, "removed-1"); err != nil {
		t.Fatalf("ReportReplicaRemoved returned error: %v", err)
	}
	if _, exists := server.Pending()[1]; exists {
		t.Fatal("pending work still present after removal")
	}
}

func TestUnexpectedAndDuplicateProgressAreRejected(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	if _, err := server.ReportReplicaReady(ctx, "d", 1, "ready-1"); err == nil {
		t.Fatal("ReportReplicaReady unexpectedly succeeded")
	} else if !errors.Is(err, ErrUnexpectedProgress) {
		t.Fatalf("error = %v, want unexpected progress", err)
	}

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if _, err := server.ReportReplicaReady(ctx, "d", 1, "ready-1"); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	if _, err := server.ReportReplicaReady(ctx, "d", 1, "ready-1"); err == nil {
		t.Fatal("duplicate ReportReplicaReady unexpectedly succeeded")
	} else if !errors.Is(err, ErrUnexpectedProgress) {
		t.Fatalf("error = %v, want unexpected progress", err)
	}
}

func TestUnknownTargetNodeAndCommandFailuresAreSurfaced(t *testing.T) {
	ctx := context.Background()
	server := mustBootstrappedServer(t, ctx, mapToClient(map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}), 8, 3, "a", "b", "c")

	_, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("error = %v, want unknown node", err)
	}

	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	nodes["d"].addTailErr = errors.New("boom")
	server = mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")
	_, err = server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("error = %v, want dispatch failed", err)
	}
	if _, exists := server.Pending()[1]; exists {
		t.Fatal("pending work should not exist after failed AddReplicaAsTail dispatch")
	}
}

func TestHeartbeatIsRecordedButDoesNotTriggerReconfiguration(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
	}
	server := mustOpenServer(t, mapToClient(nodes))

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	if got, want := server.Heartbeats()["a"], (storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
}

func TestEndToEndAddNodeFlowWithInMemoryNodes(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	server := h.server

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("adapter ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("adapter RemoveReplica returned error: %v", err)
	}

	final := server.Current()
	if got, want := replicaNodeStates(final.Cluster.Chains[1]), []string{"d:active", "b:active", "a:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
	data, err := h.backends["d"].ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData returned error: %v", err)
	}
	if !reflect.DeepEqual(data, storage.Snapshot{"seed-1": "value-1"}) {
		t.Fatalf("replica data = %v, want seed copy", data)
	}
}

func TestEndToEndDrainAndDeadFlowsWithInMemoryNodes(t *testing.T) {
	ctx := context.Background()
	t.Run("drain", func(t *testing.T) {
		h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})

		if _, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
			Kind:   coordinator.EventKindBeginDrainNode,
			NodeID: "b",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("BeginDrainNode returned error: %v", err)
		}
		if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica returned error: %v", err)
		}
		if err := h.adapters["b"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("RemoveReplica returned error: %v", err)
		}
		if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("final chain = %v, want %v", got, want)
		}
	})

	t.Run("dead", func(t *testing.T) {
		h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})

		if _, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-b", 1, coordinator.Event{
			Kind:   coordinator.EventKindMarkNodeDead,
			NodeID: "b",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("MarkNodeDead returned error: %v", err)
		}
		if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica returned error: %v", err)
		}
		if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("final chain = %v, want %v", got, want)
		}
	})
}

func TestDeterministicRepeatedHistory(t *testing.T) {
	left := runServerHistory(t)
	right := runServerHistory(t)
	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(left.heartbeats, right.heartbeats) {
		t.Fatalf("heartbeat mismatch\nleft=%#v\nright=%#v", left.heartbeats, right.heartbeats)
	}
	if !reflect.DeepEqual(left.pending, right.pending) {
		t.Fatalf("pending mismatch\nleft=%#v\nright=%#v", left.pending, right.pending)
	}
}

type recordingNodeClient struct {
	nodeID           string
	calls            []string
	addTailErr       error
	updatePeersErr   error
	markLeavingErr   error
	removeReplicaErr error
}

func newRecordingNodeClient(nodeID string) *recordingNodeClient {
	return &recordingNodeClient{nodeID: nodeID}
}

func (r *recordingNodeClient) AddReplicaAsTail(_ context.Context, cmd storage.AddReplicaAsTailCommand) error {
	if r.addTailErr != nil {
		return r.addTailErr
	}
	r.calls = append(r.calls, fmt.Sprintf("add_tail:%d", cmd.Assignment.Slot))
	return nil
}

func (r *recordingNodeClient) ActivateReplica(_ context.Context, cmd storage.ActivateReplicaCommand) error {
	r.calls = append(r.calls, fmt.Sprintf("activate:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) MarkReplicaLeaving(_ context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	if r.markLeavingErr != nil {
		return r.markLeavingErr
	}
	r.calls = append(r.calls, fmt.Sprintf("mark_leaving:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) RemoveReplica(_ context.Context, cmd storage.RemoveReplicaCommand) error {
	if r.removeReplicaErr != nil {
		return r.removeReplicaErr
	}
	r.calls = append(r.calls, fmt.Sprintf("remove:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) UpdateChainPeers(_ context.Context, cmd storage.UpdateChainPeersCommand) error {
	if r.updatePeersErr != nil {
		return r.updatePeersErr
	}
	r.calls = append(r.calls, fmt.Sprintf("update_peers:%d", cmd.Assignment.Slot))
	return nil
}

type inMemoryHarness struct {
	server   *Server
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
}

func newInMemoryHarness(t *testing.T, nodeIDs []string) *inMemoryHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
	}
	server := mustOpenServer(t, nodeClients)
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	for _, adapter := range adapters {
		adapter.BindServer(nil)
	}
	return &inMemoryHarness{
		server:   server,
		adapters: adapters,
		backends: backends,
	}
}

func (h *inMemoryHarness) seedBootstrap(t *testing.T, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	state, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         slotCount,
		ReplicationFactor: replicationFactor,
	}, uniqueNodes(nodeIDs...))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assignment, err := assignmentForNode(chain, replica.NodeID, 1)
			if err != nil {
				t.Fatalf("assignmentForNode returned error: %v", err)
			}
			adapter := h.adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
			if err := h.backends[replica.NodeID].Put(chain.Slot, fmt.Sprintf("seed-%d", chain.Slot), fmt.Sprintf("value-%d", chain.Slot)); err != nil {
				t.Fatalf("seed Put returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}
}

type historyResult struct {
	finalState coordruntime.State
	heartbeats map[string]storage.NodeStatus
	pending    map[int]PendingWork
}

func runServerHistory(t *testing.T) historyResult {
	t.Helper()
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})
	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}
	return historyResult{
		finalState: server.Current(),
		heartbeats: server.Heartbeats(),
		pending:    server.Pending(),
	}
}

func mustOpenServer(t *testing.T, nodes map[string]StorageNodeClient) *Server {
	t.Helper()
	server, err := Open(context.Background(), coordruntime.NewInMemoryStore(), nodes)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return server
}

func mustBootstrappedServer(
	t *testing.T,
	ctx context.Context,
	nodes map[string]StorageNodeClient,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServer(t, nodes)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}

func bootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: uniqueNodes(nodeIDs...),
		},
	}
}

func reconfigureCommand(
	id string,
	expected uint64,
	event coordinator.Event,
	policy coordinator.ReconfigurationPolicy,
) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{event},
			Policy: policy,
		},
	}
}

func uniqueNodes(ids ...string) []coordinator.Node {
	nodes := make([]coordinator.Node, len(ids))
	for i, id := range ids {
		nodes[i] = uniqueNode(id)
	}
	return nodes
}

func uniqueNode(id string) coordinator.Node {
	return coordinator.Node{
		ID: id,
		FailureDomains: map[string]string{
			"host": "host-" + id,
			"rack": "rack-" + id,
			"az":   "az-" + id,
		},
	}
}

func replicaNodeStates(chain coordinator.Chain) []string {
	states := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		states = append(states, fmt.Sprintf("%s:%s", replica.NodeID, replica.State))
	}
	return states
}

func mapToClient(nodes map[string]*recordingNodeClient) map[string]StorageNodeClient {
	clients := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		clients[nodeID] = node
	}
	return clients
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

func TestDispatchOrderIsDeterministic(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")
	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	got := make([]string, 0, len(nodes))
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		for _, call := range nodes[nodeID].calls {
			got = append(got, fmt.Sprintf("%s:%s", nodeID, call))
		}
	}
	want := []string{"c:update_peers:1", "d:add_tail:1"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatched calls = %v, want %v", got, want)
	}
}
