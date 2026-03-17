package coordserver

import (
	"context"
	"reflect"
	"testing"
	"time"

	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestHeartbeatPersistsHealthyLiveness(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{"a": newRecordingNodeClient("a")}
	clock := &fakeClock{now: time.Unix(0, 100)}
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})

	status := storage.NodeStatus{NodeID: "a", ReplicaCount: 2, ActiveCount: 1}
	if err := server.ReportNodeHeartbeat(ctx, status); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	if got, want := server.Heartbeats()["a"], status; !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
	record := server.Liveness()["a"]
	if got, want := record.State, coordruntime.NodeLivenessStateHealthy; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got, want := record.LastHeartbeatUnixNano, clock.now.UnixNano(); got != want {
		t.Fatalf("last heartbeat = %d, want %d", got, want)
	}
}

func TestEvaluateLivenessTransitionsSuspectThenHealthyWithoutRoutingChange(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	server := mustBootstrappedServerWithConfig(
		t,
		ctx,
		mapToClient(map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
		}),
		ServerConfig{
			LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
			Clock:          clock,
		},
		4,
		3,
		"a", "b", "c",
	)
	before, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateSuspect; got != want {
		t.Fatalf("suspect state = %q, want %q", got, want)
	}
	afterSuspect, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after suspect returned error: %v", err)
	}
	if !reflect.DeepEqual(before.Slots, afterSuspect.Slots) {
		t.Fatalf("routing slots changed on suspect\nbefore=%#v\nafter=%#v", before.Slots, afterSuspect.Slots)
	}

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat after suspect returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateHealthy; got != want {
		t.Fatalf("state after recovery heartbeat = %q, want %q", got, want)
	}
}

func TestDeadTransitionTriggersRepairAndDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("dead state = %q, want %q", got, want)
	}
	if got, want := server.Pending()[0], (PendingWork{Slot: 0, NodeID: "d", Kind: pendingKindReady}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending repair = %#v, want %#v", got, want)
	}

	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("second EvaluateLiveness returned error: %v", err)
	}
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat after dead returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("dead state after later heartbeat = %q, want %q", got, want)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
}

func TestDeadTransitionRepairCompletesWithDelayedQueuedProgress(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	h.adapters["d"].EnableQueuedProgress()
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued progress = %d, want %d", got, want)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:joining"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chain before delayed ready delivery = %v, want %v", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain after delayed ready delivery = %v, want %v", got, want)
	}
}

func TestLivenessRecoveryAfterCoordinatorReopenDoesNotDuplicateDeadAction(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	clock := &fakeClock{now: time.Unix(0, 0)}
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	initialCalls := append([]string(nil), nodes["d"].calls...)

	reopened := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	clock.Advance(1 * time.Second)
	if err := reopened.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("reopened EvaluateLiveness returned error: %v", err)
	}
	if got, want := nodes["d"].calls, initialCalls; !reflect.DeepEqual(got, want) {
		t.Fatalf("calls after reopen = %v, want %v", got, want)
	}
	if got, want := reopened.Liveness()["b"].DeadActionFired, true; got != want {
		t.Fatalf("dead action fired = %t, want %t", got, want)
	}
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func mustOpenServerWithConfig(
	t *testing.T,
	store coordruntime.Store,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
) *Server {
	t.Helper()
	server, err := OpenWithConfig(context.Background(), store, nodes, cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	return server
}

func mustBootstrappedServerWithConfig(
	t *testing.T,
	ctx context.Context,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodes, cfg)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}

func newInMemoryHarnessWithConfig(t *testing.T, nodeIDs []string, cfg ServerConfig) *inMemoryHarness {
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
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodeClients, cfg)
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
