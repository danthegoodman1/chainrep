package coordserver

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestHAFailoverRepairHistoryIsDeterministic(t *testing.T) {
	left := runHAFailoverRepairHistory(t)
	right := runHAFailoverRepairHistory(t)

	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(left.pending, right.pending) {
		t.Fatalf("pending mismatch\nleft=%#v\nright=%#v", left.pending, right.pending)
	}
	if !reflect.DeepEqual(left.routing, right.routing) {
		t.Fatalf("routing mismatch\nleft=%#v\nright=%#v", left.routing, right.routing)
	}
	if !reflect.DeepEqual(left.slotSnapshot, right.slotSnapshot) {
		t.Fatalf("slot snapshot mismatch\nleft=%#v\nright=%#v", left.slotSnapshot, right.slotSnapshot)
	}
}

func TestHAFailoverAcceptsInFlightProgressFromPriorEpoch(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["d"].EnableQueuedProgress()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	h.mustStepLeader(t)

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued ready progress = %d, want %d", got, want)
	}
	pendingBefore := h.leader.Pending()[1]
	if pendingBefore.Epoch == 0 {
		t.Fatal("pending epoch before failover = 0, want active epoch")
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	pendingAfter := h.standby.Pending()[1]
	if got, want := pendingAfter.Epoch, pendingBefore.Epoch; got != want {
		t.Fatalf("pending epoch after failover = %d, want %d", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := h.standby.Pending()[1].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after delivered ready = %q, want %q", got, want)
	}
}

func TestHADuplicateAddTailDispatchAfterFailoverIsSafe(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	entry := h.mustFindOutbox(t, OutboxCommandAddReplicaAsTail, "d", 1)
	if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("manual dispatchOutboxEntry(add tail) returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	replica := h.adapters["d"].Node().State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state after replayed add tail = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[1].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after replayed add tail = %q, want %q", got, want)
	}
}

func TestHADuplicateMarkLeavingDispatchAfterFailoverIsSafe(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	h.mustStepLeader(t)
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	entry := h.mustFindOutbox(t, OutboxCommandMarkReplicaLeaving, "c", 1)
	if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("manual dispatchOutboxEntry(mark leaving) returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	replica := h.adapters["c"].Node().State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateLeaving; got != want {
		t.Fatalf("replica state after replayed mark leaving = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[1].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after replayed mark leaving = %q, want %q", got, want)
	}
}

func TestHARestartResumesUndispatchedOutboxWork(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if err := h.leader.Close(); err != nil {
		t.Fatalf("leader.Close returned error: %v", err)
	}
	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	if got, want := h.standby.Pending()[1].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after standby resume = %q, want %q", got, want)
	}
	replica := h.adapters["d"].Node().State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state after standby resume = %q, want %q", got, want)
	}
}

func TestHADynamicAutoJoinRepairsAfterFailover(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 1, 3, []string{"a", "b", "c"})

	current := h.leader.Current()
	if _, err := h.leader.MarkNodeDead(ctx, reconfigureCommand("dead-c", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "c",
	}, coordinator.ReconfigurationPolicy{})); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if got, want := replicaNodeStates(h.leader.Current().Cluster.Chains[0]), []string{"a:active", "b:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("degraded chain after dead mark = %v, want %v", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	h.mustStepStandby(t)
	if got, want := h.standby.Pending()[0].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after dynamic join = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[0].NodeID, "d"; got != want {
		t.Fatalf("pending node after dynamic join = %q, want %q", got, want)
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := replicaNodeStates(h.standby.Current().Cluster.Chains[0]), []string{"a:active", "b:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final repaired chain after dynamic join = %v, want %v", got, want)
	}
}

func TestPostgresHAStoreConcurrentAcquireAndStaleSave(t *testing.T) {
	dsn := postgresTestDSN(t)
	if dsn == "" {
		t.Skip("CHAINREP_TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()
	storeA, err := OpenPostgresHAStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresHAStore(storeA) returned error: %v", err)
	}
	defer func() { _ = storeA.Close() }()
	if err := storeA.Reset(ctx); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	storeB, err := OpenPostgresHAStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresHAStore(storeB) returned error: %v", err)
	}
	defer func() { _ = storeB.Close() }()

	now := time.Unix(100, 0).UTC()
	type acquireResult struct {
		lease  LeaderLease
		leader bool
		err    error
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var left, right acquireResult
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		left.lease, left.leader, left.err = storeA.AcquireOrRenew(ctx, "coord-a", "leader-a", now, 2*time.Second)
	}()
	go func() {
		defer wg.Done()
		<-start
		right.lease, right.leader, right.err = storeB.AcquireOrRenew(ctx, "coord-b", "leader-b", now, 2*time.Second)
	}()
	close(start)
	wg.Wait()

	if left.err != nil {
		t.Fatalf("storeA AcquireOrRenew returned error: %v", left.err)
	}
	if right.err != nil {
		t.Fatalf("storeB AcquireOrRenew returned error: %v", right.err)
	}
	leaderCount := 0
	if left.leader {
		leaderCount++
	}
	if right.leader {
		leaderCount++
	}
	if got, want := leaderCount, 1; got != want {
		t.Fatalf("leader count = %d, want %d", got, want)
	}

	leaderStore := storeA
	leaderLease := left.lease
	staleStore := storeB
	staleLease := right.lease
	if !left.leader {
		leaderStore, staleStore = storeB, storeA
		leaderLease, staleLease = right.lease, left.lease
	}

	version, err := leaderStore.SaveSnapshot(ctx, leaderLease, now.Add(100*time.Millisecond), 0, HASnapshot{
		Outbox: []OutboxEntry{{
			ID:        "outbox-1",
			Epoch:     leaderLease.Epoch,
			NodeID:    "n1",
			Slot:      1,
			CommandID: "cmd-1",
			Kind:      OutboxCommandAddReplicaAsTail,
		}},
	})
	if err != nil {
		t.Fatalf("leader SaveSnapshot returned error: %v", err)
	}
	if got, want := version, uint64(1); got != want {
		t.Fatalf("snapshot version = %d, want %d", got, want)
	}
	if _, err := staleStore.SaveSnapshot(ctx, staleLease, now.Add(200*time.Millisecond), version, HASnapshot{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale SaveSnapshot error = %v, want ErrNotLeader", err)
	}
}

type haHistoryResult struct {
	finalState   coordruntime.State
	pending      map[int]PendingWork
	routing      RoutingSnapshot
	slotSnapshot map[string]map[string]string
}

type haInMemoryHarness struct {
	clock    *fakeClock
	store    *InMemoryHAStore
	repl     *storage.InMemoryReplicationTransport
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
	leader   *Server
	standby  *Server
}

func newHAInMemoryHarness(t *testing.T, nodeIDs []string) *haInMemoryHarness {
	t.Helper()

	clock := &fakeClock{now: time.Unix(0, 0).UTC()}
	store := NewInMemoryHAStore()
	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
	}

	newServer := func(coordinatorID, advertise string) *Server {
		return mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodeClients, ServerConfig{
			Clock: clock,
			HA: &HAConfig{
				CoordinatorID:          coordinatorID,
				AdvertiseAddress:       advertise,
				Store:                  store,
				LeaseTTL:               2 * time.Second,
				RenewInterval:          time.Second,
				DisableBackgroundLoops: true,
			},
		})
	}
	h := &haInMemoryHarness{
		clock:    clock,
		store:    store,
		repl:     repl,
		adapters: adapters,
		backends: backends,
		leader:   newServer("coord-a", "coord-a"),
		standby:  newServer("coord-b", "coord-b"),
	}
	t.Cleanup(func() {
		_ = h.leader.Close()
		_ = h.standby.Close()
	})
	return h
}

func (h *haInMemoryHarness) mustStepLeader(t *testing.T) {
	t.Helper()
	isLeader, err := h.leader.StepHA(context.Background())
	if err != nil {
		t.Fatalf("leader StepHA returned error: %v", err)
	}
	if !isLeader {
		t.Fatal("leader did not hold lease")
	}
}

func (h *haInMemoryHarness) mustStepStandby(t *testing.T) {
	t.Helper()
	isLeader, err := h.standby.StepHA(context.Background())
	if err != nil {
		t.Fatalf("standby StepHA returned error: %v", err)
	}
	if !isLeader {
		t.Fatal("standby did not hold lease")
	}
}

func (h *haInMemoryHarness) mustBind(t *testing.T, server *Server) {
	t.Helper()
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
}

func (h *haInMemoryHarness) seedBootstrap(t *testing.T, server *Server, slotCount int, replicationFactor int, nodeIDs []string) {
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
			assignment, err := assignmentForNode(chain, state.NodesByID, replica.NodeID, 1)
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
			if err := h.backends[replica.NodeID].Put(chain.Slot, "seed", replica.NodeID, storage.ObjectMetadata{Version: 1}); err != nil {
				t.Fatalf("seed Put returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
}

func (h *haInMemoryHarness) mustFindOutbox(t *testing.T, kind OutboxCommandKind, nodeID string, slot int) OutboxEntry {
	t.Helper()
	snapshot, err := h.store.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}
	for _, entry := range snapshot.Outbox {
		if entry.Kind == kind && entry.NodeID == nodeID && entry.Slot == slot {
			return entry
		}
	}
	t.Fatalf("outbox entry %q node=%s slot=%d not found", kind, nodeID, slot)
	return OutboxEntry{}
}

func runHAFailoverRepairHistory(t *testing.T) haHistoryResult {
	t.Helper()
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()
	h.adapters["d"].EnableQueuedProgress()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.standby.StepHA(ctx); err != nil {
		t.Fatalf("standby initial StepHA returned error: %v", err)
	}
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	h.mustStepLeader(t)
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(ready) returned error: %v", err)
	}
	h.mustStepStandby(t)
	if err := h.adapters["c"].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if err := h.adapters["c"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}

	routing, err := h.leader.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	snapshot, err := h.backends["d"].ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData(d,1) returned error: %v", err)
	}
	return haHistoryResult{
		finalState:   h.leader.Current(),
		pending:      h.leader.Pending(),
		routing:      routing,
		slotSnapshot: map[string]map[string]string{"d": storageSnapshotValues(snapshot)},
	}
}

func uniqueAddNodeEvent(nodeID string) coordinator.Event {
	return coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode(nodeID),
	}
}

func noBudgetPolicy() coordinator.ReconfigurationPolicy {
	return coordinator.ReconfigurationPolicy{MaxChangedChains: 1}
}

func postgresTestDSN(t *testing.T) string {
	t.Helper()
	return os.Getenv("CHAINREP_TEST_POSTGRES_DSN")
}
