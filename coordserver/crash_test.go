package coordserver

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

type durableCoordHarness struct {
	path     string
	repl     *storage.InMemoryReplicationTransport
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
}

func newDurableCoordHarness(t *testing.T, path string, nodeIDs []string) *durableCoordHarness {
	t.Helper()

	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
	}
	return &durableCoordHarness{
		path:     path,
		repl:     repl,
		adapters: adapters,
		backends: backends,
	}
}

func (h *durableCoordHarness) openServer(t *testing.T, clientIDs ...string) (*coordruntime.BadgerStore, *Server) {
	t.Helper()
	store, err := coordruntime.OpenBadgerStore(h.path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	clients := make(map[string]StorageNodeClient, len(clientIDs))
	for _, nodeID := range clientIDs {
		clients[nodeID] = h.adapters[nodeID]
	}
	server := mustOpenServerWithConfig(t, store, clients, ServerConfig{
		DispatchRetryInterval: time.Hour,
	})
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
	return store, server
}

func (h *durableCoordHarness) closeServer(t *testing.T, server *Server, store *coordruntime.BadgerStore) {
	t.Helper()
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}
}

func (h *durableCoordHarness) seedBootstrap(t *testing.T, server *Server, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	(&inMemoryHarness{
		server:   server,
		adapters: h.adapters,
		backends: h.backends,
	}).seedBootstrap(t, slotCount, replicationFactor, nodeIDs)
}

func TestNonHACrashBeforeFirstDispatchResumesOutboxAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err == nil {
		t.Fatal("AddNode unexpectedly succeeded without dispatch client for d")
	} else if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("AddNode error = %v, want ErrDispatchFailed", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after failed dispatch")
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := reopened.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after activation = %q, want %q", got, want)
	}
	if got, want := pendingKind(reopened.Current().PendingBySlot[slot].Kind), pendingKindRemoved; got != want {
		t.Fatalf("runtime pending kind after activation = %q, want %q", got, want)
	}
	leavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after replacement activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending for repaired slot after remove = %#v, want removal cleared", got)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-before-dispatch", "v1")
}

func TestNonHACrashAfterDispatchBeforeAckReplaysSafelyAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, _, err := server.rt.Reconfigure(ctx, reconfigureCommand("add-d", server.Current().Version, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("rt.Reconfigure returned error: %v", err)
	}
	server.syncViewsFromRuntime()
	server.rebuildRoutingSnapshot()
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)

	entry := mustFindRuntimeOutboxEntry(t, server, coordruntime.OutboxCommandKindAddReplicaAsTail, "d", slot)
	if err := server.dispatchRuntimeOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("dispatchRuntimeOutboxEntry(add tail) returned error: %v", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty before ack")
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after replayed add-tail repair")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after replayed add-tail repair = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-dispatch-before-ack", "v2")
}

func TestNonHACrashAfterReadyProgressBeforeMarkLeavingResumesRepairAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})
	h.adapters["d"].EnableQueuedProgress()

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := lastActiveReplicaNode(server.Current().Cluster.Chains[slot])
	if leavingNodeID == "" {
		t.Fatal("failed to determine active tail before ready progress")
	}
	delete(server.nodes, leavingNodeID)

	if err := h.adapters["d"].DeliverNextProgress(ctx); err == nil {
		t.Fatal("DeliverNextProgress unexpectedly succeeded without leaving-node client")
	} else if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("DeliverNextProgress error = %v, want ErrDispatchFailed", err)
	}
	if got, want := server.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after persisted ready progress = %q, want %q", got, want)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after failed mark-leaving dispatch")
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after resumed mark-leaving repair = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-ready-progress", "v3")
}

func TestNonHACrashAfterRemovedProgressPreservesSettledStateAndDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before removed progress")
	}
	h.adapters[leavingNodeID].EnableQueuedProgress()
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if err := h.adapters[leavingNodeID].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after removed-progress reopen = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-removed-progress", "v4")
}

func mustFindRuntimeOutboxEntry(
	t *testing.T,
	server *Server,
	kind coordruntime.OutboxCommandKind,
	nodeID string,
	slot int,
) coordruntime.OutboxEntry {
	t.Helper()
	for _, entry := range server.rt.Current().Outbox {
		if entry.Kind == kind && entry.NodeID == nodeID && entry.Slot == slot {
			return entry
		}
	}
	t.Fatalf("runtime outbox entry %q node=%s slot=%d not found", kind, nodeID, slot)
	return coordruntime.OutboxEntry{}
}

func replicaNodeWithState(chain coordinator.Chain, want coordinator.ReplicaState) string {
	for _, replica := range chain.Replicas {
		if replica.State == want {
			return replica.NodeID
		}
	}
	return ""
}

func lastActiveReplicaNode(chain coordinator.Chain) string {
	for i := len(chain.Replicas) - 1; i >= 0; i-- {
		if chain.Replicas[i].State == coordinator.ReplicaStateActive {
			return chain.Replicas[i].NodeID
		}
	}
	return ""
}

func mustPendingSlotForNode(t *testing.T, pending map[int]PendingWork, nodeID string, kind pendingKind) int {
	t.Helper()
	for slot, work := range pending {
		if work.NodeID == nodeID && work.Kind == kind {
			return slot
		}
	}
	t.Fatalf("pending work for node=%q kind=%q not found in %#v", nodeID, kind, pending)
	return 0
}

func assertActiveReplicaSet(t *testing.T, chain coordinator.Chain, wantNodeIDs ...string) {
	t.Helper()
	got := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateActive {
			got = append(got, replica.NodeID)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), wantNodeIDs...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active replica set = %v, want %v", got, want)
	}
}

func assertSlotRoundTrip(
	t *testing.T,
	ctx context.Context,
	server *Server,
	adapters map[string]*InMemoryNodeAdapter,
	slot int,
	key string,
	value string,
) {
	t.Helper()
	snapshot, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if slot < 0 || slot >= len(snapshot.Slots) {
		t.Fatalf("routing snapshot missing slot %d", slot)
	}
	route := snapshot.Slots[slot]
	if !route.Writable || !route.Readable {
		t.Fatalf("route flags for slot %d = %#v, want readable and writable", slot, route)
	}
	if route.HeadNodeID == "" || route.TailNodeID == "" {
		t.Fatalf("route head/tail for slot %d = %#v, want non-empty nodes", slot, route)
	}

	chainVersion := server.Current().SlotVersions[slot]
	if _, err := adapters[route.HeadNodeID].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 slot,
		Key:                  key,
		Value:                value,
		ExpectedChainVersion: chainVersion,
	}); err != nil {
		t.Fatalf("HandleClientPut(slot=%d, head=%q) returned error: %v", slot, route.HeadNodeID, err)
	}
	read, err := adapters[route.TailNodeID].Node().HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 slot,
		Key:                  key,
		ExpectedChainVersion: chainVersion,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(slot=%d, tail=%q) returned error: %v", slot, route.TailNodeID, err)
	}
	if !read.Found || read.Value != value {
		t.Fatalf("tail read result = %#v, want found value %q", read, value)
	}
}
