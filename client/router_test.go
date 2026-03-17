package client

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"testing"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestRouterHashesKeysDeterministicallyAndRoutesToExpectedEndpoints(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 2, 4, "route")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 1, HeadNodeID: "h1", TailNodeID: "t1", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := router.Get(ctx, key); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if _, err := router.Delete(ctx, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	if got, want := transport.putNodes, []string{"h2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}
	if got, want := transport.getNodes, []string{"t2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
	if got, want := transport.deleteNodes, []string{"h2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete nodes = %v, want %v", got, want)
	}
}

func TestRouterRefreshesOnceOnRoutingMismatchAndNotOnGenericFailure(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 4, "refresh")
	initial := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 1, HeadNodeID: "old-head", TailNodeID: "old-tail", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	refreshed := coordserver.RoutingSnapshot{
		Version:   2,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 2, HeadNodeID: "new-head", TailNodeID: "new-tail", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{initial, refreshed}}
	transport := &recordingTransport{
		putErrs: []error{
			&storage.RoutingMismatchError{
				Slot:                 1,
				ExpectedChainVersion: 1,
				CurrentChainVersion:  2,
				CurrentRole:          storage.ReplicaRoleHead,
				CurrentState:         storage.ReplicaStateActive,
				Reason:               storage.RoutingMismatchReasonWrongVersion,
			},
			nil,
		},
	}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := source.calls, 2; got != want {
		t.Fatalf("snapshot source calls = %d, want %d", got, want)
	}
	if got, want := transport.putNodes, []string{"old-head", "new-head"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}

	source = &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{initial}}
	transport = &recordingTransport{
		putErrs: []error{errors.New("boom")},
	}
	router = mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if _, err := router.Put(ctx, key, "v"); err == nil {
		t.Fatal("Put unexpectedly succeeded")
	}
	if got, want := source.calls, 1; got != want {
		t.Fatalf("snapshot source calls after generic failure = %d, want %d", got, want)
	}
}

func TestRouterSnapshotReturnsDefensiveCopy(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 0, 2, "copy")
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{
		{
			Version:   1,
			SlotCount: 2,
			Slots: []coordserver.SlotRoute{
				{Slot: 0, ChainVersion: 1, HeadNodeID: "head-0", TailNodeID: "tail-0", Writable: true, Readable: true},
				{Slot: 1, ChainVersion: 1, HeadNodeID: "head-1", TailNodeID: "tail-1", Writable: true, Readable: true},
			},
		},
	}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("Snapshot unexpectedly not loaded")
	}
	snapshot.Slots[0].HeadNodeID = "mutated-head"
	snapshot.Slots[0].TailNodeID = "mutated-tail"
	snapshot.Slots[0].Writable = false
	snapshot.Slots[0].Readable = false

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := transport.putNodes, []string{"head-0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}

	snapshot, ok = router.Snapshot()
	if !ok {
		t.Fatal("Snapshot unexpectedly not loaded on second read")
	}
	if got, want := snapshot.Slots[0].HeadNodeID, "head-0"; got != want {
		t.Fatalf("head node after external mutation = %q, want %q", got, want)
	}
	if got, want := snapshot.Slots[0].TailNodeID, "tail-0"; got != want {
		t.Fatalf("tail node after external mutation = %q, want %q", got, want)
	}
	if !snapshot.Slots[0].Writable || !snapshot.Slots[0].Readable {
		t.Fatalf("route after external mutation = %#v, want readable and writable", snapshot.Slots[0])
	}
}

func TestEndToEndRouterPutGetDeleteAndRefreshAfterReconfiguration(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c", "d"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 1, 8, "slot1")
	putResult, err := router.Put(ctx, key, "v1")
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := putResult, (storage.CommitResult{Slot: 1, Sequence: 1}); !reflect.DeepEqual(got, want) {
		t.Fatalf("put result = %#v, want %#v", got, want)
	}
	readResult, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got, want := readResult, (storage.ReadResult{Slot: 1, ChainVersion: 1, Found: true, Value: "v1"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("read result = %#v, want %#v", got, want)
	}
	assertChainValue(t, h, 1, key, "v1")

	if _, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v2"); err == nil {
		t.Fatal("Put during join unexpectedly succeeded")
	} else if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("Put during join error = %v, want no route", err)
	}
	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("router snapshot not loaded after stale write refresh")
	}
	if got, want := snapshot.Slots[1].ChainVersion, uint64(2); got != want {
		t.Fatalf("router chain version after stale write refresh = %d, want %d", got, want)
	}
	if snapshot.Slots[1].Writable {
		t.Fatalf("router slot during join = %#v, want not writable", snapshot.Slots[1])
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get during join returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value during join = %q, want %q", got, want)
	}

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after stale tail returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value after reconfiguration = %q, want %q", got, want)
	}
	snapshot, ok = router.Snapshot()
	if !ok {
		t.Fatal("router snapshot missing after stale read refresh")
	}
	if got, want := snapshot.Slots[1].ChainVersion, uint64(4); got != want {
		t.Fatalf("router chain version after stale read refresh = %d, want %d", got, want)
	}
	if !snapshot.Slots[1].Writable {
		t.Fatalf("router slot after reconfiguration = %#v, want writable", snapshot.Slots[1])
	}

	if _, err := router.Put(ctx, key, "v2"); err != nil {
		t.Fatalf("Put after repair returned error: %v", err)
	}

	if _, err := router.Delete(ctx, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if readResult.Found {
		t.Fatalf("read after delete = %#v, want not found", readResult)
	}
	assertChainValue(t, h, 1, key, "")
}

type scriptedSnapshotSource struct {
	snapshots []coordserver.RoutingSnapshot
	calls     int
}

func (s *scriptedSnapshotSource) RoutingSnapshot(_ context.Context) (coordserver.RoutingSnapshot, error) {
	index := s.calls
	if index >= len(s.snapshots) {
		index = len(s.snapshots) - 1
	}
	s.calls++
	return s.snapshots[index], nil
}

type recordingTransport struct {
	getNodes    []string
	putNodes    []string
	deleteNodes []string
	putErrs     []error
	putResults  []storage.CommitResult
}

func (t *recordingTransport) Get(_ context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	t.getNodes = append(t.getNodes, nodeID)
	return storage.ReadResult{
		Slot:         req.Slot,
		ChainVersion: req.ExpectedChainVersion,
	}, nil
}

func (t *recordingTransport) Put(_ context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	t.putNodes = append(t.putNodes, nodeID)
	if len(t.putErrs) > 0 {
		err := t.putErrs[0]
		t.putErrs = t.putErrs[1:]
		if err != nil {
			return storage.CommitResult{}, err
		}
	}
	if len(t.putResults) > 0 {
		result := t.putResults[0]
		t.putResults = t.putResults[1:]
		return result, nil
	}
	return storage.CommitResult{Slot: req.Slot, Sequence: 1}, nil
}

func (t *recordingTransport) Delete(_ context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	t.deleteNodes = append(t.deleteNodes, nodeID)
	return storage.CommitResult{Slot: req.Slot, Sequence: 1}, nil
}

type routerHarness struct {
	server    *coordserver.Server
	transport *InMemoryTransport
	adapters  map[string]*coordserver.InMemoryNodeAdapter
	backends  map[string]*storage.InMemoryBackend
}

func newRouterHarness(t *testing.T, nodeIDs []string) *routerHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	transport := NewInMemoryTransport()
	adapters := make(map[string]*coordserver.InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]coordserver.StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := coordserver.NewInMemoryNodeAdapter(nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
		transport.RegisterNode(nodeID, adapter.Node())
	}
	server, err := coordserver.Open(context.Background(), coordruntime.NewInMemoryStore(), nodeClients)
	if err != nil {
		t.Fatalf("coordserver.Open returned error: %v", err)
	}
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	return &routerHarness{
		server:    server,
		transport: transport,
		adapters:  adapters,
		backends:  backends,
	}
}

func (h *routerHarness) seedBootstrap(t *testing.T, slotCount int, replicationFactor int, nodeIDs []string) {
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
			assignment := assignmentForChainNode(chain, replica.NodeID, 1)
			adapter := h.adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}
}

func assignmentForChainNode(chain coordinator.Chain, nodeID string, chainVersion uint64) storage.ReplicaAssignment {
	position := -1
	for i, replica := range chain.Replicas {
		if replica.NodeID == nodeID {
			position = i
			break
		}
	}
	if position < 0 {
		panic("node not found in chain")
	}
	role := storage.ReplicaRoleMiddle
	switch len(chain.Replicas) {
	case 1:
		role = storage.ReplicaRoleSingle
	default:
		switch position {
		case 0:
			role = storage.ReplicaRoleHead
		case len(chain.Replicas) - 1:
			role = storage.ReplicaRoleTail
		}
	}
	assignment := storage.ReplicaAssignment{
		Slot:         chain.Slot,
		ChainVersion: chainVersion,
		Role:         role,
	}
	if position > 0 {
		assignment.Peers.PredecessorNodeID = chain.Replicas[position-1].NodeID
	}
	if position+1 < len(chain.Replicas) {
		assignment.Peers.SuccessorNodeID = chain.Replicas[position+1].NodeID
	}
	return assignment
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

func keyForSlot(t *testing.T, slot int, slotCount int, prefix string) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if int(crc32.ChecksumIEEE([]byte(key))%uint32(slotCount)) == slot {
			return key
		}
	}
	t.Fatalf("unable to find key for slot %d", slot)
	return ""
}

func assertChainValue(t *testing.T, h *routerHarness, slot int, key string, want string) {
	t.Helper()
	for nodeID, adapter := range h.adapters {
		state := adapter.Node().State()
		replica, ok := state.Replicas[slot]
		if !ok {
			continue
		}
		if replica.State != storage.ReplicaStateActive {
			continue
		}
		snapshot, err := adapter.Node().CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("CommittedSnapshot(%q, %d) returned error: %v", nodeID, slot, err)
		}
		got := snapshot[key]
		if want == "" {
			if _, exists := snapshot[key]; exists {
				t.Fatalf("node %q slot %d has value %q, want missing", nodeID, slot, got)
			}
			continue
		}
		if got != want {
			t.Fatalf("node %q slot %d value = %q, want %q", nodeID, slot, got, want)
		}
	}
}

func mustNewRouter(t *testing.T, source SnapshotSource, transport Transport) *Router {
	t.Helper()
	router, err := NewRouter(source, transport)
	if err != nil {
		t.Fatalf("NewRouter returned error: %v", err)
	}
	return router
}
