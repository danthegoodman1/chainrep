package coordserver

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

func TestReportNodeRecoveredResumesExactMatchReplica(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 1, "a")

	report := storage.NodeRecoveryReport{
		NodeID: "a",
		Replicas: []storage.RecoveredReplica{{
			Assignment: storage.ReplicaAssignment{
				Slot:         0,
				ChainVersion: 1,
				Role:         storage.ReplicaRoleSingle,
			},
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 7,
			HasCommittedData:         true,
		}},
	}
	if err := server.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("ReportNodeRecovered returned error: %v", err)
	}
	if got, want := nodes["a"].calls, []string{"resume:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	snapshot, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if got, want := snapshot.Slots[0], (SlotRoute{
		Slot:         0,
		ChainVersion: 1,
		HeadNodeID:   "a",
		TailNodeID:   "a",
		Writable:     true,
		Readable:     true,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("slot route = %#v, want %#v", got, want)
	}
	if err := server.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("duplicate ReportNodeRecovered returned error: %v", err)
	}
	if got, want := nodes["a"].calls, []string{"resume:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("calls after duplicate = %v, want %v", got, want)
	}
}

func TestReportNodeRecoveredRebuildsStaleReplicaAndDropsExtras(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 2, "a", "b")

	report := storage.NodeRecoveryReport{
		NodeID: "b",
		Replicas: []storage.RecoveredReplica{
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         0,
					ChainVersion: 0,
					Role:         storage.ReplicaRoleTail,
					Peers:        storage.ChainPeers{PredecessorNodeID: "a"},
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 3,
				HasCommittedData:         true,
			},
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         9,
					ChainVersion: 1,
					Role:         storage.ReplicaRoleSingle,
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 1,
				HasCommittedData:         true,
			},
		},
	}
	if err := server.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("ReportNodeRecovered returned error: %v", err)
	}
	if !containsCall(nodes["b"].calls, "recover:0:a") {
		t.Fatalf("calls = %v, want recover from predecessor", nodes["b"].calls)
	}
	if !containsCall(nodes["b"].calls, "drop:9") {
		t.Fatalf("calls = %v, want stale slot drop", nodes["b"].calls)
	}
}

func TestReportNodeRecoveredFailsWithoutRecoverySourceAndBlocksRouting(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 1, "a")

	err := server.ReportNodeRecovered(ctx, storage.NodeRecoveryReport{
		NodeID: "a",
		Replicas: []storage.RecoveredReplica{{
			Assignment: storage.ReplicaAssignment{
				Slot:         0,
				ChainVersion: 1,
				Role:         storage.ReplicaRoleSingle,
			},
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 0,
			HasCommittedData:         false,
		}},
	})
	if err == nil {
		t.Fatal("ReportNodeRecovered unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("error = %v, want recovery failed", err)
	}
	snapshot, snapErr := server.RoutingSnapshot(ctx)
	if snapErr != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", snapErr)
	}
	if got, want := snapshot.Slots[0].Readable, false; got != want {
		t.Fatalf("slot readable = %t, want %t", got, want)
	}
	if got, want := snapshot.Slots[0].Writable, false; got != want {
		t.Fatalf("slot writable = %t, want %t", got, want)
	}
}

func TestEndToEndRestartResumeWithRuntimeReopen(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	repl := storage.NewInMemoryReplicationTransport()

	localA := storage.NewInMemoryLocalStateStore()
	localB := storage.NewInMemoryLocalStateStore()
	backendA := storage.NewInMemoryBackend()
	backendB := storage.NewInMemoryBackend()
	repl.Register("a", backendA)
	repl.Register("b", backendB)

	adapterA, err := OpenInMemoryNodeAdapter(ctx, "a", backendA, localA, repl)
	if err != nil {
		t.Fatalf("OpenInMemoryNodeAdapter(a) returned error: %v", err)
	}
	adapterB, err := OpenInMemoryNodeAdapter(ctx, "b", backendB, localB, repl)
	if err != nil {
		t.Fatalf("OpenInMemoryNodeAdapter(b) returned error: %v", err)
	}
	repl.RegisterNode("a", adapterA.Node())
	repl.RegisterNode("b", adapterB.Node())

	server, err := Open(ctx, store, map[string]StorageNodeClient{
		"a": adapterA,
		"b": adapterB,
	})
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	adapterA.BindServer(server)
	adapterB.BindServer(server)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap", 0, 1, 2, "a", "b")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	seedServerBootstrap(t, server, map[string]*InMemoryNodeAdapter{"a": adapterA, "b": adapterB}, 1, 2, []string{"a", "b"})
	if _, err := adapterA.Node().SubmitPut(ctx, 0, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	restartedA, err := OpenInMemoryNodeAdapter(ctx, "a", backendA, localA, repl)
	if err != nil {
		t.Fatalf("restart OpenInMemoryNodeAdapter(a) returned error: %v", err)
	}
	repl.RegisterNode("a", restartedA.Node())
	restartedB, err := OpenInMemoryNodeAdapter(ctx, "b", backendB, localB, repl)
	if err != nil {
		t.Fatalf("restart OpenInMemoryNodeAdapter(b) returned error: %v", err)
	}
	repl.RegisterNode("b", restartedB.Node())

	server, err = Open(ctx, store, map[string]StorageNodeClient{
		"a": restartedA,
		"b": restartedB,
	})
	if err != nil {
		t.Fatalf("reopen server returned error: %v", err)
	}
	restartedA.BindServer(server)
	restartedB.BindServer(server)
	if err := restartedA.Node().ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if err := restartedB.Node().ReportRecoveredState(ctx); err != nil {
		t.Fatalf("tail ReportRecoveredState returned error: %v", err)
	}

	state := restartedA.Node().State().Replicas[0]
	if got, want := state.State, storage.ReplicaStateActive; got != want {
		t.Fatalf("restarted replica state = %q, want %q", got, want)
	}
	if result, err := restartedA.Node().SubmitPut(ctx, 0, "beta", "v2"); err != nil {
		t.Fatalf("SubmitPut after restart returned error: %v", err)
	} else if got, want := result.Sequence, uint64(2); got != want {
		t.Fatalf("post-restart sequence = %d, want %d", got, want)
	}
	if read, err := restartedB.Node().HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 0,
		Key:                  "beta",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("tail HandleClientGet returned error: %v", err)
	} else if !read.Found || read.Value != "v2" {
		t.Fatalf("tail read result = %#v, want value v2", read)
	}
}

func seedServerBootstrap(
	t *testing.T,
	server *Server,
	adapters map[string]*InMemoryNodeAdapter,
	slotCount int,
	replicationFactor int,
	nodeIDs []string,
) {
	t.Helper()
	state, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         slotCount,
		ReplicationFactor: replicationFactor,
	}, uniqueNodes(nodeIDs...))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, adapter := range adapters {
		adapter.BindServer(nil)
	}
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assignment, err := assignmentForNode(chain, replica.NodeID, 1)
			if err != nil {
				t.Fatalf("assignmentForNode returned error: %v", err)
			}
			adapter := adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
		}
	}
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
}
