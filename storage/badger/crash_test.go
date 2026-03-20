package badger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/danthegoodman1/chainrep/storage"
)

func TestBadgerNodeReopenAfterAddReplicaAsTailBeforeActivationRecoversViaPeer(t *testing.T) {
	ctx := context.Background()
	repl := storage.NewInMemoryReplicationTransport()

	sourceBackend := storage.NewInMemoryBackend()
	sourceLocal := storage.NewInMemoryLocalStateStore()
	source, err := storage.OpenNode(ctx, storage.Config{NodeID: "source"}, sourceBackend, sourceLocal, storage.NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("OpenNode(source) returned error: %v", err)
	}
	repl.Register("source", sourceBackend)
	repl.RegisterNode("source", source)
	if err := source.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 2, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
		Epoch:      5,
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 2, Epoch: 5}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if result, err := source.SubmitPut(ctx, 2, "alpha", "v1"); err != nil {
		t.Fatalf("source SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("source sequence = %d, want %d", got, want)
	}

	path := filepath.Join(t.TempDir(), "target.db")
	store := mustOpenStore(t, path)
	coord := storage.NewInMemoryCoordinatorClient()
	target, err := storage.OpenNode(ctx, storage.Config{NodeID: "target"}, store.Backend(), store.LocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("OpenNode(target) returned error: %v", err)
	}
	repl.Register("target", store.Backend())
	repl.RegisterNode("target", target)

	assignment := storage.ReplicaAssignment{
		Slot:         2,
		ChainVersion: 5,
		Role:         storage.ReplicaRoleTail,
		Peers:        storage.ChainPeers{PredecessorNodeID: "source"},
	}
	if err := target.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: assignment,
		Epoch:      6,
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("target.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "target"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(target) returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	repl.Register("target", reopenedStore.Backend())
	repl.RegisterNode("target", reopened)

	replica := reopened.State().Replicas[2]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened target state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].LastKnownState, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if !recoveredCoord.RecoveryReports[0].Replicas[0].HasCommittedData {
		t.Fatal("reported committed data presence = false, want true")
	}

	if err := reopened.RecoverReplica(ctx, storage.RecoverReplicaCommand{
		Assignment:   assignment,
		SourceNodeID: "source",
		Epoch:        7,
	}); err != nil {
		t.Fatalf("RecoverReplica returned error: %v", err)
	}
	if got, want := reopened.State().Replicas[2].State, storage.ReplicaStateActive; got != want {
		t.Fatalf("recovered target state = %q, want %q", got, want)
	}
	if got, err := reopened.HighestCommittedSequence(2); err != nil {
		t.Fatalf("HighestCommittedSequence returned error: %v", err)
	} else if want := uint64(1); got != want {
		t.Fatalf("recovered target highest committed sequence = %d, want %d", got, want)
	}
	if read, err := reopened.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 2,
		Key:                  "alpha",
		ExpectedChainVersion: 5,
	}); err != nil {
		t.Fatalf("HandleClientGet after recovery returned error: %v", err)
	} else if !read.Found || read.Value != "v1" {
		t.Fatalf("HandleClientGet after recovery = %#v, want value v1", read)
	}
}

func TestBadgerNodeReopenAfterMarkReplicaLeavingBeforeRemoveDropsRecoveredReplica(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, store.Backend(), store.LocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}

	assignment := storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle}
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment, Epoch: 5}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1, Epoch: 5}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("SubmitPut sequence = %d, want %d", got, want)
	}
	if err := node.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{Slot: 1, Epoch: 6}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("node.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	replica := reopened.State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].LastKnownState, storage.ReplicaStateLeaving; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}

	if err := reopened.DropRecoveredReplica(ctx, storage.DropRecoveredReplicaCommand{Slot: 1, Epoch: 7}); err != nil {
		t.Fatalf("DropRecoveredReplica returned error: %v", err)
	}
	if _, exists := reopened.State().Replicas[1]; exists {
		t.Fatal("replica still present after DropRecoveredReplica")
	}
	if _, err := reopenedStore.Backend().HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
		t.Fatalf("HighestCommittedSequence error after drop = %v, want ErrUnknownReplica", err)
	}
	state, err := reopenedStore.LocalStateStore().LoadNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("LoadNode returned error: %v", err)
	}
	if got := len(state.Replicas); got != 0 {
		t.Fatalf("persisted replicas after drop = %d, want 0", got)
	}
}
