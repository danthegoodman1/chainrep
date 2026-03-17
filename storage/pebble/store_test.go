package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/danthegoodman1/chainrep/storage"
	"github.com/danthegoodman1/chainrep/storage/storagetest"
)

func TestPebbleBackendConformance(t *testing.T) {
	storagetest.RunBackendSuite(t, func(t *testing.T) storage.Backend {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "node.db"))
		if err != nil {
			t.Fatalf("Open returned error: %v", err)
		}
		return store.Backend()
	})
}

func TestPebbleLocalStateStoreConformance(t *testing.T) {
	storagetest.RunLocalStateStoreSuite(t, func(t *testing.T) storage.LocalStateStore {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "node.db"))
		if err != nil {
			t.Fatalf("Open returned error: %v", err)
		}
		return store.LocalStateStore()
	})
}

func TestPebbleCompletedOperationsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	backend := store.Backend()
	local := store.LocalStateStore()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after CreateReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, err := backend.HighestCommittedSequence(1); err != nil {
		t.Fatalf("HighestCommittedSequence after reopen returned error: %v", err)
	} else if got != 0 {
		t.Fatalf("HighestCommittedSequence after reopen = %d, want 0", got)
	}
	if err := backend.StagePut(1, 1, "k", "v"); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after StagePut returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, err := backend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences after reopen returned error: %v", err)
	} else if !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("StagedSequences after reopen = %v, want [1]", got)
	}
	if err := backend.CommitSequence(1, 1); err != nil {
		t.Fatalf("CommitSequence returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after CommitSequence returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, found, err := backend.GetCommitted(1, "k"); err != nil {
		t.Fatalf("GetCommitted after reopen returned error: %v", err)
	} else if !found || got != "v" {
		t.Fatalf("GetCommitted after reopen = (%q, %t), want (\"v\", true)", got, found)
	}
	if got, err := backend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences after commit reopen returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences after commit reopen = %v, want none", got)
	}
	if err := local.UpsertReplica(ctx, "node-a", storage.PersistedReplica{
		Assignment:               storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
		LastKnownState:           storage.ReplicaStateActive,
		HighestCommittedSequence: 1,
		HasCommittedData:         true,
	}); err != nil {
		t.Fatalf("UpsertReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after UpsertReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	state, err := local.LoadNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("LoadNode after reopen returned error: %v", err)
	}
	if got, want := len(state.Replicas), 1; got != want {
		t.Fatalf("replica count after reopen = %d, want %d", got, want)
	}
	if err := backend.DeleteReplica(1); err != nil {
		t.Fatalf("DeleteReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after DeleteReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	defer func() { _ = store.Close() }()
	if _, err := store.Backend().HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
		t.Fatalf("HighestCommittedSequence after delete reopen error = %v, want unknown replica", err)
	}
}

func TestPebbleNodeRestartRecoveryAndClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	backend := store.Backend()
	local := store.LocalStateStore()

	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("storage.OpenNode returned error: %v", err)
	}
	repl.Register("node-a", backend)
	repl.RegisterNode("node-a", node)
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if result.Sequence != 1 {
		t.Fatalf("SubmitPut sequence = %d, want 1", result.Sequence)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Node.Close returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second Node.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopenedBackend := reopenedStore.Backend()
	reopenedLocal := reopenedStore.LocalStateStore()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedBackend, reopenedLocal, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen storage.OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	repl.Register("node-a", reopenedBackend)
	repl.RegisterNode("node-a", reopened)

	replica := reopened.State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("ResumeRecoveredReplica returned error: %v", err)
	}
	if result, err := reopened.SubmitPut(ctx, 1, "beta", "v2"); err != nil {
		t.Fatalf("SubmitPut after reopen returned error: %v", err)
	} else if result.Sequence != 2 {
		t.Fatalf("SubmitPut after reopen sequence = %d, want 2", result.Sequence)
	}
	if read, found, err := reopenedBackend.GetCommitted(1, "beta"); err != nil {
		t.Fatalf("GetCommitted returned error: %v", err)
	} else if !found || read != "v2" {
		t.Fatalf("GetCommitted = (%q, %t), want (\"v2\", true)", read, found)
	}
}

func mustOpenStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return store
}
