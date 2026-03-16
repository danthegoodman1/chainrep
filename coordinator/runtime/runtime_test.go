package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/danthegoodman1/chainrep/coordinator"
)

func TestOpenEmptyStore(t *testing.T) {
	rt, err := Open(context.Background(), NewInMemoryStore())
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	if got := rt.Current(); got.Version != 0 || got.LastLogIndex != 0 || got.Cluster.SlotCount != 0 {
		t.Fatalf("unexpected empty runtime state: %#v", got)
	}
}

func TestBootstrapPersistsAndRecovers(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	rt := mustOpenRuntime(t, store)
	first, err := rt.Bootstrap(ctx, Command{
		ID:              "bootstrap-1",
		ExpectedVersion: 0,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{SlotCount: 8, ReplicationFactor: 3},
			Nodes:  uniqueNodes("a", "b", "c"),
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, first) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, first)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestRecoverFromCheckpointAndWALTail(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	rt := mustOpenRuntime(t, store)
	bootstrapState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	plan, stateAfterReconfigure, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: bootstrapState.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}
	if len(plan.ChangedSlots) == 0 {
		t.Fatal("Reconfigure produced no changed slots")
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, stateAfterReconfigure) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, stateAfterReconfigure)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestOpenFailsOnInvalidWALOrder(t *testing.T) {
	store := NewInMemoryStore()
	store.wal = []LogRecord{
		{Index: 2, Command: bootstrapCommand("bootstrap-1", 0, 1, 1, "a")},
	}

	_, err := Open(context.Background(), store)
	if err == nil {
		t.Fatal("Open unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRecovery) {
		t.Fatalf("error = %v, want recovery failure", err)
	}
}

func TestOpenFailsWhenReplayCommandCannotApply(t *testing.T) {
	store := NewInMemoryStore()
	store.wal = []LogRecord{
		{
			Index: 1,
			Command: Command{
				ID:              "reconfigure-1",
				ExpectedVersion: 0,
				Kind:            CommandKindReconfigure,
				Reconfigure: &ReconfigureCommand{
					Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
					Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
				},
			},
		},
	}

	_, err := Open(context.Background(), store)
	if err == nil {
		t.Fatal("Open unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRecovery) {
		t.Fatalf("error = %v, want recovery failure", err)
	}
}

func TestDuplicateCommandIdenticalPayloadIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	firstState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	secondState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("duplicate Bootstrap returned error: %v", err)
	}

	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(secondState, firstState) {
		t.Fatalf("duplicate state mismatch\ngot=%#v\nwant=%#v", secondState, firstState)
	}
}

func TestDuplicateCommandDifferentPayloadFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	_, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 2, "a", "b"))
	if err == nil {
		t.Fatal("Bootstrap unexpectedly succeeded")
	}
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("error = %v, want command conflict", err)
	}
}

func TestStaleExpectedVersionFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	_, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 0,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err == nil {
		t.Fatal("Reconfigure unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestWrongLifecycleVersionFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 0,
		Kind:            CommandKindReconfigure,
		Reconfigure:     &ReconfigureCommand{},
	}); err == nil {
		t.Fatal("Reconfigure unexpectedly succeeded before bootstrap")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error = %v, want not initialized", err)
	}

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-2", 1, 8, 3, "a", "b", "c")); err == nil {
		t.Fatal("second Bootstrap unexpectedly succeeded")
	} else if !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("error = %v, want already initialized", err)
	}
}

func TestReconfigureAndApplyProgressPersistAndRecover(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	slot := plan.ChangedSlots[0].Slot
	joiningNode := plan.ChangedSlots[0].Steps[0].NodeID
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   slot,
				NodeID: joiningNode,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, state)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestCheckpointPrunesAppliedCommandsAndPreservesRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 0},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	expectedAfterCheckpoint := state
	expectedAfterCheckpoint.AppliedCommands = map[string]AppliedCommand{}

	if got := len(rt.Current().AppliedCommands); got != 0 {
		t.Fatalf("applied command count after checkpoint = %d, want 0", got)
	}
	if got := len(store.wal); got != 0 {
		t.Fatalf("wal length after checkpoint = %d, want 0", got)
	}
	if got := rt.Current(); !reflect.DeepEqual(got, expectedAfterCheckpoint) {
		t.Fatalf("state after checkpoint mismatch\ngot=%#v\nwant=%#v", got, expectedAfterCheckpoint)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, expectedAfterCheckpoint) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, expectedAfterCheckpoint)
	}
	if got := len(reopened.Current().AppliedCommands); got != 0 {
		t.Fatalf("recovered applied command count after checkpoint = %d, want 0", got)
	}

	_, err = reopened.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err == nil {
		t.Fatal("duplicate Bootstrap after checkpoint unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestInMemoryStoreReadWrite(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	if _, ok, err := store.LoadLatestCheckpoint(ctx); err != nil {
		t.Fatalf("LoadLatestCheckpoint returned error: %v", err)
	} else if ok {
		t.Fatal("LoadLatestCheckpoint unexpectedly reported a checkpoint")
	}

	record := LogRecord{Index: 1, Command: bootstrapCommand("bootstrap-1", 0, 1, 1, "a")}
	if err := store.AppendWAL(ctx, record); err != nil {
		t.Fatalf("AppendWAL returned error: %v", err)
	}
	records, err := store.LoadWAL(ctx, 0)
	if err != nil {
		t.Fatalf("LoadWAL returned error: %v", err)
	}
	if got, want := records, []LogRecord{record}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wal records = %#v, want %#v", got, want)
	}

	checkpoint := Checkpoint{State: State{Version: 1, LastLogIndex: 1, AppliedCommands: map[string]AppliedCommand{}}}
	if err := store.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}
	gotCheckpoint, ok, err := store.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint returned error: %v", err)
	}
	if !ok {
		t.Fatal("LoadLatestCheckpoint did not report checkpoint")
	}
	if gotCheckpoint.State.Version != checkpoint.State.Version {
		t.Fatalf("checkpoint version = %d, want %d", gotCheckpoint.State.Version, checkpoint.State.Version)
	}
	if gotCheckpoint.State.LastLogIndex != checkpoint.State.LastLogIndex {
		t.Fatalf(
			"checkpoint last log index = %d, want %d",
			gotCheckpoint.State.LastLogIndex,
			checkpoint.State.LastLogIndex,
		)
	}
	if !reflect.DeepEqual(gotCheckpoint.State.AppliedCommands, checkpoint.State.AppliedCommands) {
		t.Fatalf(
			"checkpoint applied commands = %#v, want %#v",
			gotCheckpoint.State.AppliedCommands,
			checkpoint.State.AppliedCommands,
		)
	}

	secondRecord := LogRecord{Index: 2, Command: bootstrapCommand("bootstrap-2", 1, 1, 1, "b")}
	if err := store.AppendWAL(ctx, secondRecord); err != nil {
		t.Fatalf("AppendWAL(second) returned error: %v", err)
	}
	if err := store.TruncateWAL(ctx, 1); err != nil {
		t.Fatalf("TruncateWAL returned error: %v", err)
	}
	records, err = store.LoadWAL(ctx, 0)
	if err != nil {
		t.Fatalf("LoadWAL after truncate returned error: %v", err)
	}
	if got, want := records, []LogRecord{secondRecord}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wal records after truncate = %#v, want %#v", got, want)
	}
}

func TestDuplicateCommandAfterCheckpointFailsStaleInsteadOfIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if _, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 0},
		},
	}); err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	_, err = rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err == nil {
		t.Fatal("duplicate Bootstrap after checkpoint unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestPostCheckpointCommandsRemainIdempotentUntilNextCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	duplicatePlan, duplicateState, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 1,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("duplicate Reconfigure returned error: %v", err)
	}
	if !reflect.DeepEqual(duplicatePlan, plan) {
		t.Fatalf("duplicate plan mismatch\ngot=%#v\nwant=%#v", duplicatePlan, plan)
	}
	if !reflect.DeepEqual(duplicateState, state) {
		t.Fatalf("duplicate state mismatch\ngot=%#v\nwant=%#v", duplicateState, state)
	}
}

func TestReplayMatchesLiveRuntime(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   plan.ChangedSlots[0].Slot,
				NodeID: plan.ChangedSlots[0].Steps[0].NodeID,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("replayed state mismatch\nreplayed=%#v\nwant=%#v", got, state)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func mustOpenRuntime(t *testing.T, store Store) *Runtime {
	t.Helper()
	rt, err := Open(context.Background(), store)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return rt
}

func bootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) Command {
	return Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: uniqueNodes(nodeIDs...),
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

func assertCoordinatorStateValid(t *testing.T, state coordinator.ClusterState) {
	t.Helper()
	if _, err := coordinator.PlanReconfiguration(state, nil, coordinator.ReconfigurationPolicy{}); err != nil {
		t.Fatalf("cluster state is not valid for coordinator replay: %v", err)
	}
}
