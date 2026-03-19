package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/danthegoodman1/chainrep/coordinator"
)

func TestBadgerStoreRecoversPendingAndOutboxState(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	defer func() { _ = store.Close() }()

	rt := mustOpenRuntime(t, store)
	bootstrapState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "add-d",
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
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopenedStore, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()
	reopened := mustOpenRuntime(t, reopenedStore)

	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, wantPending := reopened.Current().PendingBySlot[1].NodeID, "d"; got != wantPending {
		t.Fatalf("pending slot 1 node = %q, want %q", got, wantPending)
	}
	if len(reopened.Current().Outbox) == 0 {
		t.Fatal("reopened runtime lost outbox entries")
	}
}
