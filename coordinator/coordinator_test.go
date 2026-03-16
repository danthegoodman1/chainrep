package coordinator

import (
	"errors"
	"reflect"
	"testing"
)

func TestBuildInitialPlacementBuildsExpectedChainCount(t *testing.T) {
	nodes := uniqueNodes(
		"a", "b", "c", "d", "e",
	)

	placement, err := BuildInitialPlacement(Config{
		SlotCount:         DefaultSlotCount,
		ReplicationFactor: 3,
	}, nodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	if got, want := len(placement.Chains), DefaultSlotCount; got != want {
		t.Fatalf("chain count = %d, want %d", got, want)
	}

	for i, chain := range placement.Chains {
		if chain.Slot != i {
			t.Fatalf("chain[%d].Slot = %d, want %d", i, chain.Slot, i)
		}
		if got, want := len(chain.ReplicaIDs), 3; got != want {
			t.Fatalf("chain[%d] replica count = %d, want %d", i, got, want)
		}
	}
}

func TestBuildInitialPlacementRespectsFailureDomains(t *testing.T) {
	nodes := []Node{
		{ID: "a", FailureDomains: map[string]string{"host": "h1", "rack": "r1", "az": "az1"}},
		{ID: "b", FailureDomains: map[string]string{"host": "h2", "rack": "r2", "az": "az2"}},
		{ID: "c", FailureDomains: map[string]string{"host": "h3", "rack": "r3", "az": "az3"}},
		{ID: "d", FailureDomains: map[string]string{"host": "h4", "rack": "r4", "az": "az4"}},
	}

	placement, err := BuildInitialPlacement(Config{
		SlotCount:         32,
		ReplicationFactor: 3,
	}, nodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	for _, chain := range placement.Chains {
		seen := make(map[string]struct{}, len(chain.ReplicaIDs))
		for _, replicaID := range chain.ReplicaIDs {
			if _, exists := seen[replicaID]; exists {
				t.Fatalf("chain %d contains duplicate replica ID %q", chain.Slot, replicaID)
			}
			seen[replicaID] = struct{}{}
		}

		for i := 0; i < len(chain.ReplicaIDs); i++ {
			left := placement.NodesByID[chain.ReplicaIDs[i]]
			for j := i + 1; j < len(chain.ReplicaIDs); j++ {
				right := placement.NodesByID[chain.ReplicaIDs[j]]
				if hasFailureDomainConflict(left, right) {
					t.Fatalf("chain %d has failure-domain conflict between %q and %q", chain.Slot, left.ID, right.ID)
				}
			}
		}
	}
}

func TestBuildInitialPlacementFailsWhenEligibleNodesRunOut(t *testing.T) {
	nodes := []Node{
		{ID: "a", FailureDomains: map[string]string{"rack": "r1", "az": "az1"}},
		{ID: "b", FailureDomains: map[string]string{"rack": "r1", "az": "az2"}},
		{ID: "c", FailureDomains: map[string]string{"rack": "r2", "az": "az1"}},
	}

	_, err := BuildInitialPlacement(Config{
		SlotCount:         1,
		ReplicationFactor: 3,
	}, nodes)
	if err == nil {
		t.Fatal("BuildInitialPlacement unexpectedly succeeded")
	}
	if !errors.Is(err, ErrPlacementImpossible) {
		t.Fatalf("error = %v, want placement impossible", err)
	}

	var placementErr *PlacementError
	if !errors.As(err, &placementErr) {
		t.Fatalf("error = %v, want PlacementError", err)
	}
	if got, want := placementErr.Slot, 0; got != want {
		t.Fatalf("placement error slot = %d, want %d", got, want)
	}
	if got, want := placementErr.ReplicaPosition, 1; got != want {
		t.Fatalf("placement error replica position = %d, want %d", got, want)
	}
}

func TestBuildInitialPlacementIsDeterministicForSameInput(t *testing.T) {
	nodes := uniqueNodes("a", "b", "c", "d")
	cfg := Config{SlotCount: 64, ReplicationFactor: 3}

	left, err := BuildInitialPlacement(cfg, nodes)
	if err != nil {
		t.Fatalf("first BuildInitialPlacement returned error: %v", err)
	}

	right, err := BuildInitialPlacement(cfg, nodes)
	if err != nil {
		t.Fatalf("second BuildInitialPlacement returned error: %v", err)
	}

	if !reflect.DeepEqual(left, right) {
		t.Fatal("placements differ for identical input")
	}
}

func TestBuildInitialPlacementTieBreakIsIndependentOfInputOrder(t *testing.T) {
	cfg := Config{SlotCount: 8, ReplicationFactor: 2}
	orderedNodes := []Node{
		{ID: "b", FailureDomains: map[string]string{"host": "h2", "rack": "r2"}},
		{ID: "a", FailureDomains: map[string]string{"host": "h1", "rack": "r1"}},
		{ID: "d", FailureDomains: map[string]string{"host": "h4", "rack": "r4"}},
		{ID: "c", FailureDomains: map[string]string{"host": "h3", "rack": "r3"}},
	}
	reversedNodes := []Node{
		orderedNodes[3],
		orderedNodes[2],
		orderedNodes[1],
		orderedNodes[0],
	}

	left, err := BuildInitialPlacement(cfg, orderedNodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement(ordered) returned error: %v", err)
	}

	right, err := BuildInitialPlacement(cfg, reversedNodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement(reversed) returned error: %v", err)
	}

	if !reflect.DeepEqual(left, right) {
		t.Fatal("placements differ when input order changes")
	}
}

func TestValidateConfigAndNodes(t *testing.T) {
	testCases := []struct {
		name  string
		cfg   Config
		nodes []Node
		want  error
	}{
		{
			name:  "invalid slot count",
			cfg:   Config{SlotCount: 0, ReplicationFactor: 1},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
		{
			name:  "invalid replication factor",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 0},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
		{
			name:  "empty node list",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: nil,
			want:  ErrInvalidConfig,
		},
		{
			name:  "empty node ID",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{{ID: "", FailureDomains: map[string]string{"host": "h1"}}},
			want:  ErrInvalidNode,
		},
		{
			name: "duplicate node ID",
			cfg:  Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{
				{ID: "a", FailureDomains: map[string]string{"host": "h1"}},
				{ID: "a", FailureDomains: map[string]string{"host": "h2"}},
			},
			want: ErrInvalidNode,
		},
		{
			name:  "empty failure-domain key",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{{ID: "a", FailureDomains: map[string]string{"": "h1"}}},
			want:  ErrInvalidNode,
		},
		{
			name:  "empty failure-domain value",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{{ID: "a", FailureDomains: map[string]string{"host": ""}}},
			want:  ErrInvalidNode,
		},
		{
			name:  "replication factor exceeds nodes",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 2},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildInitialPlacement(tc.cfg, tc.nodes)
			if err == nil {
				t.Fatal("BuildInitialPlacement unexpectedly succeeded")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCompareNodesPrefersTailThenHeadThenReplicaThenID(t *testing.T) {
	nodes := map[string]Node{
		"a": {ID: "a"},
		"b": {ID: "b"},
		"c": {ID: "c"},
		"d": {ID: "d"},
		"e": {ID: "e"},
		"f": {ID: "f"},
		"g": {ID: "g"},
		"h": {ID: "h"},
	}

	t.Run("tail count wins first", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"a": {tailCount: 0, headCount: 3, replicaCount: 5},
			"b": {tailCount: 1, headCount: 0, replicaCount: 0},
		}
		if got := compareNodes(nodes["a"], nodes["b"], counts); got >= 0 {
			t.Fatalf("compareNodes(a, b) = %d, want a to win on lower tail count", got)
		}
	})

	t.Run("head count breaks tail tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"c": {tailCount: 1, headCount: 0, replicaCount: 5},
			"d": {tailCount: 1, headCount: 1, replicaCount: 0},
		}
		if got := compareNodes(nodes["c"], nodes["d"], counts); got >= 0 {
			t.Fatalf("compareNodes(c, d) = %d, want c to win on lower head count", got)
		}
	})

	t.Run("replica count breaks head tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"e": {tailCount: 1, headCount: 1, replicaCount: 0},
			"f": {tailCount: 1, headCount: 1, replicaCount: 1},
		}
		if got := compareNodes(nodes["e"], nodes["f"], counts); got >= 0 {
			t.Fatalf("compareNodes(e, f) = %d, want e to win on lower replica count", got)
		}
	})

	t.Run("node ID breaks full tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"g": {},
			"h": {},
		}
		if got := compareNodes(nodes["g"], nodes["h"], counts); got >= 0 {
			t.Fatalf("compareNodes(g, h) = %d, want g to win on lower node ID", got)
		}
	})
}

func TestReplicationFactorOneIsValid(t *testing.T) {
	placement, err := BuildInitialPlacement(Config{
		SlotCount:         4,
		ReplicationFactor: 1,
	}, uniqueNodes("a", "b"))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	for _, chain := range placement.Chains {
		if got, want := len(chain.ReplicaIDs), 1; got != want {
			t.Fatalf("chain %d replica count = %d, want %d", chain.Slot, got, want)
		}
	}
}

func uniqueNodes(ids ...string) []Node {
	nodes := make([]Node, 0, len(ids))
	for i, id := range ids {
		nodes = append(nodes, Node{
			ID: id,
			FailureDomains: map[string]string{
				"host": "host-" + id,
				"rack": "rack-" + id,
				"az":   "az-" + string(rune('a'+i)),
			},
		})
	}
	return nodes
}
