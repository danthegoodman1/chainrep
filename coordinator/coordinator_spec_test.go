package coordinator

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestSpecProducedStatesSatisfyCoreInvariants(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, generatedNodes("n", 5, fullDomainLayout))
	assertProducedStateInvariants(t, state)

	addPlan := assertDeterministicPlan(t, *state, []Event{
		{Kind: EventKindAddNode, Node: generatedNode("n", 5, fullDomainLayout)},
	}, ReconfigurationPolicy{MaxChangedChains: 2})
	assertProducedPlanInvariants(t, *state, addPlan)
	progressed := applyPlanProgressAndAssert(t, "spec-add", *addPlan, ReconfigurationPolicy{MaxChangedChains: 2})
	assertProducedStateInvariants(t, progressed)

	drainPlan := assertDeterministicPlan(t, *state, []Event{
		{Kind: EventKindBeginDrainNode, NodeID: state.Chains[0].Replicas[1].NodeID},
	}, ReconfigurationPolicy{})
	assertProducedPlanInvariants(t, *state, drainPlan)

	deadState := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, generatedNodes("m", 6, fullDomainLayout))
	deadPlan := assertDeterministicPlan(t, *deadState, []Event{
		{Kind: EventKindMarkNodeDead, NodeID: deadState.Chains[0].Replicas[1].NodeID},
	}, ReconfigurationPolicy{})
	assertProducedPlanInvariants(t, *deadState, deadPlan)
}

func TestSpecAppendTailPlansPreserveSurvivorsAndUseJoiningSuffix(t *testing.T) {
	testCases := []struct {
		name   string
		state  *ClusterState
		event  Event
		policy ReconfigurationPolicy
	}{
		{
			name:   "join append",
			state:  mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c")),
			event:  Event{Kind: EventKindAddNode, Node: uniqueNode("d")},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			name:   "drain append",
			state:  mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d")),
			event:  Event{Kind: EventKindBeginDrainNode, NodeID: "b"},
			policy: ReconfigurationPolicy{},
		},
		{
			name:   "dead repair append",
			state:  mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d")),
			event:  Event{Kind: EventKindMarkNodeDead, NodeID: "b"},
			policy: ReconfigurationPolicy{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan := assertDeterministicPlan(t, *tc.state, []Event{tc.event}, tc.policy)
			if len(plan.ChangedSlots) != 1 {
				t.Fatalf("changed slot count = %d, want 1", len(plan.ChangedSlots))
			}
			assertAppendTailSlotPlan(t, plan.ChangedSlots[0], &plan.UpdatedState)
		})
	}
}

func TestSpecMarkLeavingPlansPreserveMembershipAndUseLeavingSuffix(t *testing.T) {
	t.Run("join finalization", func(t *testing.T) {
		state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))
		addPlan := assertDeterministicPlan(t, *state, []Event{
			{Kind: EventKindAddNode, Node: uniqueNode("d")},
		}, ReconfigurationPolicy{MaxChangedChains: 1})
		progressed := applySpecificProgressAndAssert(t, addPlan.UpdatedState, Event{
			Kind:   EventKindReplicaBecameActive,
			Slot:   1,
			NodeID: "d",
		})

		finalizePlan := assertDeterministicPlan(t, *progressed, nil, ReconfigurationPolicy{MaxChangedChains: 1})
		if len(finalizePlan.ChangedSlots) != 1 {
			t.Fatalf("changed slot count = %d, want 1", len(finalizePlan.ChangedSlots))
		}
		assertMarkLeavingSlotPlan(t, finalizePlan.ChangedSlots[0], finalizePlan.UpdatedState.ReplicationFactor)
	})

	t.Run("drain finalization", func(t *testing.T) {
		state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))
		plan := assertDeterministicPlan(t, *state, []Event{
			{Kind: EventKindBeginDrainNode, NodeID: "b"},
		}, ReconfigurationPolicy{})
		progressed := applySpecificProgressAndAssert(t, plan.UpdatedState, Event{
			Kind:   EventKindReplicaBecameActive,
			Slot:   0,
			NodeID: "d",
		})

		finalizePlan := assertDeterministicPlan(t, *progressed, nil, ReconfigurationPolicy{})
		if len(finalizePlan.ChangedSlots) != 1 {
			t.Fatalf("changed slot count = %d, want 1", len(finalizePlan.ChangedSlots))
		}
		assertMarkLeavingSlotPlan(t, finalizePlan.ChangedSlots[0], finalizePlan.UpdatedState.ReplicationFactor)
	})
}

func TestGeneratedBootstrapFamilies(t *testing.T) {
	testCases := []struct {
		name   string
		cfg    Config
		nodes  []Node
		layout string
	}{
		{
			name:   "rf1 host-only",
			cfg:    Config{SlotCount: 4, ReplicationFactor: 1},
			nodes:  generatedNodes("h", 2, hostOnlyLayout),
			layout: "host-only",
		},
		{
			name:   "rf2 host-rack",
			cfg:    Config{SlotCount: 8, ReplicationFactor: 2},
			nodes:  generatedNodes("r", 4, hostRackLayout),
			layout: "host-rack",
		},
		{
			name:   "rf3 full domains",
			cfg:    Config{SlotCount: 8, ReplicationFactor: 3},
			nodes:  generatedNodes("f", 5, fullDomainLayout),
			layout: "full",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			left, err := BuildInitialPlacement(tc.cfg, tc.nodes)
			if err != nil {
				t.Fatalf("BuildInitialPlacement returned error: %v", err)
			}
			right, err := BuildInitialPlacement(tc.cfg, append([]Node(nil), tc.nodes...))
			if err != nil {
				t.Fatalf("second BuildInitialPlacement returned error: %v", err)
			}

			assertProducedStateInvariants(t, left)
			if !reflect.DeepEqual(left, right) {
				t.Fatalf("bootstrap not deterministic for layout %s", tc.layout)
			}
		})
	}
}

func TestGeneratedSingleEventFamilies(t *testing.T) {
	testCases := []struct {
		name    string
		cfg     Config
		nodes   []Node
		eventFn func(*ClusterState) Event
		policy  ReconfigurationPolicy
	}{
		{
			name:  "add budget zero",
			cfg:   Config{SlotCount: 8, ReplicationFactor: 3},
			nodes: generatedNodes("a", 3, fullDomainLayout),
			eventFn: func(_ *ClusterState) Event {
				return Event{Kind: EventKindAddNode, Node: generatedNode("a", 3, fullDomainLayout)}
			},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
		},
		{
			name:  "add budget one",
			cfg:   Config{SlotCount: 8, ReplicationFactor: 3},
			nodes: generatedNodes("b", 3, fullDomainLayout),
			eventFn: func(_ *ClusterState) Event {
				return Event{Kind: EventKindAddNode, Node: generatedNode("b", 3, fullDomainLayout)}
			},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			name:  "drain head",
			cfg:   Config{SlotCount: 4, ReplicationFactor: 3},
			nodes: generatedNodes("c", 4, fullDomainLayout),
			eventFn: func(state *ClusterState) Event {
				return Event{Kind: EventKindBeginDrainNode, NodeID: state.Chains[0].Replicas[0].NodeID}
			},
			policy: ReconfigurationPolicy{},
		},
		{
			name:  "dead tail",
			cfg:   Config{SlotCount: 4, ReplicationFactor: 3},
			nodes: generatedNodes("d", 5, fullDomainLayout),
			eventFn: func(state *ClusterState) Event {
				chain := state.Chains[0]
				return Event{Kind: EventKindMarkNodeDead, NodeID: chain.Replicas[len(chain.Replicas)-1].NodeID}
			},
			policy: ReconfigurationPolicy{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := mustBuildInitialState(t, tc.cfg, tc.nodes)
			plan := assertDeterministicPlan(t, *state, []Event{tc.eventFn(state)}, tc.policy)
			assertProducedPlanInvariants(t, *state, plan)
			assertProducedStateInvariants(t, &plan.UpdatedState)
		})
	}
}

func TestGeneratedRepeatedPlannerProgressFamilies(t *testing.T) {
	testCases := []struct {
		name   string
		cfg    Config
		nodes  []Node
		event  Event
		policy ReconfigurationPolicy
	}{
		{
			name:   "join to quiescence",
			cfg:    Config{SlotCount: 8, ReplicationFactor: 3},
			nodes:  uniqueNodes("a", "b", "c"),
			event:  Event{Kind: EventKindAddNode, Node: uniqueNode("d")},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			name:   "drain to quiescence",
			cfg:    Config{SlotCount: 4, ReplicationFactor: 3},
			nodes:  uniqueNodes("a", "b", "c", "d"),
			event:  Event{Kind: EventKindBeginDrainNode, NodeID: "b"},
			policy: ReconfigurationPolicy{},
		},
		{
			name:   "dead repair to quiescence",
			cfg:    Config{SlotCount: 4, ReplicationFactor: 3},
			nodes:  uniqueNodes("a", "b", "c", "d"),
			event:  Event{Kind: EventKindMarkNodeDead, NodeID: "b"},
			policy: ReconfigurationPolicy{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := mustBuildInitialState(t, tc.cfg, tc.nodes)
			plan := assertDeterministicPlan(t, *state, []Event{tc.event}, tc.policy)
			assertProducedPlanInvariants(t, *state, plan)

			finalState := applyPlanProgressAndAssert(t, tc.name, *plan, tc.policy)
			assertProducedStateInvariants(t, finalState)
		})
	}
}

func assertDeterministicPlan(
	t *testing.T,
	state ClusterState,
	events []Event,
	policy ReconfigurationPolicy,
) *ReconfigurationPlan {
	t.Helper()

	left, leftErr := PlanReconfiguration(state, events, policy)
	right, rightErr := PlanReconfiguration(state, events, policy)

	if !samePlanningOutcome(left, leftErr, right, rightErr) {
		t.Fatalf(
			"PlanReconfiguration not deterministic\nleftPlan=%v\nleftErr=%v\nrightPlan=%v\nrightErr=%v",
			left,
			leftErr,
			right,
			rightErr,
		)
	}
	if leftErr != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", leftErr)
	}
	return left
}

func samePlanningOutcome(
	left *ReconfigurationPlan,
	leftErr error,
	right *ReconfigurationPlan,
	rightErr error,
) bool {
	if (leftErr == nil) != (rightErr == nil) {
		return false
	}
	if leftErr != nil || rightErr != nil {
		return errorSummary(leftErr) == errorSummary(rightErr)
	}
	return reflect.DeepEqual(left, right)
}

func errorSummary(err error) string {
	if err == nil {
		return ""
	}
	var placementErr *PlacementError
	if errors.As(err, &placementErr) {
		return fmt.Sprintf("placement:%d:%d:%v", placementErr.Slot, placementErr.ReplicaPosition, errors.Unwrap(err))
	}
	return err.Error()
}

func assertProducedStateInvariants(t *testing.T, state *ClusterState) {
	t.Helper()

	if got, want := len(state.Chains), state.SlotCount; got != want {
		t.Fatalf("chain count = %d, want %d", got, want)
	}

	orderSeen := make(map[string]struct{}, len(state.NodeOrder))
	for _, nodeID := range state.NodeOrder {
		if _, ok := state.NodesByID[nodeID]; !ok {
			t.Fatalf("node order references unknown node %q", nodeID)
		}
		if _, exists := orderSeen[nodeID]; exists {
			t.Fatalf("node order contains duplicate node %q", nodeID)
		}
		orderSeen[nodeID] = struct{}{}
	}
	for nodeID := range state.NodesByID {
		if _, ok := state.NodeHealthByID[nodeID]; !ok {
			t.Fatalf("missing node health for node %q", nodeID)
		}
	}

	for slot, chain := range state.Chains {
		if chain.Slot != slot {
			t.Fatalf("chain[%d].Slot = %d, want %d", slot, chain.Slot, slot)
		}
		if len(chain.Replicas) < state.ReplicationFactor {
			t.Fatalf("chain %d replica count = %d, want >= %d", slot, len(chain.Replicas), state.ReplicationFactor)
		}
		if len(chain.Replicas) > state.ReplicationFactor+1 {
			t.Fatalf("chain %d replica count = %d, want <= %d", slot, len(chain.Replicas), state.ReplicationFactor+1)
		}

		seenReplicas := make(map[string]struct{}, len(chain.Replicas))
		activeCount := 0
		hasJoining := false
		hasLeaving := false
		phase := ReplicaStateActive
		for _, replica := range chain.Replicas {
			if _, ok := state.NodesByID[replica.NodeID]; !ok {
				t.Fatalf("chain %d references unknown node %q", slot, replica.NodeID)
			}
			if state.NodeHealthByID[replica.NodeID] == NodeHealthDead {
				t.Fatalf("chain %d retains dead node %q", slot, replica.NodeID)
			}
			if _, exists := seenReplicas[replica.NodeID]; exists {
				t.Fatalf("chain %d contains duplicate node %q", slot, replica.NodeID)
			}
			seenReplicas[replica.NodeID] = struct{}{}

			switch replica.State {
			case ReplicaStateActive:
				activeCount++
				if phase != ReplicaStateActive {
					t.Fatalf("chain %d has active replica after %s suffix", slot, phase)
				}
			case ReplicaStateJoining:
				hasJoining = true
				if phase == ReplicaStateLeaving {
					t.Fatalf("chain %d mixes joining after leaving", slot)
				}
				phase = ReplicaStateJoining
			case ReplicaStateLeaving:
				hasLeaving = true
				phase = ReplicaStateLeaving
			default:
				t.Fatalf("chain %d has invalid replica state %q", slot, replica.State)
			}
		}
		if hasJoining && hasLeaving {
			t.Fatalf("chain %d mixes joining and leaving replicas", slot)
		}
		if activeCount == 0 {
			t.Fatalf("chain %d has no active replicas", slot)
		}

		assertFailureDomainLegality(t, slot, chain, state.NodesByID)
	}
}

func assertFailureDomainLegality(t *testing.T, slot int, chain Chain, nodesByID map[string]Node) {
	t.Helper()
	for i := 0; i < len(chain.Replicas); i++ {
		left := nodesByID[chain.Replicas[i].NodeID]
		for j := i + 1; j < len(chain.Replicas); j++ {
			right := nodesByID[chain.Replicas[j].NodeID]
			if hasFailureDomainConflict(left, right) {
				t.Fatalf("chain %d has failure-domain conflict between %q and %q", slot, left.ID, right.ID)
			}
		}
	}
}

func assertProducedPlanInvariants(t *testing.T, before ClusterState, plan *ReconfigurationPlan) {
	t.Helper()

	assertProducedStateInvariants(t, &plan.UpdatedState)
	for _, slotPlan := range plan.ChangedSlots {
		if slotPlan.Slot < 0 || slotPlan.Slot >= before.SlotCount {
			t.Fatalf("slot plan references invalid slot %d", slotPlan.Slot)
		}
		if !reflect.DeepEqual(slotPlan.Before, before.Chains[slotPlan.Slot]) {
			t.Fatalf("slot %d before chain does not match input state", slotPlan.Slot)
		}
		if !reflect.DeepEqual(slotPlan.After, plan.UpdatedState.Chains[slotPlan.Slot]) {
			t.Fatalf("slot %d after chain does not match updated state", slotPlan.Slot)
		}
		if len(slotPlan.Steps) == 0 {
			t.Fatalf("slot %d has no steps", slotPlan.Slot)
		}

		beforeIDs := replicaIDs(slotPlan.Before.Replicas)
		afterIDs := replicaIDs(slotPlan.After.Replicas)
		if disjointChain(beforeIDs, afterIDs) {
			t.Fatalf("slot %d produced a fully disjoint remap", slotPlan.Slot)
		}
		if intersectionCount(activeReplicaIDs(slotPlan.Before), activeReplicaIDs(slotPlan.After)) == 0 {
			t.Fatalf("slot %d lost active-anchor continuity", slotPlan.Slot)
		}

		stepKinds := distinctStepKinds(slotPlan.Steps)
		if len(stepKinds) != 1 {
			t.Fatalf("slot %d has mixed step kinds: %v", slotPlan.Slot, stepKinds)
		}

		switch slotPlan.Steps[0].Kind {
		case StepKindAppendTail:
			assertAppendTailSlotPlan(t, slotPlan, &plan.UpdatedState)
		case StepKindMarkLeaving:
			assertMarkLeavingSlotPlan(t, slotPlan, plan.UpdatedState.ReplicationFactor)
		default:
			t.Fatalf("slot %d has unknown step kind %q", slotPlan.Slot, slotPlan.Steps[0].Kind)
		}
	}
}

func assertAppendTailSlotPlan(t *testing.T, slotPlan SlotPlan, updatedState *ClusterState) {
	t.Helper()

	beforeIDs := replicaIDs(slotPlan.Before.Replicas)
	afterIDs := replicaIDs(slotPlan.After.Replicas)
	addedIDs := diffOrdered(afterIDs, beforeIDs)
	if got, want := len(addedIDs), len(slotPlan.Steps); got != want {
		t.Fatalf("slot %d added ids = %v, steps = %v", slotPlan.Slot, addedIDs, slotPlan.Steps)
	}
	if len(addedIDs) > 0 {
		suffix := slotPlan.After.Replicas[len(slotPlan.After.Replicas)-len(addedIDs):]
		for i, replica := range suffix {
			if replica.NodeID != addedIDs[i] {
				t.Fatalf("slot %d added node %q not in tail suffix position", slotPlan.Slot, addedIDs[i])
			}
			if replica.State != ReplicaStateJoining {
				t.Fatalf("slot %d added node %q state = %q, want joining", slotPlan.Slot, replica.NodeID, replica.State)
			}
			if updatedState.NodeHealthByID[replica.NodeID] != NodeHealthAlive {
				t.Fatalf("slot %d added node %q is not alive", slotPlan.Slot, replica.NodeID)
			}
			if updatedState.DrainingNodeIDs[replica.NodeID] {
				t.Fatalf("slot %d added node %q is draining", slotPlan.Slot, replica.NodeID)
			}
		}
	}

	commonBefore := filterOrdered(beforeIDs, toSet(afterIDs))
	commonAfter := filterOrdered(afterIDs, toSet(beforeIDs))
	if !reflect.DeepEqual(commonBefore, commonAfter) {
		t.Fatalf("slot %d survivor order changed across append_tail\nbefore=%v\nafter=%v", slotPlan.Slot, commonBefore, commonAfter)
	}
}

func assertMarkLeavingSlotPlan(t *testing.T, slotPlan SlotPlan, replicationFactor int) {
	t.Helper()

	beforeIDs := replicaIDs(slotPlan.Before.Replicas)
	afterIDs := replicaIDs(slotPlan.After.Replicas)
	if !sameIDSet(beforeIDs, afterIDs) {
		t.Fatalf("slot %d changed membership during mark_leaving\nbefore=%v\nafter=%v", slotPlan.Slot, beforeIDs, afterIDs)
	}
	if got, want := len(activeReplicaIDs(slotPlan.After)), replicationFactor; got != want {
		t.Fatalf("slot %d active replicas = %d, want %d", slotPlan.Slot, got, want)
	}

	for _, step := range slotPlan.Steps {
		index := findReplicaIndex(slotPlan.After, step.NodeID)
		if index < 0 {
			t.Fatalf("slot %d leaving node %q missing from after chain", slotPlan.Slot, step.NodeID)
		}
		if slotPlan.After.Replicas[index].State != ReplicaStateLeaving {
			t.Fatalf("slot %d node %q state = %q, want leaving", slotPlan.Slot, step.NodeID, slotPlan.After.Replicas[index].State)
		}
	}
}

func applySpecificProgressAndAssert(t *testing.T, state ClusterState, event Event) *ClusterState {
	t.Helper()
	updated, err := ApplyProgress(state, event)
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}
	assertProducedStateInvariants(t, updated)
	return updated
}

func applyPlanProgressAndAssert(
	t *testing.T,
	label string,
	plan ReconfigurationPlan,
	policy ReconfigurationPolicy,
) *ClusterState {
	t.Helper()

	state := &plan.UpdatedState
	for _, slotPlan := range plan.ChangedSlots {
		for _, step := range slotPlan.Steps {
			progressEvent := Event{Slot: slotPlan.Slot, NodeID: step.NodeID}
			switch step.Kind {
			case StepKindAppendTail:
				progressEvent.Kind = EventKindReplicaBecameActive
			case StepKindMarkLeaving:
				progressEvent.Kind = EventKindReplicaRemoved
			default:
				t.Fatalf("%s: unknown step kind %q", label, step.Kind)
			}

			var err error
			state, err = ApplyProgress(*state, progressEvent)
			if err != nil {
				t.Fatalf("%s: ApplyProgress(%+v) returned error: %v", label, progressEvent, err)
			}
			assertProducedStateInvariants(t, state)
		}
	}

	return driveToQuiescenceAndAssert(t, label, state, policy)
}

func driveToQuiescenceAndAssert(
	t *testing.T,
	label string,
	state *ClusterState,
	policy ReconfigurationPolicy,
) *ClusterState {
	t.Helper()

	current := state
	for round := 0; round < 128; round++ {
		plan := assertDeterministicPlan(t, *current, nil, policy)
		assertProducedPlanInvariants(t, *current, plan)
		if len(plan.ChangedSlots) == 0 {
			assertProducedStateInvariants(t, &plan.UpdatedState)
			return &plan.UpdatedState
		}
		current = applyPlanProgressAndAssertNoQuiesce(t, fmt.Sprintf("%s-round-%d", label, round), *plan)
	}
	t.Fatalf("%s: coordinator did not quiesce within 128 rounds", label)
	return nil
}

func applyPlanProgressAndAssertNoQuiesce(
	t *testing.T,
	label string,
	plan ReconfigurationPlan,
) *ClusterState {
	t.Helper()

	state := &plan.UpdatedState
	for _, slotPlan := range plan.ChangedSlots {
		for _, step := range slotPlan.Steps {
			progressEvent := Event{Slot: slotPlan.Slot, NodeID: step.NodeID}
			switch step.Kind {
			case StepKindAppendTail:
				progressEvent.Kind = EventKindReplicaBecameActive
			case StepKindMarkLeaving:
				progressEvent.Kind = EventKindReplicaRemoved
			}

			var err error
			state, err = ApplyProgress(*state, progressEvent)
			if err != nil {
				t.Fatalf("%s: ApplyProgress(%+v) returned error: %v", label, progressEvent, err)
			}
			assertProducedStateInvariants(t, state)
		}
	}
	return state
}

type domainLayout func(prefix string, index int) map[string]string

func generatedNodes(prefix string, count int, layout domainLayout) []Node {
	nodes := make([]Node, 0, count)
	for i := 0; i < count; i++ {
		nodes = append(nodes, generatedNode(prefix, i, layout))
	}
	return nodes
}

func generatedNode(prefix string, index int, layout domainLayout) Node {
	id := fmt.Sprintf("%s-%02d", prefix, index)
	return Node{
		ID:             id,
		FailureDomains: layout(prefix, index),
	}
}

func hostOnlyLayout(prefix string, index int) map[string]string {
	return map[string]string{
		"host": fmt.Sprintf("%s-host-%02d", prefix, index),
	}
}

func hostRackLayout(prefix string, index int) map[string]string {
	return map[string]string{
		"host": fmt.Sprintf("%s-host-%02d", prefix, index),
		"rack": fmt.Sprintf("%s-rack-%02d", prefix, index),
	}
}

func fullDomainLayout(prefix string, index int) map[string]string {
	return map[string]string{
		"host": fmt.Sprintf("%s-host-%02d", prefix, index),
		"rack": fmt.Sprintf("%s-rack-%02d", prefix, index),
		"az":   fmt.Sprintf("%s-az-%02d", prefix, index),
	}
}

func distinctStepKinds(steps []ReconfigurationStep) []StepKind {
	values := make(map[StepKind]struct{}, len(steps))
	for _, step := range steps {
		values[step.Kind] = struct{}{}
	}
	kinds := make([]StepKind, 0, len(values))
	for kind := range values {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

func filterOrdered(values []string, keep map[string]struct{}) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := keep[value]; ok {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func toSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func diffOrdered(left []string, right []string) []string {
	rightSet := toSet(right)
	diff := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; !ok {
			diff = append(diff, value)
		}
	}
	return diff
}

func sameIDSet(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSorted := append([]string(nil), left...)
	rightSorted := append([]string(nil), right...)
	sort.Strings(leftSorted)
	sort.Strings(rightSorted)
	return reflect.DeepEqual(leftSorted, rightSorted)
}

func intersectionCount(left []string, right []string) int {
	rightSet := toSet(right)
	count := 0
	for _, value := range left {
		if _, ok := rightSet[value]; ok {
			count++
		}
	}
	return count
}
