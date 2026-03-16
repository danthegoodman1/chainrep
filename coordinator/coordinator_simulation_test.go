package coordinator

import (
	"errors"
	"reflect"
	"testing"
)

type historyAction struct {
	label       string
	events      []Event
	policy      ReconfigurationPolicy
	expectError error
	checkPlan   func(*testing.T, *ReconfigurationPlan)
}

func TestSimulationBalancedHistory(t *testing.T) {
	initial := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d", "e"))

	final := runDeterministicHistory(t, "balanced", initial, []historyAction{
		{
			label:  "add-f-no-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("f")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
		},
		{
			label:  "reconcile-f",
			events: nil,
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			label:  "drain-b",
			events: []Event{{Kind: EventKindBeginDrainNode, NodeID: "b"}},
			policy: ReconfigurationPolicy{},
		},
		{
			label:  "dead-d",
			events: []Event{{Kind: EventKindMarkNodeDead, NodeID: "d"}},
			policy: ReconfigurationPolicy{},
		},
	})

	assertProducedStateInvariants(t, final)
}

func TestSimulationFailureHeavyHistory(t *testing.T) {
	initial := mustBuildInitialState(t, Config{SlotCount: 12, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d", "e", "f", "g"))

	final := runDeterministicHistory(t, "failure-heavy", initial, []historyAction{
		{
			label:  "dead-b",
			events: []Event{{Kind: EventKindMarkNodeDead, NodeID: "b"}},
			policy: ReconfigurationPolicy{},
		},
		{
			label:  "dead-d",
			events: []Event{{Kind: EventKindMarkNodeDead, NodeID: "d"}},
			policy: ReconfigurationPolicy{},
		},
		{
			label:  "add-h",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("h")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			label:  "drain-a",
			events: []Event{{Kind: EventKindBeginDrainNode, NodeID: "a"}},
			policy: ReconfigurationPolicy{},
		},
	})

	assertProducedStateInvariants(t, final)
}

func TestSimulationBudgetPressureHistory(t *testing.T) {
	initial := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	final := runDeterministicHistory(t, "budget-pressure", initial, []historyAction{
		{
			label:  "add-d-no-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("d")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
			checkPlan: func(t *testing.T, plan *ReconfigurationPlan) {
				if got, want := plan.UnassignedNodeIDs, []string{"d"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("unassigned nodes = %v, want %v", got, want)
				}
			},
		},
		{
			label:  "reconcile-d",
			events: nil,
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			label:  "add-e-no-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("e")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
		},
		{
			label:  "reconcile-e",
			events: nil,
			policy: ReconfigurationPolicy{MaxChangedChains: 2},
		},
	})

	assertProducedStateInvariants(t, final)
	if len(collectUnassignedNodeIDs(*final)) != 0 {
		t.Fatalf("expected no unassigned live nodes at end, got %v", collectUnassignedNodeIDs(*final))
	}
}

func TestSimulationJoinRebalanceContinuityHistory(t *testing.T) {
	initial := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	final := runDeterministicHistory(t, "join-continuity", initial, []historyAction{
		{
			label:  "add-d",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("d")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})

	assertProducedStateInvariants(t, final)
	if containsNodeID(collectUnassignedNodeIDs(*final), "d") {
		t.Fatal("expected node d to be assigned by the end of the continuity history")
	}
}

func TestSimulationOrderSensitiveHistory(t *testing.T) {
	initial := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	left := runDeterministicHistory(t, "order-left", initial, []historyAction{
		{
			label:  "add-d-no-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("d")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
		},
		{
			label:  "add-e-with-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("e")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})

	right := runDeterministicHistory(t, "order-right", initial, []historyAction{
		{
			label:  "add-e-with-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("e")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 1},
		},
		{
			label:  "add-d-no-budget",
			events: []Event{{Kind: EventKindAddNode, Node: uniqueNode("d")}},
			policy: ReconfigurationPolicy{MaxChangedChains: 0},
		},
	})

	assertProducedStateInvariants(t, left)
	assertProducedStateInvariants(t, right)
	if reflect.DeepEqual(left, right) {
		t.Fatal("expected different final states for different caller-ordered histories")
	}
}

func TestSimulationPathologicalHistoryFailsDeterministically(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateJoining},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": {ID: "a", FailureDomains: map[string]string{"rack": "r1", "az": "az1"}},
			"b": {ID: "b", FailureDomains: map[string]string{"rack": "r2", "az": "az2"}},
			"c": {ID: "c", FailureDomains: map[string]string{"rack": "r3", "az": "az3"}},
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c"},
		SlotCount:         1,
		ReplicationFactor: 2,
	}

	runDeterministicHistory(t, "pathological-broken-anchor", &state, []historyAction{
		{
			label:       "dead-active-anchor",
			events:      []Event{{Kind: EventKindMarkNodeDead, NodeID: "a"}},
			policy:      ReconfigurationPolicy{},
			expectError: ErrBrokenChain,
		},
	})
}

func runDeterministicHistory(
	t *testing.T,
	name string,
	initial *ClusterState,
	actions []historyAction,
) *ClusterState {
	t.Helper()

	current := cloneStateForSimulation(t, initial)
	assertProducedStateInvariants(t, current)

	for stepIndex, action := range actions {
		plan, err := PlanReconfiguration(*current, action.events, action.policy)
		deterministicPlan, deterministicErr := PlanReconfiguration(*current, action.events, action.policy)
		if !samePlanningOutcome(plan, err, deterministicPlan, deterministicErr) {
			t.Fatalf(
				"%s step %d (%s): non-deterministic planning\nevents=%v\npolicy=%+v\nleftErr=%v\nrightErr=%v",
				name,
				stepIndex,
				action.label,
				action.events,
				action.policy,
				err,
				deterministicErr,
			)
		}

		if action.expectError != nil {
			if err == nil {
				t.Fatalf("%s step %d (%s): expected error %v", name, stepIndex, action.label, action.expectError)
			}
			if !errors.Is(err, action.expectError) {
				t.Fatalf("%s step %d (%s): error = %v, want %v", name, stepIndex, action.label, err, action.expectError)
			}
			return current
		}
		if err != nil {
			t.Fatalf(
				"%s step %d (%s): PlanReconfiguration returned error: %v\nevents=%v\npolicy=%+v",
				name,
				stepIndex,
				action.label,
				err,
				action.events,
				action.policy,
			)
		}

		assertProducedPlanInvariants(t, *current, plan)
		if action.checkPlan != nil {
			action.checkPlan(t, plan)
		}

		current = applyPlanProgressAndAssert(t, historyLabel(name, stepIndex, action.label), *plan, action.policy)
	}

	return current
}

func cloneStateForSimulation(t *testing.T, state *ClusterState) *ClusterState {
	t.Helper()
	cloned, err := cloneAndValidateState(*state)
	if err != nil {
		t.Fatalf("cloneAndValidateState returned error: %v", err)
	}
	return &cloned
}

func historyLabel(name string, stepIndex int, label string) string {
	return name + "-step-" + string(rune('0'+stepIndex)) + "-" + label
}
