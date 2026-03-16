package coordinator

import (
	"fmt"
	"testing"
)

func BenchmarkBuildInitialPlacement_4096Slots_128Nodes(b *testing.B) {
	cfg := Config{
		SlotCount:         4096,
		ReplicationFactor: 3,
	}
	nodes := benchmarkNodes(128)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := BuildInitialPlacement(cfg, nodes)
		if err != nil {
			b.Fatalf("BuildInitialPlacement returned error: %v", err)
		}
	}
}

func BenchmarkPlanReconfiguration_AddNode_NoRebalance_4096Slots_128Nodes(b *testing.B) {
	state := mustBuildInitialStateFromBenchmark(b, Config{
		SlotCount:         4096,
		ReplicationFactor: 3,
	}, benchmarkNodes(128))

	event := Event{
		Kind: EventKindAddNode,
		Node: benchmarkNode(128),
	}
	policy := ReconfigurationPolicy{MaxChangedChains: 0}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := PlanReconfiguration(*state, []Event{event}, policy)
		if err != nil {
			b.Fatalf("PlanReconfiguration returned error: %v", err)
		}
	}
}

func BenchmarkPlanReconfiguration_AddNode_Budget8_4096Slots_128Nodes(b *testing.B) {
	state := mustBuildInitialStateFromBenchmark(b, Config{
		SlotCount:         4096,
		ReplicationFactor: 3,
	}, benchmarkNodes(128))

	event := Event{
		Kind: EventKindAddNode,
		Node: benchmarkNode(128),
	}
	// Larger budgets can trigger rejected disjoint-remap plans on this topology.
	// Budget 8 exercises a successful bounded rebalance for the benchmark.
	policy := ReconfigurationPolicy{MaxChangedChains: 8}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := PlanReconfiguration(*state, []Event{event}, policy)
		if err != nil {
			b.Fatalf("PlanReconfiguration returned error: %v", err)
		}
	}
}

func BenchmarkPlanReconfiguration_MarkNodeDead_4096Slots_128Nodes(b *testing.B) {
	state := mustBuildInitialStateFromBenchmark(b, Config{
		SlotCount:         4096,
		ReplicationFactor: 3,
	}, benchmarkNodes(128))

	event := Event{
		Kind:   EventKindMarkNodeDead,
		NodeID: benchmarkNodeID(64),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := PlanReconfiguration(*state, []Event{event}, ReconfigurationPolicy{})
		if err != nil {
			b.Fatalf("PlanReconfiguration returned error: %v", err)
		}
	}
}

func BenchmarkPlanReconfiguration_BeginDrainNode_4096Slots_128Nodes(b *testing.B) {
	state := mustBuildInitialStateFromBenchmark(b, Config{
		SlotCount:         4096,
		ReplicationFactor: 3,
	}, benchmarkNodes(128))

	event := Event{
		Kind:   EventKindBeginDrainNode,
		NodeID: benchmarkNodeID(64),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := PlanReconfiguration(*state, []Event{event}, ReconfigurationPolicy{})
		if err != nil {
			b.Fatalf("PlanReconfiguration returned error: %v", err)
		}
	}
}

func mustBuildInitialStateFromBenchmark(b *testing.B, cfg Config, nodes []Node) *ClusterState {
	b.Helper()

	state, err := BuildInitialPlacement(cfg, nodes)
	if err != nil {
		b.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	return state
}

func benchmarkNodes(count int) []Node {
	nodes := make([]Node, 0, count)
	for i := 0; i < count; i++ {
		nodes = append(nodes, benchmarkNode(i))
	}
	return nodes
}

func benchmarkNode(index int) Node {
	id := benchmarkNodeID(index)
	return Node{
		ID: id,
		FailureDomains: map[string]string{
			"host": fmt.Sprintf("host-%03d", index),
			"rack": fmt.Sprintf("rack-%03d", index),
			"az":   fmt.Sprintf("az-%03d", index),
		},
	}
}

func benchmarkNodeID(index int) string {
	return fmt.Sprintf("node-%03d", index)
}
