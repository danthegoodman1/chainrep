package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestInMemoryBackendStagesThenCommitsSequences(t *testing.T) {
	backend := NewInMemoryBackend()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := backend.StagePut(1, 1, "k1", "v1"); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := backend.StageDelete(1, 2, "k1"); err != nil {
		t.Fatalf("StageDelete returned error: %v", err)
	}

	committed, err := backend.CommittedSnapshot(1)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	if len(committed) != 0 {
		t.Fatalf("committed snapshot before commit = %v, want empty", committed)
	}
	if got, want := mustStagedSequences(t, backend, 1), []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}

	if err := backend.CommitSequence(1, 1); err != nil {
		t.Fatalf("CommitSequence(1) returned error: %v", err)
	}
	if got, want := mustCommittedSnapshot(t, backend, 1), (Snapshot{"k1": "v1"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot after seq1 = %v, want %v", got, want)
	}
	if got, want := mustStagedSequences(t, backend, 1), []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences after seq1 = %v, want %v", got, want)
	}

	if err := backend.CommitSequence(1, 2); err != nil {
		t.Fatalf("CommitSequence(2) returned error: %v", err)
	}
	if got, want := mustCommittedSnapshot(t, backend, 1), (Snapshot{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot after seq2 = %v, want %v", got, want)
	}
	if got := mustStagedSequences(t, backend, 1); len(got) != 0 {
		t.Fatalf("staged sequences after seq2 = %v, want empty", got)
	}
}

func TestSingleReplicaSubmitPutAndDeleteCommitImmediately(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, Config{NodeID: "node-a"}, backend, coord, transport)

	mustActivateReplica(t, node, 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})

	result, err := node.SubmitPut(ctx, 1, "k", "v")
	if err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	if got, want := result, (CommitResult{Slot: 1, Sequence: 1}); !reflect.DeepEqual(got, want) {
		t.Fatalf("commit result = %#v, want %#v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 1), (Snapshot{"k": "v"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustHighestCommitted(t, node, 1), uint64(1); got != want {
		t.Fatalf("highest committed = %d, want %d", got, want)
	}

	result, err = node.SubmitDelete(ctx, 1, "k")
	if err != nil {
		t.Fatalf("SubmitDelete returned error: %v", err)
	}
	if got, want := result, (CommitResult{Slot: 1, Sequence: 2}); !reflect.DeepEqual(got, want) {
		t.Fatalf("commit result = %#v, want %#v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 1), (Snapshot{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
	if got := mustNodeStagedSequences(t, node, 1); len(got) != 0 {
		t.Fatalf("staged sequences = %v, want empty", got)
	}
}

func TestHeadMiddleTailPutAndDeleteReplicateAndCommit(t *testing.T) {
	ctx := context.Background()
	nodes, _, _ := setupActiveChain(t, 7, []string{"head", "mid", "tail"})

	putResult, err := nodes["head"].SubmitPut(ctx, 7, "k", "v")
	if err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	if got, want := putResult, (CommitResult{Slot: 7, Sequence: 1}); !reflect.DeepEqual(got, want) {
		t.Fatalf("put result = %#v, want %#v", got, want)
	}
	assertCommittedStateEqual(t, nodes, 7, Snapshot{"k": "v"}, 1)

	deleteResult, err := nodes["head"].SubmitDelete(ctx, 7, "k")
	if err != nil {
		t.Fatalf("SubmitDelete returned error: %v", err)
	}
	if got, want := deleteResult, (CommitResult{Slot: 7, Sequence: 2}); !reflect.DeepEqual(got, want) {
		t.Fatalf("delete result = %#v, want %#v", got, want)
	}
	assertCommittedStateEqual(t, nodes, 7, Snapshot{}, 2)
}

func TestPipelineStagesLaterWritesBeforeEarlierCommitAndCommitsInOrder(t *testing.T) {
	ctx := context.Background()
	transport := &scriptedTransport{}
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, Config{NodeID: "mid"}, backend, coord, transport)
	mustActivateReplica(t, node, 5, ReplicaAssignment{
		Slot:         5,
		ChainVersion: 1,
		Role:         ReplicaRoleMiddle,
		Peers: ChainPeers{
			PredecessorNodeID: "head",
			SuccessorNodeID:   "tail",
		},
	})

	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:  WriteOperation{Slot: 5, Sequence: 1, Kind: OperationKindPut, Key: "k1", Value: "v1"},
		FromNodeID: "head",
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=1) returned error: %v", err)
	}
	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:  WriteOperation{Slot: 5, Sequence: 2, Kind: OperationKindPut, Key: "k2", Value: "v2"},
		FromNodeID: "head",
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=2) returned error: %v", err)
	}
	if got, want := mustNodeStagedSequences(t, node, 5), []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}

	if err := node.HandleCommitWrite(ctx, CommitWriteRequest{Slot: 5, Sequence: 2, FromNodeID: "tail"}); err == nil {
		t.Fatal("HandleCommitWrite(seq=2) unexpectedly succeeded before seq=1")
	} else if !errors.Is(err, ErrSequenceMismatch) {
		t.Fatalf("error = %v, want sequence mismatch", err)
	}

	if err := node.HandleCommitWrite(ctx, CommitWriteRequest{Slot: 5, Sequence: 1, FromNodeID: "tail"}); err != nil {
		t.Fatalf("HandleCommitWrite(seq=1) returned error: %v", err)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 5), (Snapshot{"k1": "v1"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot after seq1 = %v, want %v", got, want)
	}
	if got, want := mustNodeStagedSequences(t, node, 5), []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences after seq1 = %v, want %v", got, want)
	}

	if err := node.HandleCommitWrite(ctx, CommitWriteRequest{Slot: 5, Sequence: 2, FromNodeID: "tail"}); err != nil {
		t.Fatalf("HandleCommitWrite(seq=2) returned error: %v", err)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 5), (Snapshot{"k1": "v1", "k2": "v2"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot after seq2 = %v, want %v", got, want)
	}
	if got := mustNodeStagedSequences(t, node, 5); len(got) != 0 {
		t.Fatalf("staged sequences after seq2 = %v, want empty", got)
	}
}

func TestOutOfOrderForwardAndCommitRequestsAreRejected(t *testing.T) {
	ctx := context.Background()
	transport := &scriptedTransport{}
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, Config{NodeID: "mid"}, backend, coord, transport)
	mustActivateReplica(t, node, 5, ReplicaAssignment{
		Slot:         5,
		ChainVersion: 1,
		Role:         ReplicaRoleMiddle,
		Peers:        ChainPeers{PredecessorNodeID: "head", SuccessorNodeID: "tail"},
	})

	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:  WriteOperation{Slot: 5, Sequence: 2, Kind: OperationKindPut, Key: "k", Value: "v"},
		FromNodeID: "head",
	}); err == nil {
		t.Fatal("HandleForwardWrite(seq=2) unexpectedly succeeded")
	} else if !errors.Is(err, ErrSequenceMismatch) {
		t.Fatalf("error = %v, want sequence mismatch", err)
	}

	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:  WriteOperation{Slot: 5, Sequence: 1, Kind: OperationKindPut, Key: "k", Value: "v"},
		FromNodeID: "head",
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=1) returned error: %v", err)
	}
	if err := node.HandleCommitWrite(ctx, CommitWriteRequest{Slot: 5, Sequence: 2, FromNodeID: "tail"}); err == nil {
		t.Fatal("HandleCommitWrite(seq=2) unexpectedly succeeded")
	} else if !errors.Is(err, ErrSequenceMismatch) {
		t.Fatalf("error = %v, want sequence mismatch", err)
	}
}

func TestWriteValidationAndDownstreamFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("non-head rejected", func(t *testing.T) {
		nodes, _, _ := setupActiveChain(t, 3, []string{"head", "mid", "tail"})
		if _, err := nodes["mid"].SubmitPut(ctx, 3, "k", "v"); err == nil {
			t.Fatal("SubmitPut on middle unexpectedly succeeded")
		} else if !errors.Is(err, ErrWriteRejected) {
			t.Fatalf("error = %v, want write rejected", err)
		}
	})

	t.Run("inactive rejected", func(t *testing.T) {
		transport := NewInMemoryReplicationTransport()
		backend := NewInMemoryBackend()
		coord := NewInMemoryCoordinatorClient()
		node := mustNewNode(t, Config{NodeID: "a"}, backend, coord, transport)
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail returned error: %v", err)
		}
		if _, err := node.SubmitPut(ctx, 1, "k", "v"); err == nil {
			t.Fatal("SubmitPut unexpectedly succeeded for catching_up replica")
		} else if !errors.Is(err, ErrWriteRejected) {
			t.Fatalf("error = %v, want write rejected", err)
		}
	})

	t.Run("peer mismatch rejected", func(t *testing.T) {
		transport := &scriptedTransport{}
		backend := NewInMemoryBackend()
		coord := NewInMemoryCoordinatorClient()
		node := mustNewNode(t, Config{NodeID: "mid"}, backend, coord, transport)
		mustActivateReplica(t, node, 4, ReplicaAssignment{
			Slot:         4,
			ChainVersion: 1,
			Role:         ReplicaRoleMiddle,
			Peers:        ChainPeers{PredecessorNodeID: "head", SuccessorNodeID: "tail"},
		})
		if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
			Operation:  WriteOperation{Slot: 4, Sequence: 1, Kind: OperationKindPut, Key: "k", Value: "v"},
			FromNodeID: "wrong",
		}); err == nil {
			t.Fatal("HandleForwardWrite unexpectedly succeeded")
		} else if !errors.Is(err, ErrPeerMismatch) {
			t.Fatalf("error = %v, want peer mismatch", err)
		}
	})

	t.Run("unknown slot rejected", func(t *testing.T) {
		transport := NewInMemoryReplicationTransport()
		node := mustNewNode(t, Config{NodeID: "a"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
		if _, err := node.SubmitPut(ctx, 999, "k", "v"); err == nil {
			t.Fatal("SubmitPut unexpectedly succeeded on unknown slot")
		} else if !errors.Is(err, ErrUnknownReplica) {
			t.Fatalf("error = %v, want unknown replica", err)
		}
	})

	t.Run("downstream failure leaves staged but uncommitted", func(t *testing.T) {
		transport := &scriptedTransport{
			forwardErr: errors.New("downstream unavailable"),
		}
		backend := NewInMemoryBackend()
		coord := NewInMemoryCoordinatorClient()
		node := mustNewNode(t, Config{NodeID: "head"}, backend, coord, transport)
		mustActivateReplica(t, node, 6, ReplicaAssignment{
			Slot:         6,
			ChainVersion: 1,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		})

		if _, err := node.SubmitPut(ctx, 6, "k", "v"); err == nil {
			t.Fatal("SubmitPut unexpectedly succeeded")
		} else if !containsError(err, "downstream unavailable") {
			t.Fatalf("error = %v, want downstream unavailable context", err)
		}
		if got, want := mustNodeCommittedSnapshot(t, node, 6), (Snapshot{}); !reflect.DeepEqual(got, want) {
			t.Fatalf("committed snapshot = %v, want empty", got)
		}
		if got, want := mustNodeStagedSequences(t, node, 6), []uint64{1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("staged sequences = %v, want %v", got, want)
		}
		if got, want := mustHighestCommitted(t, node, 6), uint64(0); got != want {
			t.Fatalf("highest committed = %d, want %d", got, want)
		}
	})
}

func TestMultiSlotIndependenceAndDeterministicHistory(t *testing.T) {
	left := runReplicationHistory(t)
	right := runReplicationHistory(t)

	if !reflect.DeepEqual(left.finalStates, right.finalStates) {
		t.Fatalf("final states mismatch\nleft=%v\nright=%v", left.finalStates, right.finalStates)
	}
	if !reflect.DeepEqual(left.highestCommitted, right.highestCommitted) {
		t.Fatalf("highest committed mismatch\nleft=%v\nright=%v", left.highestCommitted, right.highestCommitted)
	}
}

type replicationHistory struct {
	finalStates      map[string]map[int]Snapshot
	highestCommitted map[string]map[int]uint64
}

func runReplicationHistory(t *testing.T) replicationHistory {
	t.Helper()
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	nodes := map[string]*Node{}
	backends := map[string]*InMemoryBackend{}
	for _, nodeID := range []string{"a", "b", "c"} {
		backend := NewInMemoryBackend()
		backends[nodeID] = backend
		transport.Register(nodeID, backend)
		node := mustNewNode(t, Config{NodeID: nodeID}, backend, NewInMemoryCoordinatorClient(), transport)
		nodes[nodeID] = node
		transport.RegisterNode(nodeID, node)
	}
	setupChainOnExistingNodes(t, nodes, 11, []string{"a", "b", "c"})
	setupChainOnExistingNodes(t, nodes, 12, []string{"c", "b", "a"})

	if _, err := nodes["a"].SubmitPut(ctx, 11, "x", "1"); err != nil {
		t.Fatalf("SubmitPut(slot=11) returned error: %v", err)
	}
	if _, err := nodes["a"].SubmitPut(ctx, 11, "y", "2"); err != nil {
		t.Fatalf("SubmitPut(slot=11 second) returned error: %v", err)
	}
	if _, err := nodes["c"].SubmitPut(ctx, 12, "z", "9"); err != nil {
		t.Fatalf("SubmitPut(slot=12) returned error: %v", err)
	}
	if _, err := nodes["a"].SubmitDelete(ctx, 11, "x"); err != nil {
		t.Fatalf("SubmitDelete(slot=11) returned error: %v", err)
	}

	result := replicationHistory{
		finalStates:      map[string]map[int]Snapshot{},
		highestCommitted: map[string]map[int]uint64{},
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		result.finalStates[nodeID] = map[int]Snapshot{}
		result.highestCommitted[nodeID] = map[int]uint64{}
		for _, slot := range []int{11, 12} {
			snapshot, err := nodes[nodeID].CommittedSnapshot(slot)
			if err == nil {
				result.finalStates[nodeID][slot] = snapshot
				result.highestCommitted[nodeID][slot] = mustHighestCommitted(t, nodes[nodeID], slot)
			}
		}
	}
	if !reflect.DeepEqual(result.finalStates["a"][11], Snapshot{"y": "2"}) {
		t.Fatalf("slot 11 state = %v, want %v", result.finalStates["a"][11], Snapshot{"y": "2"})
	}
	if !reflect.DeepEqual(result.finalStates["c"][12], Snapshot{"z": "9"}) {
		t.Fatalf("slot 12 state = %v, want %v", result.finalStates["c"][12], Snapshot{"z": "9"})
	}
	return result
}

type scriptedTransport struct {
	forwardErr error
	commitErr  error
	forwards   []ForwardWriteRequest
	commits    []CommitWriteRequest
}

func (t *scriptedTransport) FetchSnapshot(_ context.Context, _ string, _ int) (Snapshot, error) {
	return Snapshot{}, nil
}

func (t *scriptedTransport) ForwardWrite(_ context.Context, _ string, req ForwardWriteRequest) error {
	t.forwards = append(t.forwards, req)
	if t.forwardErr != nil {
		return t.forwardErr
	}
	return nil
}

func (t *scriptedTransport) CommitWrite(_ context.Context, _ string, req CommitWriteRequest) error {
	t.commits = append(t.commits, req)
	if t.commitErr != nil {
		return t.commitErr
	}
	return nil
}

func setupActiveChain(
	t *testing.T,
	slot int,
	nodeIDs []string,
) (map[string]*Node, map[string]*InMemoryBackend, *InMemoryReplicationTransport) {
	t.Helper()
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	nodes := make(map[string]*Node, len(nodeIDs))
	backends := make(map[string]*InMemoryBackend, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := NewInMemoryBackend()
		backends[nodeID] = backend
		transport.Register(nodeID, backend)
		node := mustNewNode(t, Config{NodeID: nodeID}, backend, NewInMemoryCoordinatorClient(), transport)
		nodes[nodeID] = node
		transport.RegisterNode(nodeID, node)
	}

	mustActivateReplica(t, nodes[nodeIDs[0]], slot, ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})
	for i := 1; i < len(nodeIDs); i++ {
		if err := nodes[nodeIDs[i]].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{
				Slot:         slot,
				ChainVersion: 1,
				Role:         ReplicaRoleTail,
				Peers:        ChainPeers{PredecessorNodeID: nodeIDs[i-1]},
			},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(%q) returned error: %v", nodeIDs[i], err)
		}
		if err := nodes[nodeIDs[i]].ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(%q) returned error: %v", nodeIDs[i], err)
		}
	}
	for i, nodeID := range nodeIDs {
		role := ReplicaRoleMiddle
		switch {
		case len(nodeIDs) == 1:
			role = ReplicaRoleSingle
		case i == 0:
			role = ReplicaRoleHead
		case i == len(nodeIDs)-1:
			role = ReplicaRoleTail
		}
		assignment := ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         role,
		}
		if i > 0 {
			assignment.Peers.PredecessorNodeID = nodeIDs[i-1]
		}
		if i+1 < len(nodeIDs) {
			assignment.Peers.SuccessorNodeID = nodeIDs[i+1]
		}
		if err := nodes[nodeID].UpdateChainPeers(ctx, UpdateChainPeersCommand{Assignment: assignment}); err != nil {
			t.Fatalf("UpdateChainPeers(%q) returned error: %v", nodeID, err)
		}
	}
	return nodes, backends, transport
}

func setupChainOnExistingNodes(t *testing.T, nodes map[string]*Node, slot int, nodeIDs []string) {
	t.Helper()
	ctx := context.Background()
	mustActivateReplica(t, nodes[nodeIDs[0]], slot, ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})
	for i := 1; i < len(nodeIDs); i++ {
		if err := nodes[nodeIDs[i]].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{
				Slot:         slot,
				ChainVersion: 1,
				Role:         ReplicaRoleTail,
				Peers:        ChainPeers{PredecessorNodeID: nodeIDs[i-1]},
			},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(%q) returned error: %v", nodeIDs[i], err)
		}
		if err := nodes[nodeIDs[i]].ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(%q) returned error: %v", nodeIDs[i], err)
		}
	}
	for i, nodeID := range nodeIDs {
		role := ReplicaRoleMiddle
		switch {
		case len(nodeIDs) == 1:
			role = ReplicaRoleSingle
		case i == 0:
			role = ReplicaRoleHead
		case i == len(nodeIDs)-1:
			role = ReplicaRoleTail
		}
		assignment := ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         role,
		}
		if i > 0 {
			assignment.Peers.PredecessorNodeID = nodeIDs[i-1]
		}
		if i+1 < len(nodeIDs) {
			assignment.Peers.SuccessorNodeID = nodeIDs[i+1]
		}
		if err := nodes[nodeID].UpdateChainPeers(ctx, UpdateChainPeersCommand{Assignment: assignment}); err != nil {
			t.Fatalf("UpdateChainPeers(%q) returned error: %v", nodeID, err)
		}
	}
}

func mustActivateReplica(t *testing.T, node *Node, slot int, assignment ReplicaAssignment) {
	t.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func assertCommittedStateEqual(t *testing.T, nodes map[string]*Node, slot int, want Snapshot, wantSequence uint64) {
	t.Helper()
	for nodeID, node := range nodes {
		if got := mustNodeCommittedSnapshot(t, node, slot); !reflect.DeepEqual(got, want) {
			t.Fatalf("node %q committed snapshot = %v, want %v", nodeID, got, want)
		}
		if got, wantSeq := mustHighestCommitted(t, node, slot), wantSequence; got != wantSeq {
			t.Fatalf("node %q highest committed = %d, want %d", nodeID, got, wantSeq)
		}
		if staged := mustNodeStagedSequences(t, node, slot); len(staged) != 0 {
			t.Fatalf("node %q staged sequences = %v, want empty", nodeID, staged)
		}
	}
}

func mustCommittedSnapshot(t *testing.T, backend *InMemoryBackend, slot int) Snapshot {
	t.Helper()
	snapshot, err := backend.CommittedSnapshot(slot)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	return snapshot
}

func mustStagedSequences(t *testing.T, backend *InMemoryBackend, slot int) []uint64 {
	t.Helper()
	sequences, err := backend.StagedSequences(slot)
	if err != nil {
		t.Fatalf("StagedSequences returned error: %v", err)
	}
	return sequences
}

func mustNodeCommittedSnapshot(t *testing.T, node *Node, slot int) Snapshot {
	t.Helper()
	snapshot, err := node.CommittedSnapshot(slot)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	return snapshot
}

func mustNodeStagedSequences(t *testing.T, node *Node, slot int) []uint64 {
	t.Helper()
	sequences, err := node.StagedSequences(slot)
	if err != nil {
		t.Fatalf("StagedSequences returned error: %v", err)
	}
	return sequences
}

func mustHighestCommitted(t *testing.T, node *Node, slot int) uint64 {
	t.Helper()
	sequence, err := node.HighestCommittedSequence(slot)
	if err != nil {
		t.Fatalf("HighestCommittedSequence returned error: %v", err)
	}
	return sequence
}

func containsError(err error, substring string) bool {
	return err != nil && strings.Contains(err.Error(), substring)
}
