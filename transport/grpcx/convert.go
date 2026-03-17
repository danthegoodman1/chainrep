package grpcx

import (
	"fmt"

	"github.com/danthegoodman1/chainrep/coordinator"
	"github.com/danthegoodman1/chainrep/coordserver"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
	grpcproto "github.com/danthegoodman1/chainrep/proto/chainrep/v1"
)

func protoNode(node coordinator.Node) *grpcproto.Node {
	domains := make([]*grpcproto.FailureDomain, 0, len(node.FailureDomains))
	for key, value := range node.FailureDomains {
		domains = append(domains, &grpcproto.FailureDomain{Key: key, Value: value})
	}
	return &grpcproto.Node{
		Id:             node.ID,
		RpcAddress:     node.RPCAddress,
		FailureDomains: domains,
	}
}

func fromProtoNode(node *grpcproto.Node) coordinator.Node {
	if node == nil {
		return coordinator.Node{}
	}
	domains := make(map[string]string, len(node.FailureDomains))
	for _, domain := range node.FailureDomains {
		domains[domain.Key] = domain.Value
	}
	return coordinator.Node{
		ID:             node.Id,
		RPCAddress:     node.RpcAddress,
		FailureDomains: domains,
	}
}

func protoRoutingSnapshot(snapshot coordserver.RoutingSnapshot) *grpcproto.RoutingSnapshotResponse {
	slots := make([]*grpcproto.SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		slots = append(slots, &grpcproto.SlotRoute{
			Slot:         int32(slot.Slot),
			ChainVersion: slot.ChainVersion,
			HeadNodeId:   slot.HeadNodeID,
			HeadEndpoint: slot.HeadEndpoint,
			TailNodeId:   slot.TailNodeID,
			TailEndpoint: slot.TailEndpoint,
			Writable:     slot.Writable,
			Readable:     slot.Readable,
		})
	}
	return &grpcproto.RoutingSnapshotResponse{
		Version:   snapshot.Version,
		SlotCount: int32(snapshot.SlotCount),
		Slots:     slots,
	}
}

func fromProtoRoutingSnapshot(snapshot *grpcproto.RoutingSnapshotResponse) coordserver.RoutingSnapshot {
	if snapshot == nil {
		return coordserver.RoutingSnapshot{}
	}
	slots := make([]coordserver.SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		slots = append(slots, coordserver.SlotRoute{
			Slot:         int(slot.Slot),
			ChainVersion: slot.ChainVersion,
			HeadNodeID:   slot.HeadNodeId,
			HeadEndpoint: slot.HeadEndpoint,
			TailNodeID:   slot.TailNodeId,
			TailEndpoint: slot.TailEndpoint,
			Writable:     slot.Writable,
			Readable:     slot.Readable,
		})
	}
	return coordserver.RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: int(snapshot.SlotCount),
		Slots:     slots,
	}
}

func protoAssignment(assignment storage.ReplicaAssignment) *grpcproto.ReplicaAssignment {
	return &grpcproto.ReplicaAssignment{
		Slot:         int32(assignment.Slot),
		ChainVersion: assignment.ChainVersion,
		Role:         string(assignment.Role),
		Peers:        protoChainPeers(assignment.Peers),
	}
}

func fromProtoAssignment(assignment *grpcproto.ReplicaAssignment) storage.ReplicaAssignment {
	if assignment == nil {
		return storage.ReplicaAssignment{}
	}
	return storage.ReplicaAssignment{
		Slot:         int(assignment.Slot),
		ChainVersion: assignment.ChainVersion,
		Role:         storage.ReplicaRole(assignment.Role),
		Peers:        fromProtoChainPeers(assignment.Peers),
	}
}

func protoChainPeers(peers storage.ChainPeers) *grpcproto.ChainPeers {
	return &grpcproto.ChainPeers{
		PredecessorNodeId: peers.PredecessorNodeID,
		PredecessorTarget: peers.PredecessorTarget,
		SuccessorNodeId:   peers.SuccessorNodeID,
		SuccessorTarget:   peers.SuccessorTarget,
	}
}

func fromProtoChainPeers(peers *grpcproto.ChainPeers) storage.ChainPeers {
	if peers == nil {
		return storage.ChainPeers{}
	}
	return storage.ChainPeers{
		PredecessorNodeID: peers.PredecessorNodeId,
		PredecessorTarget: peers.PredecessorTarget,
		SuccessorNodeID:   peers.SuccessorNodeId,
		SuccessorTarget:   peers.SuccessorTarget,
	}
}

func protoNodeStatus(status storage.NodeStatus) *grpcproto.NodeStatus {
	return &grpcproto.NodeStatus{
		NodeId:          status.NodeID,
		ReplicaCount:    int32(status.ReplicaCount),
		ActiveCount:     int32(status.ActiveCount),
		CatchingUpCount: int32(status.CatchingUpCount),
		LeavingCount:    int32(status.LeavingCount),
	}
}

func fromProtoNodeStatus(status *grpcproto.NodeStatus) storage.NodeStatus {
	if status == nil {
		return storage.NodeStatus{}
	}
	return storage.NodeStatus{
		NodeID:          status.NodeId,
		ReplicaCount:    int(status.ReplicaCount),
		ActiveCount:     int(status.ActiveCount),
		CatchingUpCount: int(status.CatchingUpCount),
		LeavingCount:    int(status.LeavingCount),
	}
}

func protoNodeRecovery(report storage.NodeRecoveryReport) *grpcproto.NodeRecoveryReport {
	replicas := make([]*grpcproto.RecoveredReplica, 0, len(report.Replicas))
	for _, replica := range report.Replicas {
		replicas = append(replicas, &grpcproto.RecoveredReplica{
			Assignment:               protoAssignment(replica.Assignment),
			LastKnownState:           string(replica.LastKnownState),
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return &grpcproto.NodeRecoveryReport{
		NodeId:   report.NodeID,
		Replicas: replicas,
	}
}

func fromProtoNodeRecovery(report *grpcproto.NodeRecoveryReport) storage.NodeRecoveryReport {
	if report == nil {
		return storage.NodeRecoveryReport{}
	}
	replicas := make([]storage.RecoveredReplica, 0, len(report.Replicas))
	for _, replica := range report.Replicas {
		replicas = append(replicas, storage.RecoveredReplica{
			Assignment:               fromProtoAssignment(replica.Assignment),
			LastKnownState:           storage.ReplicaState(replica.LastKnownState),
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return storage.NodeRecoveryReport{
		NodeID:   report.NodeId,
		Replicas: replicas,
	}
}

func protoServerState(state coordruntime.State) *grpcproto.ServerState {
	return &grpcproto.ServerState{Version: state.Version}
}

func fromProtoCommitResult(result *grpcproto.CommitResult) storage.CommitResult {
	if result == nil {
		return storage.CommitResult{}
	}
	return storage.CommitResult{
		Slot:     int(result.Slot),
		Sequence: result.Sequence,
	}
}

func protoCommitResult(result storage.CommitResult) *grpcproto.CommitResult {
	return &grpcproto.CommitResult{
		Slot:     int32(result.Slot),
		Sequence: result.Sequence,
	}
}

func protoReadResult(result storage.ReadResult) *grpcproto.ReadResult {
	return &grpcproto.ReadResult{
		Slot:         int32(result.Slot),
		ChainVersion: result.ChainVersion,
		Found:        result.Found,
		Value:        result.Value,
	}
}

func fromProtoReadResult(result *grpcproto.ReadResult) storage.ReadResult {
	if result == nil {
		return storage.ReadResult{}
	}
	return storage.ReadResult{
		Slot:         int(result.Slot),
		ChainVersion: result.ChainVersion,
		Found:        result.Found,
		Value:        result.Value,
	}
}

func protoSnapshot(snapshot storage.Snapshot) []*grpcproto.SnapshotEntry {
	entries := make([]*grpcproto.SnapshotEntry, 0, len(snapshot))
	for key, value := range snapshot {
		entries = append(entries, &grpcproto.SnapshotEntry{Key: key, Value: value})
	}
	return entries
}

func snapshotFromProtoEntries(entries []*grpcproto.SnapshotEntry) (storage.Snapshot, error) {
	snapshot := make(storage.Snapshot, len(entries))
	for _, entry := range entries {
		if entry == nil {
			return nil, fmt.Errorf("nil snapshot entry")
		}
		snapshot[entry.Key] = entry.Value
	}
	return snapshot, nil
}
