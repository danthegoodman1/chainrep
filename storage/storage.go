package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrInvalidConfig             = errors.New("invalid storage config")
	ErrReplicaExists             = errors.New("storage replica already exists")
	ErrUnknownReplica            = errors.New("unknown storage replica")
	ErrInvalidTransition         = errors.New("invalid storage replica transition")
	ErrSnapshotSourceUnavailable = errors.New("storage snapshot source unavailable")
)

type Config struct {
	NodeID string
}

type Snapshot map[string]string

type Backend interface {
	CreateReplica(slot int) error
	DeleteReplica(slot int) error
	Snapshot(slot int) (Snapshot, error)
	InstallSnapshot(slot int, snap Snapshot) error
}

type CoordinatorClient interface {
	ReportReplicaReady(ctx context.Context, slot int) error
	ReportReplicaRemoved(ctx context.Context, slot int) error
	ReportNodeHeartbeat(ctx context.Context, status NodeStatus) error
}

type ReplicationTransport interface {
	FetchSnapshot(ctx context.Context, fromNodeID string, slot int) (Snapshot, error)
}

type ReplicaState string

const (
	ReplicaStatePending    ReplicaState = "pending"
	ReplicaStateCatchingUp ReplicaState = "catching_up"
	ReplicaStateActive     ReplicaState = "active"
	ReplicaStateLeaving    ReplicaState = "leaving"
	ReplicaStateRemoved    ReplicaState = "removed"
)

type ReplicaRole string

const (
	ReplicaRoleSingle ReplicaRole = "single"
	ReplicaRoleHead   ReplicaRole = "head"
	ReplicaRoleMiddle ReplicaRole = "middle"
	ReplicaRoleTail   ReplicaRole = "tail"
)

type ChainPeers struct {
	PredecessorNodeID string
	SuccessorNodeID   string
}

type ReplicaAssignment struct {
	Slot         int
	ChainVersion uint64
	Role         ReplicaRole
	Peers        ChainPeers
}

type ReplicaStatus struct {
	Assignment ReplicaAssignment
	State      ReplicaState
}

type NodeState struct {
	NodeID   string
	Replicas map[int]ReplicaStatus
}

type NodeStatus struct {
	NodeID          string
	ReplicaCount    int
	ActiveCount     int
	CatchingUpCount int
	LeavingCount    int
}

type AddReplicaAsTailCommand struct {
	Assignment ReplicaAssignment
}

type ActivateReplicaCommand struct {
	Slot int
}

type MarkReplicaLeavingCommand struct {
	Slot int
}

type RemoveReplicaCommand struct {
	Slot int
}

type UpdateChainPeersCommand struct {
	Assignment ReplicaAssignment
}

type replicaRecord struct {
	assignment ReplicaAssignment
	state      ReplicaState
}

type Node struct {
	nodeID   string
	backend  Backend
	coord    CoordinatorClient
	repl     ReplicationTransport
	replicas map[int]replicaRecord
}

func NewNode(cfg Config, backend Backend, coord CoordinatorClient, repl ReplicationTransport) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("%w: node ID must not be empty", ErrInvalidConfig)
	}
	if backend == nil {
		return nil, fmt.Errorf("%w: backend must not be nil", ErrInvalidConfig)
	}
	if coord == nil {
		return nil, fmt.Errorf("%w: coordinator client must not be nil", ErrInvalidConfig)
	}
	if repl == nil {
		return nil, fmt.Errorf("%w: replication transport must not be nil", ErrInvalidConfig)
	}

	return &Node{
		nodeID:   cfg.NodeID,
		backend:  backend,
		coord:    coord,
		repl:     repl,
		replicas: make(map[int]replicaRecord),
	}, nil
}

func (n *Node) AddReplicaAsTail(ctx context.Context, cmd AddReplicaAsTailCommand) error {
	if cmd.Assignment.Slot < 0 {
		return fmt.Errorf("%w: slot must be >= 0", ErrInvalidConfig)
	}
	if _, exists := n.replicas[cmd.Assignment.Slot]; exists {
		return fmt.Errorf("%w: slot %d", ErrReplicaExists, cmd.Assignment.Slot)
	}

	if err := n.backend.CreateReplica(cmd.Assignment.Slot); err != nil {
		return fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}

	rollback := true
	defer func() {
		if rollback {
			_ = n.backend.DeleteReplica(cmd.Assignment.Slot)
		}
	}()

	record := replicaRecord{
		assignment: cloneAssignment(cmd.Assignment),
		state:      ReplicaStatePending,
	}
	n.replicas[cmd.Assignment.Slot] = record

	if sourceNodeID := cmd.Assignment.Peers.PredecessorNodeID; sourceNodeID != "" {
		snapshot, err := n.repl.FetchSnapshot(ctx, sourceNodeID, cmd.Assignment.Slot)
		if err != nil {
			delete(n.replicas, cmd.Assignment.Slot)
			return fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
		}
		if err := n.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
			delete(n.replicas, cmd.Assignment.Slot)
			return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
		}
	}

	record.state = ReplicaStateCatchingUp
	n.replicas[cmd.Assignment.Slot] = record
	rollback = false
	return nil
}

func (n *Node) ActivateReplica(ctx context.Context, cmd ActivateReplicaCommand) error {
	record, ok := n.replicas[cmd.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateCatchingUp {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}
	if err := n.coord.ReportReplicaReady(ctx, cmd.Slot); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaReady: %w", err)
	}

	record.state = ReplicaStateActive
	n.replicas[cmd.Slot] = record
	return nil
}

func (n *Node) MarkReplicaLeaving(_ context.Context, cmd MarkReplicaLeavingCommand) error {
	record, ok := n.replicas[cmd.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateActive {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}

	record.state = ReplicaStateLeaving
	n.replicas[cmd.Slot] = record
	return nil
}

func (n *Node) RemoveReplica(ctx context.Context, cmd RemoveReplicaCommand) error {
	record, ok := n.replicas[cmd.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateLeaving && record.state != ReplicaStateRemoved {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}

	if record.state == ReplicaStateLeaving {
		if err := n.backend.DeleteReplica(cmd.Slot); err != nil {
			return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
		}
		record.state = ReplicaStateRemoved
		n.replicas[cmd.Slot] = record
	}
	if err := n.coord.ReportReplicaRemoved(ctx, cmd.Slot); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaRemoved: %w", err)
	}

	delete(n.replicas, cmd.Slot)
	return nil
}

func (n *Node) UpdateChainPeers(_ context.Context, cmd UpdateChainPeersCommand) error {
	record, ok := n.replicas[cmd.Assignment.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	n.replicas[cmd.Assignment.Slot] = record
	return nil
}

func (n *Node) ReportHeartbeat(ctx context.Context) error {
	status := n.snapshotNodeStatus()
	if err := n.coord.ReportNodeHeartbeat(ctx, status); err != nil {
		return fmt.Errorf("err in n.coord.ReportNodeHeartbeat: %w", err)
	}
	return nil
}

func (n *Node) State() NodeState {
	state := NodeState{
		NodeID:   n.nodeID,
		Replicas: make(map[int]ReplicaStatus, len(n.replicas)),
	}
	for slot, record := range n.replicas {
		state.Replicas[slot] = ReplicaStatus{
			Assignment: cloneAssignment(record.assignment),
			State:      record.state,
		}
	}
	return state
}

func (n *Node) snapshotNodeStatus() NodeStatus {
	status := NodeStatus{NodeID: n.nodeID}
	slots := make([]int, 0, len(n.replicas))
	for slot := range n.replicas {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	for _, slot := range slots {
		record := n.replicas[slot]
		status.ReplicaCount++
		switch record.state {
		case ReplicaStateActive:
			status.ActiveCount++
		case ReplicaStateCatchingUp:
			status.CatchingUpCount++
		case ReplicaStateLeaving:
			status.LeavingCount++
		}
	}
	return status
}

func cloneAssignment(assignment ReplicaAssignment) ReplicaAssignment {
	return ReplicaAssignment{
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Role:         assignment.Role,
		Peers: ChainPeers{
			PredecessorNodeID: assignment.Peers.PredecessorNodeID,
			SuccessorNodeID:   assignment.Peers.SuccessorNodeID,
		},
	}
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	cloned := make(Snapshot, len(snapshot))
	for key, value := range snapshot {
		cloned[key] = value
	}
	return cloned
}
