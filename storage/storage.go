package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

var (
	ErrInvalidConfig             = errors.New("invalid storage config")
	ErrReplicaExists             = errors.New("storage replica already exists")
	ErrUnknownReplica            = errors.New("unknown storage replica")
	ErrInvalidTransition         = errors.New("invalid storage replica transition")
	ErrSnapshotSourceUnavailable = errors.New("storage snapshot source unavailable")
	ErrWriteRejected             = errors.New("storage write rejected")
	ErrSequenceMismatch          = errors.New("storage sequence mismatch")
	ErrPeerMismatch              = errors.New("storage peer mismatch")
	ErrStateMismatch             = errors.New("storage state mismatch")
	ErrProtocolConflict          = errors.New("storage replica protocol conflict")
	ErrReplicaBackpressure       = errors.New("storage replica backpressure")
	ErrBufferedMessageLimit      = ErrReplicaBackpressure
	ErrWriteBackpressure         = errors.New("storage client write backpressure")
	ErrCatchupBackpressure       = errors.New("storage catch-up backpressure")
	ErrRoutingMismatch           = errors.New("storage routing mismatch")
	ErrWriteTimeout              = errors.New("storage write wait timed out or was canceled")
	ErrAmbiguousWrite            = errors.New("storage client write outcome is ambiguous")
)

type Config struct {
	NodeID                            string
	MaxInFlightClientWritesPerNode    int
	MaxInFlightClientWritesPerSlot    int
	MaxBufferedReplicaMessagesPerNode int
	MaxBufferedReplicaMessagesPerSlot int
	MaxConcurrentCatchups             int
	WriteCommitTimeout                time.Duration
}

type Snapshot map[string]string

type Backend interface {
	CreateReplica(slot int) error
	DeleteReplica(slot int) error
	Snapshot(slot int) (Snapshot, error)
	InstallSnapshot(slot int, snap Snapshot) error
	SetHighestCommittedSequence(slot int, sequence uint64) error
	StagePut(slot int, sequence uint64, key string, value string) error
	StageDelete(slot int, sequence uint64, key string) error
	CommitSequence(slot int, sequence uint64) error
	CommittedSnapshot(slot int) (Snapshot, error)
	GetCommitted(slot int, key string) (string, bool, error)
	HighestCommittedSequence(slot int) (uint64, error)
	StagedSequences(slot int) ([]uint64, error)
	Close() error
}

type LocalStateStore interface {
	LoadNode(ctx context.Context, nodeID string) (PersistedNodeState, error)
	UpsertReplica(ctx context.Context, nodeID string, replica PersistedReplica) error
	DeleteReplica(ctx context.Context, nodeID string, slot int) error
	Close() error
}

type CoordinatorClient interface {
	ReportReplicaReady(ctx context.Context, slot int) error
	ReportReplicaRemoved(ctx context.Context, slot int) error
	ReportNodeRecovered(ctx context.Context, report NodeRecoveryReport) error
	ReportNodeHeartbeat(ctx context.Context, status NodeStatus) error
}

type ReplicationTransport interface {
	FetchSnapshot(ctx context.Context, fromNodeID string, slot int) (Snapshot, error)
	FetchCommittedSequence(ctx context.Context, fromNodeID string, slot int) (uint64, error)
	ForwardWrite(ctx context.Context, toNodeID string, req ForwardWriteRequest) error
	CommitWrite(ctx context.Context, toNodeID string, req CommitWriteRequest) error
}

type OperationKind string

const (
	OperationKindPut    OperationKind = "put"
	OperationKindDelete OperationKind = "delete"
)

type WriteOperation struct {
	Slot     int
	Sequence uint64
	Kind     OperationKind
	Key      string
	Value    string
}

type ForwardWriteRequest struct {
	Operation  WriteOperation
	FromNodeID string
}

type CommitWriteRequest struct {
	Slot       int
	Sequence   uint64
	FromNodeID string
}

type CommitResult struct {
	Slot     int
	Sequence uint64
}

type ClientGetRequest struct {
	Slot                 int
	Key                  string
	ExpectedChainVersion uint64
}

type ClientPutRequest struct {
	Slot                 int
	Key                  string
	Value                string
	ExpectedChainVersion uint64
}

type ClientDeleteRequest struct {
	Slot                 int
	Key                  string
	ExpectedChainVersion uint64
}

type ReadResult struct {
	Slot         int
	ChainVersion uint64
	Found        bool
	Value        string
}

type AmbiguousWriteError struct {
	Slot                 int
	Kind                 OperationKind
	ExpectedChainVersion uint64
	Cause                error
}

type BackpressureResource string

const (
	BackpressureResourceClientWrite   BackpressureResource = "client_write"
	BackpressureResourceReplicaBuffer BackpressureResource = "replica_buffer"
	BackpressureResourceCatchup       BackpressureResource = "catchup"
)

type BackpressureError struct {
	Slot     int
	Current  int
	Limit    int
	Resource BackpressureResource
	Cause    error
}

func (e *BackpressureError) Error() string {
	if e.Slot >= 0 {
		return fmt.Sprintf(
			"%s: %s slot %d current=%d limit=%d",
			e.Cause,
			e.Resource,
			e.Slot,
			e.Current,
			e.Limit,
		)
	}
	return fmt.Sprintf("%s: %s current=%d limit=%d", e.Cause, e.Resource, e.Current, e.Limit)
}

func (e *BackpressureError) Unwrap() error {
	return e.Cause
}

func (e *BackpressureError) Is(target error) bool {
	return target == e.Cause
}

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf(
		"%s: %s on slot %d version %d may or may not have committed: %v",
		ErrAmbiguousWrite,
		e.Kind,
		e.Slot,
		e.ExpectedChainVersion,
		e.Cause,
	)
}

func (e *AmbiguousWriteError) Unwrap() error {
	return e.Cause
}

func (e *AmbiguousWriteError) Is(target error) bool {
	return target == ErrAmbiguousWrite
}

type RoutingMismatchReason string

const (
	RoutingMismatchReasonUnknownSlot     RoutingMismatchReason = "unknown_slot"
	RoutingMismatchReasonWrongVersion    RoutingMismatchReason = "wrong_version"
	RoutingMismatchReasonWrongRole       RoutingMismatchReason = "wrong_role"
	RoutingMismatchReasonInactiveReplica RoutingMismatchReason = "inactive_replica"
)

type RoutingMismatchError struct {
	Slot                 int
	ExpectedChainVersion uint64
	CurrentChainVersion  uint64
	CurrentRole          ReplicaRole
	CurrentState         ReplicaState
	Reason               RoutingMismatchReason
}

func (e *RoutingMismatchError) Error() string {
	return fmt.Sprintf(
		"%s: slot %d expected version %d, current version %d, role %q, state %q, reason %q",
		ErrRoutingMismatch,
		e.Slot,
		e.ExpectedChainVersion,
		e.CurrentChainVersion,
		e.CurrentRole,
		e.CurrentState,
		e.Reason,
	)
}

func (e *RoutingMismatchError) Unwrap() error {
	return ErrRoutingMismatch
}

type ReplicaState string

const (
	ReplicaStatePending    ReplicaState = "pending"
	ReplicaStateCatchingUp ReplicaState = "catching_up"
	ReplicaStateActive     ReplicaState = "active"
	ReplicaStateLeaving    ReplicaState = "leaving"
	ReplicaStateRecovered  ReplicaState = "recovered"
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

type PersistedReplica struct {
	Assignment               ReplicaAssignment
	LastKnownState           ReplicaState
	HighestCommittedSequence uint64
	HasCommittedData         bool
}

type PersistedNodeState struct {
	NodeID   string
	Replicas []PersistedReplica
}

type RecoveredReplica struct {
	Assignment               ReplicaAssignment
	LastKnownState           ReplicaState
	HighestCommittedSequence uint64
	HasCommittedData         bool
}

type NodeRecoveryReport struct {
	NodeID   string
	Replicas []RecoveredReplica
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

type ResumeRecoveredReplicaCommand struct {
	Assignment ReplicaAssignment
}

type RecoverReplicaCommand struct {
	Assignment   ReplicaAssignment
	SourceNodeID string
}

type DropRecoveredReplicaCommand struct {
	Slot int
}

type replicaRecord struct {
	assignment               ReplicaAssignment
	state                    ReplicaState
	nextSequence             uint64
	highestCommittedSequence uint64
	localDataPresent         bool
	lastKnownState           ReplicaState
	pendingWrites            map[uint64]pendingWrite
	stagedForwards           map[uint64]ForwardWriteRequest
	bufferedForwards         map[uint64]ForwardWriteRequest
	bufferedCommits          map[uint64]CommitWriteRequest
	recentCommittedForwards  map[uint64]ForwardWriteRequest
	recentCommittedCommits   map[uint64]CommitWriteRequest
	recentForwardOrder       []uint64
	recentCommitOrder        []uint64
	inFlightClientWrites     int
}

type pendingWrite struct {
	completed bool
}

type Node struct {
	nodeID                            string
	backend                           Backend
	local                             LocalStateStore
	coord                             CoordinatorClient
	repl                              ReplicationTransport
	replicas                          map[int]replicaRecord
	maxInFlightClientWritesPerNode    int
	maxInFlightClientWritesPerSlot    int
	maxBufferedReplicaMessagesPerNode int
	maxBufferedReplicaMessagesPerSlot int
	maxConcurrentCatchups             int
	writeCommitTimeout                time.Duration
	inFlightClientWrites              int
	inFlightCatchups                  int
	closeErr                          error
	closed                            bool
}

const defaultWriteCommitTimeout = 5 * time.Second

func NewNode(
	ctx context.Context,
	cfg Config,
	backend Backend,
	coord CoordinatorClient,
	repl ReplicationTransport,
) (*Node, error) {
	return OpenNode(ctx, cfg, backend, NewInMemoryLocalStateStore(), coord, repl)
}

func OpenNode(
	ctx context.Context,
	cfg Config,
	backend Backend,
	local LocalStateStore,
	coord CoordinatorClient,
	repl ReplicationTransport,
) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("%w: node ID must not be empty", ErrInvalidConfig)
	}
	if cfg.MaxInFlightClientWritesPerNode < 0 {
		return nil, fmt.Errorf("%w: max in-flight client writes per node must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxInFlightClientWritesPerSlot < 0 {
		return nil, fmt.Errorf("%w: max in-flight client writes per slot must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxBufferedReplicaMessagesPerNode < 0 {
		return nil, fmt.Errorf("%w: max buffered replica messages per node must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxBufferedReplicaMessagesPerSlot < 0 {
		return nil, fmt.Errorf("%w: max buffered replica messages per slot must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxConcurrentCatchups < 0 {
		return nil, fmt.Errorf("%w: max concurrent catchups must be >= 0", ErrInvalidConfig)
	}
	if cfg.WriteCommitTimeout < 0 {
		return nil, fmt.Errorf("%w: write commit timeout must be >= 0", ErrInvalidConfig)
	}
	if backend == nil {
		return nil, fmt.Errorf("%w: backend must not be nil", ErrInvalidConfig)
	}
	if local == nil {
		return nil, fmt.Errorf("%w: local state store must not be nil", ErrInvalidConfig)
	}
	if coord == nil {
		return nil, fmt.Errorf("%w: coordinator client must not be nil", ErrInvalidConfig)
	}
	if repl == nil {
		return nil, fmt.Errorf("%w: replication transport must not be nil", ErrInvalidConfig)
	}

	node := &Node{
		nodeID:                            cfg.NodeID,
		backend:                           backend,
		local:                             local,
		coord:                             coord,
		repl:                              repl,
		replicas:                          make(map[int]replicaRecord),
		maxInFlightClientWritesPerNode:    cfg.MaxInFlightClientWritesPerNode,
		maxInFlightClientWritesPerSlot:    cfg.MaxInFlightClientWritesPerSlot,
		maxBufferedReplicaMessagesPerNode: cfg.MaxBufferedReplicaMessagesPerNode,
		maxBufferedReplicaMessagesPerSlot: cfg.MaxBufferedReplicaMessagesPerSlot,
		maxConcurrentCatchups:             cfg.MaxConcurrentCatchups,
		writeCommitTimeout:                cfg.WriteCommitTimeout,
	}
	if node.maxBufferedReplicaMessagesPerSlot == 0 {
		node.maxBufferedReplicaMessagesPerSlot = 64
	}
	if node.writeCommitTimeout == 0 {
		node.writeCommitTimeout = defaultWriteCommitTimeout
	}

	persisted, err := node.local.LoadNode(ctx, cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("err in node.local.LoadNode: %w", err)
	}
	for _, replica := range persisted.Replicas {
		record := replicaRecord{
			assignment:               cloneAssignment(replica.Assignment),
			state:                    ReplicaStateRecovered,
			nextSequence:             replica.HighestCommittedSequence + 1,
			highestCommittedSequence: replica.HighestCommittedSequence,
			localDataPresent:         false,
			lastKnownState:           replica.LastKnownState,
		}

		if _, err := backend.HighestCommittedSequence(replica.Assignment.Slot); err == nil {
			record.localDataPresent = true
			sequence, err := backend.HighestCommittedSequence(replica.Assignment.Slot)
			if err != nil {
				return nil, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
			}
			record.highestCommittedSequence = sequence
			record.nextSequence = sequence + 1
		} else if !errors.Is(err, ErrUnknownReplica) {
			return nil, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
		}
		record = node.ensureProtocolState(record)

		node.replicas[replica.Assignment.Slot] = record
	}

	return node, nil
}

func (n *Node) AddReplicaAsTail(ctx context.Context, cmd AddReplicaAsTailCommand) error {
	if cmd.Assignment.Slot < 0 {
		return fmt.Errorf("%w: slot must be >= 0", ErrInvalidConfig)
	}
	if _, exists := n.replicas[cmd.Assignment.Slot]; exists {
		return fmt.Errorf("%w: slot %d", ErrReplicaExists, cmd.Assignment.Slot)
	}
	needsCatchup := cmd.Assignment.Peers.PredecessorNodeID != ""
	if needsCatchup {
		if err := n.admitCatchup(); err != nil {
			return err
		}
		defer n.releaseCatchup()
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
		assignment:       cloneAssignment(cmd.Assignment),
		state:            ReplicaStatePending,
		nextSequence:     1,
		localDataPresent: true,
		lastKnownState:   ReplicaStatePending,
	}
	record = n.ensureProtocolState(record)
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
		highestCommittedSequence, err := n.repl.FetchCommittedSequence(ctx, sourceNodeID, cmd.Assignment.Slot)
		if err != nil {
			delete(n.replicas, cmd.Assignment.Slot)
			return fmt.Errorf("err in n.repl.FetchCommittedSequence: %w", err)
		}
		if err := n.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
			delete(n.replicas, cmd.Assignment.Slot)
			return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
		}
		record.highestCommittedSequence = highestCommittedSequence
		record.nextSequence = highestCommittedSequence + 1
	}

	record.state = ReplicaStateCatchingUp
	record.lastKnownState = ReplicaStateCatchingUp
	n.replicas[cmd.Assignment.Slot] = record
	if err := n.persistReplica(ctx, record); err != nil {
		delete(n.replicas, cmd.Assignment.Slot)
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
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

	record = n.replicas[cmd.Slot]
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	n.replicas[cmd.Slot] = record
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (n *Node) MarkReplicaLeaving(ctx context.Context, cmd MarkReplicaLeavingCommand) error {
	record, ok := n.replicas[cmd.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateActive {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}

	record.state = ReplicaStateLeaving
	record.lastKnownState = ReplicaStateLeaving
	n.replicas[cmd.Slot] = record
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
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
		record.localDataPresent = false
		record.lastKnownState = ReplicaStateRemoved
		n.replicas[cmd.Slot] = record
		if err := n.local.DeleteReplica(ctx, n.nodeID, cmd.Slot); err != nil {
			return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
		}
	}
	if err := n.coord.ReportReplicaRemoved(ctx, cmd.Slot); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaRemoved: %w", err)
	}

	delete(n.replicas, cmd.Slot)
	return nil
}

func (n *Node) UpdateChainPeers(ctx context.Context, cmd UpdateChainPeersCommand) error {
	record, ok := n.replicas[cmd.Assignment.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	n.replicas[cmd.Assignment.Slot] = record
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
		n.replicas[cmd.Assignment.Slot] = record
	}
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (n *Node) ReportHeartbeat(ctx context.Context) error {
	status := n.snapshotNodeStatus()
	if err := n.coord.ReportNodeHeartbeat(ctx, status); err != nil {
		return fmt.Errorf("err in n.coord.ReportNodeHeartbeat: %w", err)
	}
	return nil
}

func (n *Node) ReportRecoveredState(ctx context.Context) error {
	report := NodeRecoveryReport{
		NodeID:   n.nodeID,
		Replicas: make([]RecoveredReplica, 0, len(n.replicas)),
	}
	slots := sortedReplicaSlots(n.replicas)
	for _, slot := range slots {
		record := n.replicas[slot]
		if record.state != ReplicaStateRecovered {
			continue
		}
		report.Replicas = append(report.Replicas, RecoveredReplica{
			Assignment:               cloneAssignment(record.assignment),
			LastKnownState:           record.lastKnownState,
			HighestCommittedSequence: record.highestCommittedSequence,
			HasCommittedData:         record.localDataPresent,
		})
	}
	if err := n.coord.ReportNodeRecovered(ctx, report); err != nil {
		return fmt.Errorf("err in n.coord.ReportNodeRecovered: %w", err)
	}
	return nil
}

func (n *Node) ResumeRecoveredReplica(ctx context.Context, cmd ResumeRecoveredReplicaCommand) error {
	record, ok := n.replicas[cmd.Assignment.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, record.state)
	}
	if !record.localDataPresent {
		return fmt.Errorf("%w: slot %d has no committed data to resume", ErrStateMismatch, cmd.Assignment.Slot)
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	record.nextSequence = record.highestCommittedSequence + 1
	n.replicas[cmd.Assignment.Slot] = record
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (n *Node) RecoverReplica(ctx context.Context, cmd RecoverReplicaCommand) error {
	record, exists := n.replicas[cmd.Assignment.Slot]
	if exists && record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, record.state)
	}
	if err := n.admitCatchup(); err != nil {
		return err
	}
	defer n.releaseCatchup()
	if err := n.ensureBackendReplica(cmd.Assignment.Slot); err != nil {
		return fmt.Errorf("err in n.ensureBackendReplica: %w", err)
	}
	snapshot, err := n.repl.FetchSnapshot(ctx, cmd.SourceNodeID, cmd.Assignment.Slot)
	if err != nil {
		return fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
	}
	if err := n.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	highestCommittedSequence, err := n.repl.FetchCommittedSequence(ctx, cmd.SourceNodeID, cmd.Assignment.Slot)
	if err != nil {
		return fmt.Errorf("err in n.repl.FetchCommittedSequence: %w", err)
	}
	if err := n.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}

	record = replicaRecord{
		assignment:               cloneAssignment(cmd.Assignment),
		state:                    ReplicaStateActive,
		nextSequence:             highestCommittedSequence + 1,
		highestCommittedSequence: highestCommittedSequence,
		localDataPresent:         true,
		lastKnownState:           ReplicaStateActive,
	}
	record = n.ensureProtocolState(record)
	n.replicas[cmd.Assignment.Slot] = record
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (n *Node) DropRecoveredReplica(ctx context.Context, cmd DropRecoveredReplicaCommand) error {
	record, ok := n.replicas[cmd.Slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}
	if err := n.backend.DeleteReplica(cmd.Slot); err != nil && !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
	}
	if err := n.local.DeleteReplica(ctx, n.nodeID, cmd.Slot); err != nil {
		return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
	}
	delete(n.replicas, cmd.Slot)
	return nil
}

func (n *Node) Close() error {
	if n.closed {
		return n.closeErr
	}
	n.closed = true

	var errs []error
	if sameOwnedResource(n.backend, n.local) {
		errs = append(errs, n.backend.Close())
	} else {
		errs = append(errs, n.backend.Close(), n.local.Close())
	}
	n.closeErr = errors.Join(errs...)
	return n.closeErr
}

func (n *Node) SubmitPut(ctx context.Context, slot int, key string, value string) (CommitResult, error) {
	return n.submitWrite(ctx, slot, OperationKindPut, key, value)
}

func (n *Node) SubmitDelete(ctx context.Context, slot int, key string) (CommitResult, error) {
	return n.submitWrite(ctx, slot, OperationKindDelete, key, "")
}

func (n *Node) HandleClientGet(_ context.Context, req ClientGetRequest) (ReadResult, error) {
	record, ok := n.replicas[req.Slot]
	if !ok {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, replicaRecord{}, RoutingMismatchReasonUnknownSlot)
	}
	if record.state != ReplicaStateActive {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != req.ExpectedChainVersion {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record, RoutingMismatchReasonWrongVersion)
	}
	if record.assignment.Role != ReplicaRoleTail && record.assignment.Role != ReplicaRoleSingle {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record, RoutingMismatchReasonWrongRole)
	}

	value, found, err := n.backend.GetCommitted(req.Slot, req.Key)
	if err != nil {
		return ReadResult{}, fmt.Errorf("err in n.backend.GetCommitted: %w", err)
	}
	return ReadResult{
		Slot:         req.Slot,
		ChainVersion: record.assignment.ChainVersion,
		Found:        found,
		Value:        value,
	}, nil
}

func (n *Node) HandleClientPut(ctx context.Context, req ClientPutRequest) (CommitResult, error) {
	if err := n.validateClientWrite(req.Slot, req.ExpectedChainVersion); err != nil {
		return CommitResult{}, err
	}
	result, err := n.submitWrite(ctx, req.Slot, OperationKindPut, req.Key, req.Value)
	if err != nil {
		if isAmbiguousWriteCause(err) {
			return CommitResult{}, newAmbiguousWriteError(req.Slot, OperationKindPut, req.ExpectedChainVersion, err)
		}
		return CommitResult{}, err
	}
	return result, nil
}

func (n *Node) HandleClientDelete(ctx context.Context, req ClientDeleteRequest) (CommitResult, error) {
	if err := n.validateClientWrite(req.Slot, req.ExpectedChainVersion); err != nil {
		return CommitResult{}, err
	}
	result, err := n.submitWrite(ctx, req.Slot, OperationKindDelete, req.Key, "")
	if err != nil {
		if isAmbiguousWriteCause(err) {
			return CommitResult{}, newAmbiguousWriteError(req.Slot, OperationKindDelete, req.ExpectedChainVersion, err)
		}
		return CommitResult{}, err
	}
	return result, nil
}

func (n *Node) HandleForwardWrite(ctx context.Context, req ForwardWriteRequest) error {
	record, err := n.activeReplicaRecord(req.Operation.Slot)
	if err != nil {
		return err
	}
	record = n.ensureProtocolState(record)
	if req.Operation.Sequence < record.nextSequence {
		return n.handlePastForward(record, req)
	}
	if record.assignment.Peers.PredecessorNodeID == "" || record.assignment.Peers.PredecessorNodeID != req.FromNodeID {
		return fmt.Errorf(
			"%w: slot %d expected predecessor %q, got %q",
			ErrPeerMismatch,
			req.Operation.Slot,
			record.assignment.Peers.PredecessorNodeID,
			req.FromNodeID,
		)
	}
	switch {
	case req.Operation.Sequence > record.nextSequence:
		record, err = n.bufferFutureForward(record, req)
		if err != nil {
			return err
		}
		n.replicas[req.Operation.Slot] = record
		return nil
	default:
		return n.applyForward(ctx, record, req)
	}
}

func (n *Node) HandleCommitWrite(ctx context.Context, req CommitWriteRequest) error {
	record, err := n.activeReplicaRecord(req.Slot)
	if err != nil {
		return err
	}
	record = n.ensureProtocolState(record)
	if req.Sequence <= record.highestCommittedSequence {
		return n.handlePastCommit(record, req)
	}
	if record.assignment.Peers.SuccessorNodeID == "" || record.assignment.Peers.SuccessorNodeID != req.FromNodeID {
		return fmt.Errorf(
			"%w: slot %d expected successor %q, got %q",
			ErrPeerMismatch,
			req.Slot,
			record.assignment.Peers.SuccessorNodeID,
			req.FromNodeID,
		)
	}
	switch {
	case req.Sequence > record.highestCommittedSequence+1 || !n.hasCommittableSequence(record, req.Sequence):
		record, err = n.bufferFutureCommit(record, req)
		if err != nil {
			return err
		}
		n.replicas[req.Slot] = record
		return nil
	default:
		return n.applyCommit(ctx, record, req)
	}
}

func (n *Node) CommittedSnapshot(slot int) (Snapshot, error) {
	snapshot, err := n.backend.CommittedSnapshot(slot)
	if err != nil {
		return nil, fmt.Errorf("err in n.backend.CommittedSnapshot: %w", err)
	}
	return snapshot, nil
}

func (n *Node) StagedSequences(slot int) ([]uint64, error) {
	sequences, err := n.backend.StagedSequences(slot)
	if err != nil {
		return nil, fmt.Errorf("err in n.backend.StagedSequences: %w", err)
	}
	return sequences, nil
}

func (n *Node) BufferedForwardSequences(slot int) ([]uint64, error) {
	record, ok := n.replicas[slot]
	if !ok {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record = n.ensureProtocolState(record)
	sequences := make([]uint64, 0, len(record.bufferedForwards))
	for sequence := range record.bufferedForwards {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (n *Node) BufferedCommitSequences(slot int) ([]uint64, error) {
	record, ok := n.replicas[slot]
	if !ok {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record = n.ensureProtocolState(record)
	sequences := make([]uint64, 0, len(record.bufferedCommits))
	for sequence := range record.bufferedCommits {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (n *Node) HighestCommittedSequence(slot int) (uint64, error) {
	record, ok := n.replicas[slot]
	if !ok {
		return 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return record.highestCommittedSequence, nil
}

func (n *Node) InFlightClientWrites() int {
	return n.inFlightClientWrites
}

func (n *Node) InFlightClientWritesForSlot(slot int) (int, error) {
	record, ok := n.replicas[slot]
	if !ok {
		return 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return record.inFlightClientWrites, nil
}

func (n *Node) BufferedReplicaMessages() int {
	return n.bufferedReplicaMessagesForNode()
}

func (n *Node) CatchupCount() int {
	return n.inFlightCatchups
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
	slots := sortedReplicaSlots(n.replicas)
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

func cloneWriteOperation(operation WriteOperation) WriteOperation {
	return WriteOperation{
		Slot:     operation.Slot,
		Sequence: operation.Sequence,
		Kind:     operation.Kind,
		Key:      operation.Key,
		Value:    operation.Value,
	}
}

func (n *Node) submitWrite(
	ctx context.Context,
	slot int,
	kind OperationKind,
	key string,
	value string,
) (CommitResult, error) {
	record, err := n.activeReplicaRecord(slot)
	if err != nil {
		return CommitResult{}, err
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		return CommitResult{}, fmt.Errorf(
			"%w: slot %d role %q cannot accept writes",
			ErrWriteRejected,
			slot,
			record.assignment.Role,
		)
	}
	if err := n.admitClientWrite(slot); err != nil {
		return CommitResult{}, err
	}
	record = n.replicas[slot]
	releasedAdmission := false
	defer func() {
		if releasedAdmission {
			return
		}
		n.releaseClientWrite(slot)
		releasedAdmission = true
	}()

	operation := WriteOperation{
		Slot:     slot,
		Sequence: record.nextSequence,
		Kind:     kind,
		Key:      key,
		Value:    value,
	}
	if err := n.stageOperation(operation); err != nil {
		return CommitResult{}, err
	}
	record = n.ensurePendingWrites(record)
	record.pendingWrites[operation.Sequence] = pendingWrite{}

	record.nextSequence++
	n.replicas[slot] = record

	switch record.assignment.Role {
	case ReplicaRoleSingle:
		if err := n.commitLocalSequence(ctx, slot, operation.Sequence); err != nil {
			return CommitResult{}, err
		}
	case ReplicaRoleHead:
		if record.assignment.Peers.SuccessorNodeID == "" {
			return CommitResult{}, fmt.Errorf("%w: slot %d head has no successor", ErrStateMismatch, slot)
		}
		if err := n.repl.ForwardWrite(ctx, record.assignment.Peers.SuccessorNodeID, ForwardWriteRequest{
			Operation:  cloneWriteOperation(operation),
			FromNodeID: n.nodeID,
		}); err != nil {
			return CommitResult{}, fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
		}
		if err := n.awaitWriteCompletion(ctx, slot, operation.Sequence); err != nil {
			return CommitResult{}, err
		}
	}

	n.releaseClientWrite(slot)
	releasedAdmission = true
	return CommitResult{Slot: slot, Sequence: operation.Sequence}, nil
}

func (n *Node) activeReplicaRecord(slot int) (replicaRecord, error) {
	record, ok := n.replicas[slot]
	if !ok {
		return replicaRecord{}, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if record.state != ReplicaStateActive {
		return replicaRecord{}, fmt.Errorf("%w: slot %d is %q", ErrWriteRejected, slot, record.state)
	}
	return record, nil
}

func (n *Node) stageOperation(operation WriteOperation) error {
	switch operation.Kind {
	case OperationKindPut:
		if err := n.backend.StagePut(operation.Slot, operation.Sequence, operation.Key, operation.Value); err != nil {
			return fmt.Errorf("err in n.backend.StagePut: %w", err)
		}
	case OperationKindDelete:
		if err := n.backend.StageDelete(operation.Slot, operation.Sequence, operation.Key); err != nil {
			return fmt.Errorf("err in n.backend.StageDelete: %w", err)
		}
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, operation.Kind)
	}
	return nil
}

func (n *Node) validateClientWrite(slot int, expectedChainVersion uint64) error {
	record, ok := n.replicas[slot]
	if !ok {
		return newRoutingMismatch(slot, expectedChainVersion, replicaRecord{}, RoutingMismatchReasonUnknownSlot)
	}
	if record.state != ReplicaStateActive {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != expectedChainVersion {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonWrongVersion)
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonWrongRole)
	}
	return nil
}

func newRoutingMismatch(
	slot int,
	expectedChainVersion uint64,
	record replicaRecord,
	reason RoutingMismatchReason,
) error {
	return &RoutingMismatchError{
		Slot:                 slot,
		ExpectedChainVersion: expectedChainVersion,
		CurrentChainVersion:  record.assignment.ChainVersion,
		CurrentRole:          record.assignment.Role,
		CurrentState:         record.state,
		Reason:               reason,
	}
}

func newAmbiguousWriteError(
	slot int,
	kind OperationKind,
	expectedChainVersion uint64,
	cause error,
) error {
	return &AmbiguousWriteError{
		Slot:                 slot,
		Kind:                 kind,
		ExpectedChainVersion: expectedChainVersion,
		Cause:                cause,
	}
}

func isAmbiguousWriteCause(err error) bool {
	return errors.Is(err, ErrWriteTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func newWriteBackpressureError(slot int, current int, limit int) error {
	return &BackpressureError{
		Slot:     slot,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceClientWrite,
		Cause:    ErrWriteBackpressure,
	}
}

func newReplicaBackpressureError(slot int, current int, limit int) error {
	return &BackpressureError{
		Slot:     slot,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceReplicaBuffer,
		Cause:    ErrReplicaBackpressure,
	}
}

func newCatchupBackpressureError(current int, limit int) error {
	return &BackpressureError{
		Slot:     -1,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceCatchup,
		Cause:    ErrCatchupBackpressure,
	}
}

func (n *Node) admitClientWrite(slot int) error {
	if n.maxInFlightClientWritesPerNode > 0 && n.inFlightClientWrites >= n.maxInFlightClientWritesPerNode {
		return newWriteBackpressureError(slot, n.inFlightClientWrites, n.maxInFlightClientWritesPerNode)
	}
	record := n.replicas[slot]
	if n.maxInFlightClientWritesPerSlot > 0 && record.inFlightClientWrites >= n.maxInFlightClientWritesPerSlot {
		return newWriteBackpressureError(slot, record.inFlightClientWrites, n.maxInFlightClientWritesPerSlot)
	}
	record.inFlightClientWrites++
	n.replicas[slot] = record
	n.inFlightClientWrites++
	return nil
}

func (n *Node) releaseClientWrite(slot int) {
	record, ok := n.replicas[slot]
	if !ok {
		if n.inFlightClientWrites > 0 {
			n.inFlightClientWrites--
		}
		return
	}
	if record.inFlightClientWrites > 0 {
		record.inFlightClientWrites--
	}
	n.replicas[slot] = record
	if n.inFlightClientWrites > 0 {
		n.inFlightClientWrites--
	}
}

func (n *Node) admitCatchup() error {
	if n.maxConcurrentCatchups > 0 && n.inFlightCatchups >= n.maxConcurrentCatchups {
		return newCatchupBackpressureError(n.inFlightCatchups, n.maxConcurrentCatchups)
	}
	n.inFlightCatchups++
	return nil
}

func (n *Node) releaseCatchup() {
	if n.inFlightCatchups > 0 {
		n.inFlightCatchups--
	}
}

func (n *Node) bufferedReplicaMessagesForNode() int {
	total := 0
	for _, record := range n.replicas {
		total += len(record.bufferedForwards) + len(record.bufferedCommits)
	}
	return total
}

func (n *Node) commitLocalSequence(ctx context.Context, slot int, sequence uint64) error {
	if err := n.backend.CommitSequence(slot, sequence); err != nil {
		return fmt.Errorf("err in n.backend.CommitSequence: %w", err)
	}
	record := n.replicas[slot]
	record = n.ensureProtocolState(record)
	record = n.ensurePendingWrites(record)
	record.highestCommittedSequence = sequence
	record.localDataPresent = true
	if staged, ok := record.stagedForwards[sequence]; ok {
		delete(record.stagedForwards, sequence)
		record = n.recordCommittedForward(record, staged)
	}
	delete(record.pendingWrites, sequence)
	n.replicas[slot] = record
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
		n.replicas[slot] = record
	}
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (n *Node) ensurePendingWrites(record replicaRecord) replicaRecord {
	if record.pendingWrites == nil {
		record.pendingWrites = map[uint64]pendingWrite{}
	}
	return record
}

func (n *Node) ensureProtocolState(record replicaRecord) replicaRecord {
	record = n.ensurePendingWrites(record)
	if record.stagedForwards == nil {
		record.stagedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.bufferedForwards == nil {
		record.bufferedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.bufferedCommits == nil {
		record.bufferedCommits = map[uint64]CommitWriteRequest{}
	}
	if record.recentCommittedForwards == nil {
		record.recentCommittedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.recentCommittedCommits == nil {
		record.recentCommittedCommits = map[uint64]CommitWriteRequest{}
	}
	return record
}

func (n *Node) awaitWriteCompletion(ctx context.Context, slot int, sequence uint64) error {
	if n.writeCommitted(slot, sequence) {
		return nil
	}
	waitCtx, cancel := withDefaultTimeout(ctx, n.writeCommitTimeout)
	defer cancel()
	if waiter, ok := n.repl.(interface {
		AwaitWriteCommit(ctx context.Context, check func() bool) error
	}); ok {
		if err := waiter.AwaitWriteCommit(waitCtx, func() bool {
			return n.writeCommitted(slot, sequence)
		}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return fmt.Errorf("%w: err in repl.AwaitWriteCommit: %w", ErrWriteTimeout, err)
			}
			return fmt.Errorf("err in repl.AwaitWriteCommit: %w", err)
		}
	}
	if !n.writeCommitted(slot, sequence) {
		return fmt.Errorf(
			"%w: slot %d sequence %d was not committed before write completion wait ended",
			ErrStateMismatch,
			slot,
			sequence,
		)
	}
	return nil
}

func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= timeout {
			return ctx, func() {}
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func (n *Node) writeCommitted(slot int, sequence uint64) bool {
	record, ok := n.replicas[slot]
	if !ok {
		return false
	}
	return record.highestCommittedSequence >= sequence
}

func (n *Node) applyForward(ctx context.Context, record replicaRecord, req ForwardWriteRequest) error {
	record = n.ensureProtocolState(record)
	if err := n.stageOperation(req.Operation); err != nil {
		return err
	}
	record.stagedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	record.nextSequence++
	n.replicas[req.Operation.Slot] = record

	if record.assignment.Peers.SuccessorNodeID == "" {
		if err := n.commitLocalSequence(ctx, req.Operation.Slot, req.Operation.Sequence); err != nil {
			return err
		}
		record = n.replicas[req.Operation.Slot]
		if record.assignment.Peers.PredecessorNodeID != "" {
			if err := n.repl.CommitWrite(ctx, record.assignment.Peers.PredecessorNodeID, CommitWriteRequest{
				Slot:       req.Operation.Slot,
				Sequence:   req.Operation.Sequence,
				FromNodeID: n.nodeID,
			}); err != nil {
				return fmt.Errorf("err in n.repl.CommitWrite: %w", err)
			}
		}
	} else {
		if err := n.repl.ForwardWrite(ctx, record.assignment.Peers.SuccessorNodeID, ForwardWriteRequest{
			Operation:  cloneWriteOperation(req.Operation),
			FromNodeID: n.nodeID,
		}); err != nil {
			return fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
		}
	}

	record = n.replicas[req.Operation.Slot]
	return n.drainBufferedReplicaMessages(ctx, record.assignment.Slot)
}

func (n *Node) applyCommit(ctx context.Context, record replicaRecord, req CommitWriteRequest) error {
	record = n.ensureProtocolState(record)
	if err := n.commitLocalSequence(ctx, req.Slot, req.Sequence); err != nil {
		return err
	}

	record = n.replicas[req.Slot]
	record = n.ensureProtocolState(record)
	record = n.recordCommittedCommit(record, req)
	if pending, ok := record.pendingWrites[req.Sequence]; ok {
		pending.completed = true
		record.pendingWrites[req.Sequence] = pending
	}
	n.replicas[req.Slot] = record
	if record.assignment.Peers.PredecessorNodeID != "" {
		if err := n.repl.CommitWrite(ctx, record.assignment.Peers.PredecessorNodeID, CommitWriteRequest{
			Slot:       req.Slot,
			Sequence:   req.Sequence,
			FromNodeID: n.nodeID,
		}); err != nil {
			return fmt.Errorf("err in n.repl.CommitWrite: %w", err)
		}
	}
	return n.drainBufferedReplicaMessages(ctx, req.Slot)
}

func (n *Node) drainBufferedReplicaMessages(ctx context.Context, slot int) error {
	for {
		record, ok := n.replicas[slot]
		if !ok {
			return nil
		}
		record = n.ensureProtocolState(record)
		if req, ok := record.bufferedForwards[record.nextSequence]; ok {
			delete(record.bufferedForwards, record.nextSequence)
			n.replicas[slot] = record
			if err := n.applyForward(ctx, record, req); err != nil {
				return err
			}
			continue
		}
		nextCommit := record.highestCommittedSequence + 1
		if req, ok := record.bufferedCommits[nextCommit]; ok && n.hasCommittableSequence(record, nextCommit) {
			delete(record.bufferedCommits, nextCommit)
			n.replicas[slot] = record
			if err := n.applyCommit(ctx, record, req); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (n *Node) handlePastForward(record replicaRecord, req ForwardWriteRequest) error {
	record = n.ensureProtocolState(record)
	if staged, ok := record.stagedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(staged, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d forward payload conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	if committed, ok := record.recentCommittedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(committed, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d committed forward conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	return fmt.Errorf("%w: slot %d sequence %d is outside retained forward history", ErrSequenceMismatch, req.Operation.Slot, req.Operation.Sequence)
}

func (n *Node) handlePastCommit(record replicaRecord, req CommitWriteRequest) error {
	record = n.ensureProtocolState(record)
	if committed, ok := record.recentCommittedCommits[req.Sequence]; ok {
		if sameCommitRequest(committed, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d committed ack conflict", ErrProtocolConflict, req.Slot, req.Sequence)
	}
	return fmt.Errorf("%w: slot %d sequence %d is outside retained commit history", ErrSequenceMismatch, req.Slot, req.Sequence)
}

func (n *Node) bufferFutureForward(record replicaRecord, req ForwardWriteRequest) (replicaRecord, error) {
	record = n.ensureProtocolState(record)
	if existing, ok := record.bufferedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(existing, req) {
			return record, nil
		}
		return record, fmt.Errorf("%w: slot %d sequence %d buffered forward conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	if n.totalBufferedMessages(record) >= n.maxBufferedReplicaMessagesPerSlot {
		return record, newReplicaBackpressureError(req.Operation.Slot, n.totalBufferedMessages(record), n.maxBufferedReplicaMessagesPerSlot)
	}
	if n.maxBufferedReplicaMessagesPerNode > 0 && n.bufferedReplicaMessagesForNode() >= n.maxBufferedReplicaMessagesPerNode {
		return record, newReplicaBackpressureError(req.Operation.Slot, n.bufferedReplicaMessagesForNode(), n.maxBufferedReplicaMessagesPerNode)
	}
	record.bufferedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	return record, nil
}

func (n *Node) bufferFutureCommit(record replicaRecord, req CommitWriteRequest) (replicaRecord, error) {
	record = n.ensureProtocolState(record)
	if existing, ok := record.bufferedCommits[req.Sequence]; ok {
		if sameCommitRequest(existing, req) {
			return record, nil
		}
		return record, fmt.Errorf("%w: slot %d sequence %d buffered commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
	}
	if n.totalBufferedMessages(record) >= n.maxBufferedReplicaMessagesPerSlot {
		return record, newReplicaBackpressureError(req.Slot, n.totalBufferedMessages(record), n.maxBufferedReplicaMessagesPerSlot)
	}
	if n.maxBufferedReplicaMessagesPerNode > 0 && n.bufferedReplicaMessagesForNode() >= n.maxBufferedReplicaMessagesPerNode {
		return record, newReplicaBackpressureError(req.Slot, n.bufferedReplicaMessagesForNode(), n.maxBufferedReplicaMessagesPerNode)
	}
	record.bufferedCommits[req.Sequence] = cloneCommitRequest(req)
	return record, nil
}

func (n *Node) totalBufferedMessages(record replicaRecord) int {
	return len(record.bufferedForwards) + len(record.bufferedCommits)
}

func (n *Node) hasStagedForward(record replicaRecord, sequence uint64) bool {
	record = n.ensureProtocolState(record)
	_, ok := record.stagedForwards[sequence]
	return ok
}

func (n *Node) hasCommittableSequence(record replicaRecord, sequence uint64) bool {
	if n.hasStagedForward(record, sequence) {
		return true
	}
	record = n.ensurePendingWrites(record)
	_, ok := record.pendingWrites[sequence]
	return ok
}

func (n *Node) recordCommittedForward(record replicaRecord, req ForwardWriteRequest) replicaRecord {
	record = n.ensureProtocolState(record)
	record.recentCommittedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	record.recentForwardOrder = append(record.recentForwardOrder, req.Operation.Sequence)
	for len(record.recentForwardOrder) > n.maxBufferedReplicaMessagesPerSlot {
		evicted := record.recentForwardOrder[0]
		record.recentForwardOrder = record.recentForwardOrder[1:]
		delete(record.recentCommittedForwards, evicted)
	}
	return record
}

func (n *Node) recordCommittedCommit(record replicaRecord, req CommitWriteRequest) replicaRecord {
	record = n.ensureProtocolState(record)
	record.recentCommittedCommits[req.Sequence] = cloneCommitRequest(req)
	record.recentCommitOrder = append(record.recentCommitOrder, req.Sequence)
	for len(record.recentCommitOrder) > n.maxBufferedReplicaMessagesPerSlot {
		evicted := record.recentCommitOrder[0]
		record.recentCommitOrder = record.recentCommitOrder[1:]
		delete(record.recentCommittedCommits, evicted)
	}
	return record
}

func sameForwardRequest(left ForwardWriteRequest, right ForwardWriteRequest) bool {
	return left.FromNodeID == right.FromNodeID && left.Operation == right.Operation
}

func sameCommitRequest(left CommitWriteRequest, right CommitWriteRequest) bool {
	return left == right
}

func cloneForwardRequest(req ForwardWriteRequest) ForwardWriteRequest {
	return ForwardWriteRequest{
		Operation:  cloneWriteOperation(req.Operation),
		FromNodeID: req.FromNodeID,
	}
}

func cloneCommitRequest(req CommitWriteRequest) CommitWriteRequest {
	return CommitWriteRequest{
		Slot:       req.Slot,
		Sequence:   req.Sequence,
		FromNodeID: req.FromNodeID,
	}
}

func (n *Node) persistReplica(ctx context.Context, record replicaRecord) error {
	persisted := PersistedReplica{
		Assignment:               cloneAssignment(record.assignment),
		LastKnownState:           record.lastKnownState,
		HighestCommittedSequence: record.highestCommittedSequence,
		HasCommittedData:         record.localDataPresent,
	}
	if err := n.local.UpsertReplica(ctx, n.nodeID, persisted); err != nil {
		return fmt.Errorf("err in n.local.UpsertReplica: %w", err)
	}
	return nil
}

func (n *Node) ensureBackendReplica(slot int) error {
	if _, err := n.backend.HighestCommittedSequence(slot); err == nil {
		return nil
	} else if !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.HighestCommittedSequence: %w", err)
	}
	if err := n.backend.CreateReplica(slot); err != nil && !errors.Is(err, ErrReplicaExists) {
		return fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}
	return nil
}

func sortedReplicaSlots(replicas map[int]replicaRecord) []int {
	slots := make([]int, 0, len(replicas))
	for slot := range replicas {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	return slots
}

func sameOwnedResource(left any, right any) bool {
	if left == nil || right == nil {
		return false
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if !leftValue.IsValid() || !rightValue.IsValid() || leftValue.Type() != rightValue.Type() {
		return false
	}
	if leftValue.Type().Comparable() {
		return leftValue.Interface() == rightValue.Interface()
	}
	switch leftValue.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return leftValue.Pointer() == rightValue.Pointer()
	default:
		return false
	}
}
