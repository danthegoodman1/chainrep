package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/danthegoodman1/chainrep/coordinator"
	"github.com/danthegoodman1/chainrep/storage"
)

var (
	ErrInvalidCommand     = errors.New("invalid coordinator runtime command")
	ErrVersionMismatch    = errors.New("coordinator runtime version mismatch")
	ErrCommandConflict    = errors.New("coordinator runtime command conflict")
	ErrAlreadyInitialized = errors.New("coordinator runtime already initialized")
	ErrNotInitialized     = errors.New("coordinator runtime not initialized")
	ErrRecovery           = errors.New("coordinator runtime recovery failed")
)

type State struct {
	Version          uint64
	LastLogIndex     uint64
	Cluster          coordinator.ClusterState
	SlotVersions     map[int]uint64
	CompletedProgressBySlot map[int][]CompletedProgressRecord
	NodeLivenessByID map[string]NodeLivenessRecord
	AppliedCommands  map[string]AppliedCommand
}

type CompletedProgressKind string

const (
	CompletedProgressKindReady   CompletedProgressKind = "ready"
	CompletedProgressKindRemoved CompletedProgressKind = "removed"
)

type CompletedProgressRecord struct {
	NodeID      string
	Kind        CompletedProgressKind
	SlotVersion uint64
}

type NodeLivenessState string

const (
	NodeLivenessStateHealthy NodeLivenessState = "healthy"
	NodeLivenessStateSuspect NodeLivenessState = "suspect"
	NodeLivenessStateDead    NodeLivenessState = "dead"
)

type NodeLivenessRecord struct {
	LastHeartbeatUnixNano int64
	State                 NodeLivenessState
	LastStatus            storage.NodeStatus
	DeadActionFired       bool
}

type AppliedCommand struct {
	Command                Command
	Version                uint64
	LastLogIndex           uint64
	Cluster                coordinator.ClusterState
	SlotVersions           map[int]uint64
	CompletedProgressBySlot map[int][]CompletedProgressRecord
	NodeLivenessByID       map[string]NodeLivenessRecord
	Plan                   *coordinator.ReconfigurationPlan
}

type CommandKind string

const (
	CommandKindBootstrap   CommandKind = "bootstrap"
	CommandKindReconfigure CommandKind = "reconfigure"
	CommandKindProgress    CommandKind = "progress"
	CommandKindHeartbeat   CommandKind = "heartbeat"
	CommandKindLiveness    CommandKind = "liveness"
)

type Command struct {
	ID              string
	ExpectedVersion uint64
	Kind            CommandKind
	Bootstrap       *BootstrapCommand
	Reconfigure     *ReconfigureCommand
	Progress        *ProgressCommand
	Heartbeat       *HeartbeatCommand
	Liveness        *LivenessCommand
}

type BootstrapCommand struct {
	Config coordinator.Config
	Nodes  []coordinator.Node
}

type ReconfigureCommand struct {
	Events []coordinator.Event
	Policy coordinator.ReconfigurationPolicy
}

type ProgressCommand struct {
	Event coordinator.Event
}

type HeartbeatCommand struct {
	Status              storage.NodeStatus
	ObservedAtUnixNano  int64
}

type LivenessCommand struct {
	NodeID              string
	State               NodeLivenessState
	EvaluatedAtUnixNano int64
	DeadActionFired     bool
}

type LogRecord struct {
	Index   uint64
	Command Command
}

type Checkpoint struct {
	State State
}

type Store interface {
	LoadLatestCheckpoint(ctx context.Context) (Checkpoint, bool, error)
	LoadWAL(ctx context.Context, afterIndex uint64) ([]LogRecord, error)
	AppendWAL(ctx context.Context, record LogRecord) error
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
	TruncateWAL(ctx context.Context, throughIndex uint64) error
}

type Runtime struct {
	store Store
	state State
}

func Open(ctx context.Context, store Store) (*Runtime, error) {
	checkpoint, ok, err := store.LoadLatestCheckpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("err in store.LoadLatestCheckpoint: %w", err)
	}

	state := zeroState()
	if ok {
		state = cloneState(checkpoint.State)
	}

	records, err := store.LoadWAL(ctx, state.LastLogIndex)
	if err != nil {
		return nil, fmt.Errorf("err in store.LoadWAL: %w", err)
	}

	r := &Runtime{
		store: store,
		state: state,
	}
	expectedIndex := r.state.LastLogIndex
	for _, record := range records {
		if record.Index != expectedIndex+1 {
			return nil, fmt.Errorf(
				"%w: unexpected WAL index %d after %d",
				ErrRecovery,
				record.Index,
				expectedIndex,
			)
		}
		if err := r.replayRecord(record); err != nil {
			return nil, fmt.Errorf("err in r.replayRecord: %w", err)
		}
		expectedIndex = record.Index
	}

	return r, nil
}

func (r *Runtime) Current() State {
	return cloneState(r.state)
}

func (r *Runtime) Bootstrap(ctx context.Context, cmd Command) (State, error) {
	cluster, _, duplicate, err := r.executeBootstrap(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeBootstrap: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return cloneState(r.state), nil
}

func (r *Runtime) Reconfigure(
	ctx context.Context,
	cmd Command,
) (coordinator.ReconfigurationPlan, State, error) {
	cluster, plan, duplicate, err := r.executeReconfigure(cmd)
	if err != nil {
		return coordinator.ReconfigurationPlan{}, State{}, fmt.Errorf("err in r.executeReconfigure: %w", err)
	}
	if duplicate != nil {
		snapshot := r.snapshotForApplied(*duplicate)
		if duplicate.Plan == nil {
			return coordinator.ReconfigurationPlan{}, snapshot, nil
		}
		return *clonePlan(duplicate.Plan), snapshot, nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, plan)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return coordinator.ReconfigurationPlan{}, State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return *clonePlan(plan), cloneState(r.state), nil
}

func (r *Runtime) ApplyProgress(ctx context.Context, cmd Command) (State, error) {
	cluster, _, duplicate, err := r.executeProgress(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeProgress: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return cloneState(r.state), nil
}

func (r *Runtime) Heartbeat(ctx context.Context, cmd Command) (State, error) {
	_, _, duplicate, err := r.executeHeartbeat(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeHeartbeat: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, r.state.Cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState
	return cloneState(r.state), nil
}

func (r *Runtime) ApplyLiveness(ctx context.Context, cmd Command) (State, error) {
	_, _, duplicate, err := r.executeLiveness(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeLiveness: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, r.state.Cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState
	return cloneState(r.state), nil
}

func (r *Runtime) Checkpoint(ctx context.Context) error {
	checkpointState := cloneState(r.state)
	checkpointState.AppliedCommands = map[string]AppliedCommand{}

	if err := r.store.SaveCheckpoint(ctx, Checkpoint{State: checkpointState}); err != nil {
		return fmt.Errorf("err in r.store.SaveCheckpoint: %w", err)
	}
	if err := r.store.TruncateWAL(ctx, checkpointState.LastLogIndex); err != nil {
		return fmt.Errorf("err in r.store.TruncateWAL: %w", err)
	}

	r.state.AppliedCommands = map[string]AppliedCommand{}
	return nil
}

func (r *Runtime) replayRecord(record LogRecord) error {
	cluster, plan, duplicate, err := r.executeCommand(record.Command)
	if err != nil {
		return fmt.Errorf("%w: replay command %q: %w", ErrRecovery, record.Command.ID, err)
	}
	if duplicate != nil {
		return fmt.Errorf("%w: duplicate command %q present in WAL replay", ErrRecovery, record.Command.ID)
	}

	nextState := r.nextStateForApplied(record.Index, record.Command, cluster, plan)
	r.state = nextState
	return nil
}

func (r *Runtime) executeBootstrap(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeReconfigure(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeProgress(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeHeartbeat(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeLiveness(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeCommand(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	duplicate, err := r.validateCommand(cmd)
	if err != nil || duplicate != nil {
		if err != nil {
			return coordinator.ClusterState{}, nil, duplicate, fmt.Errorf("err in r.validateCommand: %w", err)
		}
		return coordinator.ClusterState{}, nil, duplicate, nil
	}

	switch cmd.Kind {
	case CommandKindBootstrap:
		if isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrAlreadyInitialized
		}
		cluster, err := coordinator.BuildInitialPlacement(
			cmd.Bootstrap.Config,
			cloneNodes(cmd.Bootstrap.Nodes),
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.BuildInitialPlacement: %w", err)
		}
		return cloneClusterState(*cluster), nil, nil, nil
	case CommandKindReconfigure:
		if !isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrNotInitialized
		}
		plan, err := coordinator.PlanReconfiguration(
			cloneClusterState(r.state.Cluster),
			cloneEvents(cmd.Reconfigure.Events),
			cmd.Reconfigure.Policy,
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.PlanReconfiguration: %w", err)
		}
		return cloneClusterState(plan.UpdatedState), clonePlan(plan), nil, nil
	case CommandKindProgress:
		if !isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrNotInitialized
		}
		cluster, err := coordinator.ApplyProgress(
			cloneClusterState(r.state.Cluster),
			cloneEvent(cmd.Progress.Event),
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.ApplyProgress: %w", err)
		}
		return cloneClusterState(*cluster), nil, nil, nil
	case CommandKindHeartbeat:
		return cloneClusterState(r.state.Cluster), nil, nil, nil
	case CommandKindLiveness:
		if cmd.Liveness.NodeID == "" {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("%w: liveness node ID must not be empty", ErrInvalidCommand)
		}
		return cloneClusterState(r.state.Cluster), nil, nil, nil
	default:
		return coordinator.ClusterState{}, nil, nil, fmt.Errorf(
			"%w: unsupported command kind %q",
			ErrInvalidCommand,
			cmd.Kind,
		)
	}
}

func (r *Runtime) validateCommand(cmd Command) (*AppliedCommand, error) {
	if cmd.ID == "" {
		return nil, fmt.Errorf("%w: command ID must not be empty", ErrInvalidCommand)
	}
	if err := validateCommandPayload(cmd); err != nil {
		return nil, fmt.Errorf("err in validateCommandPayload: %w", err)
	}

	if applied, ok := r.state.AppliedCommands[cmd.ID]; ok {
		if !reflect.DeepEqual(applied.Command, cloneCommand(cmd)) {
			return nil, fmt.Errorf("%w: command %q was already applied with different payload", ErrCommandConflict, cmd.ID)
		}
		cloned := cloneAppliedCommand(applied)
		return &cloned, nil
	}

	if cmd.ExpectedVersion != r.state.Version {
		return nil, fmt.Errorf(
			"%w: expected version %d does not match current version %d",
			ErrVersionMismatch,
			cmd.ExpectedVersion,
			r.state.Version,
		)
	}

	return nil, nil
}

func validateCommandPayload(cmd Command) error {
	switch cmd.Kind {
	case CommandKindBootstrap:
		if cmd.Bootstrap == nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.Liveness != nil {
			return fmt.Errorf("%w: bootstrap command must set only bootstrap payload", ErrInvalidCommand)
		}
	case CommandKindReconfigure:
		if cmd.Reconfigure == nil || cmd.Bootstrap != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.Liveness != nil {
			return fmt.Errorf("%w: reconfigure command must set only reconfigure payload", ErrInvalidCommand)
		}
	case CommandKindProgress:
		if cmd.Progress == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Heartbeat != nil || cmd.Liveness != nil {
			return fmt.Errorf("%w: progress command must set only progress payload", ErrInvalidCommand)
		}
	case CommandKindHeartbeat:
		if cmd.Heartbeat == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Liveness != nil {
			return fmt.Errorf("%w: heartbeat command must set only heartbeat payload", ErrInvalidCommand)
		}
		if cmd.Heartbeat.Status.NodeID == "" {
			return fmt.Errorf("%w: heartbeat node ID must not be empty", ErrInvalidCommand)
		}
	case CommandKindLiveness:
		if cmd.Liveness == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Heartbeat != nil {
			return fmt.Errorf("%w: liveness command must set only liveness payload", ErrInvalidCommand)
		}
		if cmd.Liveness.NodeID == "" {
			return fmt.Errorf("%w: liveness node ID must not be empty", ErrInvalidCommand)
		}
	default:
		return fmt.Errorf("%w: unsupported command kind %q", ErrInvalidCommand, cmd.Kind)
	}
	return nil
}

func (r *Runtime) commitCandidate(
	cmd Command,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) (LogRecord, State) {
	index := r.state.LastLogIndex + 1
	record := LogRecord{
		Index:   index,
		Command: cloneCommand(cmd),
	}
	return record, r.nextStateForApplied(index, cmd, cluster, plan)
}

func (r *Runtime) nextStateForApplied(
	logIndex uint64,
	cmd Command,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) State {
	next := cloneState(r.state)
	next.Version++
	next.LastLogIndex = logIndex
	next.Cluster = cloneClusterState(cluster)
	next.SlotVersions = nextSlotVersions(next.SlotVersions, next.Version, cmd.Kind, cluster, plan)
	next.CompletedProgressBySlot = nextCompletedProgress(next.CompletedProgressBySlot, next.SlotVersions, cmd)
	next.NodeLivenessByID = nextNodeLiveness(next.NodeLivenessByID, cmd)
	if next.AppliedCommands == nil {
		next.AppliedCommands = make(map[string]AppliedCommand)
	}
	next.AppliedCommands[cmd.ID] = AppliedCommand{
		Command:                 cloneCommand(cmd),
		Version:                 next.Version,
		LastLogIndex:            logIndex,
		Cluster:                 cloneClusterState(cluster),
		SlotVersions:            cloneSlotVersions(next.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(next.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(next.NodeLivenessByID),
		Plan:                    clonePlan(plan),
	}
	return next
}

func (r *Runtime) snapshotForApplied(applied AppliedCommand) State {
	snapshot := State{
		Version:                applied.Version,
		LastLogIndex:           applied.LastLogIndex,
		Cluster:                cloneClusterState(applied.Cluster),
		SlotVersions:           cloneSlotVersions(applied.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(applied.CompletedProgressBySlot),
		NodeLivenessByID: cloneNodeLivenessMap(applied.NodeLivenessByID),
		AppliedCommands:        make(map[string]AppliedCommand),
	}
	for id, existing := range r.state.AppliedCommands {
		if existing.Version <= applied.Version {
			snapshot.AppliedCommands[id] = cloneAppliedCommand(existing)
		}
	}
	return snapshot
}

func zeroState() State {
	return State{
		SlotVersions:            map[int]uint64{},
		CompletedProgressBySlot: map[int][]CompletedProgressRecord{},
		NodeLivenessByID:        map[string]NodeLivenessRecord{},
		AppliedCommands:         map[string]AppliedCommand{},
	}
}

func isInitialized(state State) bool {
	return state.Cluster.SlotCount > 0
}

func cloneState(state State) State {
	cloned := State{
		Version:                state.Version,
		LastLogIndex:           state.LastLogIndex,
		Cluster:                cloneClusterState(state.Cluster),
		SlotVersions:           cloneSlotVersions(state.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(state.CompletedProgressBySlot),
		NodeLivenessByID: cloneNodeLivenessMap(state.NodeLivenessByID),
		AppliedCommands:        make(map[string]AppliedCommand, len(state.AppliedCommands)),
	}
	for id, applied := range state.AppliedCommands {
		cloned.AppliedCommands[id] = cloneAppliedCommand(applied)
	}
	return cloned
}

func cloneAppliedCommand(applied AppliedCommand) AppliedCommand {
	return AppliedCommand{
		Command:                 cloneCommand(applied.Command),
		Version:                 applied.Version,
		LastLogIndex:            applied.LastLogIndex,
		Cluster:                 cloneClusterState(applied.Cluster),
		SlotVersions:            cloneSlotVersions(applied.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(applied.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(applied.NodeLivenessByID),
		Plan:                    clonePlan(applied.Plan),
	}
}

func nextSlotVersions(
	current map[int]uint64,
	version uint64,
	kind CommandKind,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) map[int]uint64 {
	next := cloneSlotVersions(current)
	switch kind {
	case CommandKindBootstrap:
		next = make(map[int]uint64, len(cluster.Chains))
		for _, chain := range cluster.Chains {
			next[chain.Slot] = version
		}
	case CommandKindReconfigure:
		if plan == nil {
			return next
		}
		for _, slotPlan := range plan.ChangedSlots {
			next[slotPlan.Slot] = version
		}
	}
	return next
}

func cloneSlotVersions(slotVersions map[int]uint64) map[int]uint64 {
	cloned := make(map[int]uint64, len(slotVersions))
	for slot, version := range slotVersions {
		cloned[slot] = version
	}
	return cloned
}

const completedProgressHistoryLimit = 8

func nextCompletedProgress(
	current map[int][]CompletedProgressRecord,
	slotVersions map[int]uint64,
	cmd Command,
) map[int][]CompletedProgressRecord {
	next := cloneCompletedProgressMap(current)
	if cmd.Kind != CommandKindProgress || cmd.Progress == nil {
		return next
	}

	var kind CompletedProgressKind
	switch cmd.Progress.Event.Kind {
	case coordinator.EventKindReplicaBecameActive:
		kind = CompletedProgressKindReady
	case coordinator.EventKindReplicaRemoved:
		kind = CompletedProgressKindRemoved
	default:
		return next
	}

	slot := cmd.Progress.Event.Slot
	record := CompletedProgressRecord{
		NodeID:      cmd.Progress.Event.NodeID,
		Kind:        kind,
		SlotVersion: slotVersions[slot],
	}
	next[slot] = append(next[slot], record)
	if len(next[slot]) > completedProgressHistoryLimit {
		next[slot] = append([]CompletedProgressRecord(nil), next[slot][len(next[slot])-completedProgressHistoryLimit:]...)
	}
	return next
}

func cloneCompletedProgressMap(current map[int][]CompletedProgressRecord) map[int][]CompletedProgressRecord {
	cloned := make(map[int][]CompletedProgressRecord, len(current))
	for slot, records := range current {
		cloned[slot] = append([]CompletedProgressRecord(nil), records...)
	}
	return cloned
}

func cloneCommand(cmd Command) Command {
	cloned := Command{
		ID:              cmd.ID,
		ExpectedVersion: cmd.ExpectedVersion,
		Kind:            cmd.Kind,
	}
	if cmd.Bootstrap != nil {
		cloned.Bootstrap = &BootstrapCommand{
			Config: cmd.Bootstrap.Config,
			Nodes:  cloneNodes(cmd.Bootstrap.Nodes),
		}
	}
	if cmd.Reconfigure != nil {
		cloned.Reconfigure = &ReconfigureCommand{
			Events: cloneEvents(cmd.Reconfigure.Events),
			Policy: cmd.Reconfigure.Policy,
		}
	}
	if cmd.Progress != nil {
		cloned.Progress = &ProgressCommand{
			Event: cloneEvent(cmd.Progress.Event),
		}
	}
	if cmd.Heartbeat != nil {
		cloned.Heartbeat = &HeartbeatCommand{
			Status:             cloneNodeStatus(cmd.Heartbeat.Status),
			ObservedAtUnixNano: cmd.Heartbeat.ObservedAtUnixNano,
		}
	}
	if cmd.Liveness != nil {
		cloned.Liveness = &LivenessCommand{
			NodeID:              cmd.Liveness.NodeID,
			State:               cmd.Liveness.State,
			EvaluatedAtUnixNano: cmd.Liveness.EvaluatedAtUnixNano,
			DeadActionFired:     cmd.Liveness.DeadActionFired,
		}
	}
	return cloned
}

func nextNodeLiveness(
	current map[string]NodeLivenessRecord,
	cmd Command,
) map[string]NodeLivenessRecord {
	next := cloneNodeLivenessMap(current)
	switch cmd.Kind {
	case CommandKindHeartbeat:
		record := next[cmd.Heartbeat.Status.NodeID]
		record.LastHeartbeatUnixNano = cmd.Heartbeat.ObservedAtUnixNano
		record.LastStatus = cloneNodeStatus(cmd.Heartbeat.Status)
		if record.State != NodeLivenessStateDead {
			record.State = NodeLivenessStateHealthy
			record.DeadActionFired = false
		}
		next[cmd.Heartbeat.Status.NodeID] = record
	case CommandKindLiveness:
		record := next[cmd.Liveness.NodeID]
		record.State = cmd.Liveness.State
		record.DeadActionFired = cmd.Liveness.DeadActionFired
		if cmd.Liveness.EvaluatedAtUnixNano != 0 && record.LastHeartbeatUnixNano == 0 {
			record.LastHeartbeatUnixNano = cmd.Liveness.EvaluatedAtUnixNano
		}
		next[cmd.Liveness.NodeID] = record
	}
	return next
}

func cloneNodeLivenessMap(current map[string]NodeLivenessRecord) map[string]NodeLivenessRecord {
	cloned := make(map[string]NodeLivenessRecord, len(current))
	for nodeID, record := range current {
		cloned[nodeID] = NodeLivenessRecord{
			LastHeartbeatUnixNano: record.LastHeartbeatUnixNano,
			State:                 record.State,
			LastStatus:            cloneNodeStatus(record.LastStatus),
			DeadActionFired:       record.DeadActionFired,
		}
	}
	return cloned
}

func cloneNodeStatus(status storage.NodeStatus) storage.NodeStatus {
	return storage.NodeStatus{
		NodeID:          status.NodeID,
		ReplicaCount:    status.ReplicaCount,
		ActiveCount:     status.ActiveCount,
		CatchingUpCount: status.CatchingUpCount,
		LeavingCount:    status.LeavingCount,
	}
}

func clonePlan(plan *coordinator.ReconfigurationPlan) *coordinator.ReconfigurationPlan {
	if plan == nil {
		return nil
	}
	cloned := &coordinator.ReconfigurationPlan{
		UpdatedState:      cloneClusterState(plan.UpdatedState),
		UnassignedNodeIDs: append([]string(nil), plan.UnassignedNodeIDs...),
		ChangedSlots:      make([]coordinator.SlotPlan, len(plan.ChangedSlots)),
	}
	for i, slotPlan := range plan.ChangedSlots {
		cloned.ChangedSlots[i] = coordinator.SlotPlan{
			Slot:   slotPlan.Slot,
			Before: cloneChain(slotPlan.Before),
			After:  cloneChain(slotPlan.After),
			Steps:  append([]coordinator.ReconfigurationStep(nil), slotPlan.Steps...),
		}
	}
	return cloned
}

func cloneClusterState(state coordinator.ClusterState) coordinator.ClusterState {
	cloned := coordinator.ClusterState{
		Chains:            make([]coordinator.Chain, len(state.Chains)),
		NodesByID:         make(map[string]coordinator.Node, len(state.NodesByID)),
		NodeHealthByID:    make(map[string]coordinator.NodeHealth, len(state.NodeHealthByID)),
		DrainingNodeIDs:   make(map[string]bool, len(state.DrainingNodeIDs)),
		NodeOrder:         append([]string(nil), state.NodeOrder...),
		SlotCount:         state.SlotCount,
		ReplicationFactor: state.ReplicationFactor,
	}
	for i, chain := range state.Chains {
		cloned.Chains[i] = cloneChain(chain)
	}
	for id, node := range state.NodesByID {
		cloned.NodesByID[id] = coordinator.Node{
			ID:             node.ID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: cloneFailureDomains(node.FailureDomains),
		}
	}
	for id, health := range state.NodeHealthByID {
		cloned.NodeHealthByID[id] = health
	}
	for id, draining := range state.DrainingNodeIDs {
		cloned.DrainingNodeIDs[id] = draining
	}
	return cloned
}

func cloneChain(chain coordinator.Chain) coordinator.Chain {
	cloned := coordinator.Chain{
		Slot:     chain.Slot,
		Replicas: make([]coordinator.Replica, len(chain.Replicas)),
	}
	copy(cloned.Replicas, chain.Replicas)
	return cloned
}

func cloneNodes(nodes []coordinator.Node) []coordinator.Node {
	cloned := make([]coordinator.Node, len(nodes))
	for i, node := range nodes {
		cloned[i] = coordinator.Node{
			ID:             node.ID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: cloneFailureDomains(node.FailureDomains),
		}
	}
	return cloned
}

func cloneEvents(events []coordinator.Event) []coordinator.Event {
	cloned := make([]coordinator.Event, len(events))
	for i, event := range events {
		cloned[i] = cloneEvent(event)
	}
	return cloned
}

func cloneEvent(event coordinator.Event) coordinator.Event {
	return coordinator.Event{
		Kind:   event.Kind,
		Node: coordinator.Node{
			ID:             event.Node.ID,
			RPCAddress:     event.Node.RPCAddress,
			FailureDomains: cloneFailureDomains(event.Node.FailureDomains),
		},
		NodeID: event.NodeID,
		Slot:   event.Slot,
	}
}

func cloneFailureDomains(domains map[string]string) map[string]string {
	if len(domains) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(domains))
	for key, value := range domains {
		cloned[key] = value
	}
	return cloned
}
