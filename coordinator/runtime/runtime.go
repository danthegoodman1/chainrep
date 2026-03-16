package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/danthegoodman1/chainrep/coordinator"
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
	Version         uint64
	LastLogIndex    uint64
	Cluster         coordinator.ClusterState
	AppliedCommands map[string]AppliedCommand
}

type AppliedCommand struct {
	Command      Command
	Version      uint64
	LastLogIndex uint64
	Cluster      coordinator.ClusterState
	Plan         *coordinator.ReconfigurationPlan
}

type CommandKind string

const (
	CommandKindBootstrap   CommandKind = "bootstrap"
	CommandKindReconfigure CommandKind = "reconfigure"
	CommandKindProgress    CommandKind = "progress"
)

type Command struct {
	ID              string
	ExpectedVersion uint64
	Kind            CommandKind
	Bootstrap       *BootstrapCommand
	Reconfigure     *ReconfigureCommand
	Progress        *ProgressCommand
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
		if cmd.Bootstrap == nil || cmd.Reconfigure != nil || cmd.Progress != nil {
			return fmt.Errorf("%w: bootstrap command must set only bootstrap payload", ErrInvalidCommand)
		}
	case CommandKindReconfigure:
		if cmd.Reconfigure == nil || cmd.Bootstrap != nil || cmd.Progress != nil {
			return fmt.Errorf("%w: reconfigure command must set only reconfigure payload", ErrInvalidCommand)
		}
	case CommandKindProgress:
		if cmd.Progress == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil {
			return fmt.Errorf("%w: progress command must set only progress payload", ErrInvalidCommand)
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
	if next.AppliedCommands == nil {
		next.AppliedCommands = make(map[string]AppliedCommand)
	}
	next.AppliedCommands[cmd.ID] = AppliedCommand{
		Command:      cloneCommand(cmd),
		Version:      next.Version,
		LastLogIndex: logIndex,
		Cluster:      cloneClusterState(cluster),
		Plan:         clonePlan(plan),
	}
	return next
}

func (r *Runtime) snapshotForApplied(applied AppliedCommand) State {
	snapshot := State{
		Version:         applied.Version,
		LastLogIndex:    applied.LastLogIndex,
		Cluster:         cloneClusterState(applied.Cluster),
		AppliedCommands: make(map[string]AppliedCommand),
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
		AppliedCommands: map[string]AppliedCommand{},
	}
}

func isInitialized(state State) bool {
	return state.Cluster.SlotCount > 0
}

func cloneState(state State) State {
	cloned := State{
		Version:         state.Version,
		LastLogIndex:    state.LastLogIndex,
		Cluster:         cloneClusterState(state.Cluster),
		AppliedCommands: make(map[string]AppliedCommand, len(state.AppliedCommands)),
	}
	for id, applied := range state.AppliedCommands {
		cloned.AppliedCommands[id] = cloneAppliedCommand(applied)
	}
	return cloned
}

func cloneAppliedCommand(applied AppliedCommand) AppliedCommand {
	return AppliedCommand{
		Command:      cloneCommand(applied.Command),
		Version:      applied.Version,
		LastLogIndex: applied.LastLogIndex,
		Cluster:      cloneClusterState(applied.Cluster),
		Plan:         clonePlan(applied.Plan),
	}
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
	return cloned
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
		Node:   coordinator.Node{ID: event.Node.ID, FailureDomains: cloneFailureDomains(event.Node.FailureDomains)},
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
