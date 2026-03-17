package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/storage"
)

var (
	ErrUnknownNode          = errors.New("unknown coordinator server node")
	ErrDispatchFailed       = errors.New("coordinator server dispatch failed")
	ErrUnexpectedProgress   = errors.New("unexpected coordinator server progress")
	ErrConflictingPending   = errors.New("conflicting coordinator server pending work")
	ErrStateMismatch        = errors.New("coordinator server state mismatch")
	ErrInvalidServerCommand = errors.New("invalid coordinator server command")
	ErrRecoveryFailed       = errors.New("coordinator server recovery failed")
	ErrInvalidServerConfig  = errors.New("invalid coordinator server config")
)

type StorageNodeClient interface {
	AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error
	ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error
	MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error
	RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error
	UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error
	ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error
	RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error
	DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error
}

type pendingKind string

const (
	pendingKindReady   pendingKind = "ready"
	pendingKindRemoved pendingKind = "removed"
)

type PendingWork struct {
	Slot      int
	NodeID    string
	Kind      pendingKind
	CommandID string
}

type SlotRoute struct {
	Slot         int
	ChainVersion uint64
	HeadNodeID   string
	TailNodeID   string
	Writable     bool
	Readable     bool
}

type RoutingSnapshot struct {
	Version   uint64
	SlotCount int
	Slots     []SlotRoute
}

type LivenessPolicy struct {
	SuspectAfter time.Duration
	DeadAfter    time.Duration
}

type Clock interface {
	Now() time.Time
}

type ServerConfig struct {
	LivenessPolicy LivenessPolicy
	Clock          Clock
}

type Server struct {
	rt                     *coordruntime.Runtime
	nodes                  map[string]StorageNodeClient
	heartbeats             map[string]storage.NodeStatus
	liveness               map[string]coordruntime.NodeLivenessRecord
	pending                map[int]PendingWork
	routingSnapshot        RoutingSnapshot
	lastPolicy             coordinator.ReconfigurationPolicy
	unavailableReplicas    map[string]map[int]bool
	lastRecoveryReports    map[string]storage.NodeRecoveryReport
	livenessPolicy         LivenessPolicy
	clock                  Clock
}

func Open(ctx context.Context, store coordruntime.Store, nodes map[string]StorageNodeClient) (*Server, error) {
	return OpenWithConfig(ctx, store, nodes, ServerConfig{})
}

func OpenWithConfig(
	ctx context.Context,
	store coordruntime.Store,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
) (*Server, error) {
	if err := validateServerConfig(cfg); err != nil {
		return nil, fmt.Errorf("err in validateServerConfig: %w", err)
	}
	rt, err := coordruntime.Open(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("err in coordruntime.Open: %w", err)
	}

	clonedNodes := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		clonedNodes[nodeID] = node
	}

	server := &Server{
		rt:         rt,
		nodes:      clonedNodes,
		heartbeats: map[string]storage.NodeStatus{},
		liveness:   map[string]coordruntime.NodeLivenessRecord{},
		pending:    map[int]PendingWork{},
		unavailableReplicas: map[string]map[int]bool{},
		lastRecoveryReports: map[string]storage.NodeRecoveryReport{},
		livenessPolicy: cfg.LivenessPolicy,
		clock:          cfg.Clock,
	}
	if server.clock == nil {
		server.clock = realClock{}
	}
	server.syncViewsFromRuntime()
	server.rebuildRoutingSnapshot()
	return server, nil
}

func (s *Server) Current() coordruntime.State {
	return s.rt.Current()
}

func (s *Server) Heartbeats() map[string]storage.NodeStatus {
	cloned := make(map[string]storage.NodeStatus, len(s.heartbeats))
	for nodeID, status := range s.heartbeats {
		cloned[nodeID] = status
	}
	return cloned
}

func (s *Server) Liveness() map[string]coordruntime.NodeLivenessRecord {
	cloned := make(map[string]coordruntime.NodeLivenessRecord, len(s.liveness))
	for nodeID, record := range s.liveness {
		cloned[nodeID] = cloneLivenessRecord(record)
	}
	return cloned
}

func (s *Server) Pending() map[int]PendingWork {
	cloned := make(map[int]PendingWork, len(s.pending))
	for slot, pending := range s.pending {
		cloned[slot] = pending
	}
	return cloned
}

func (s *Server) RoutingSnapshot(_ context.Context) (RoutingSnapshot, error) {
	return cloneRoutingSnapshot(s.routingSnapshot), nil
}

func (s *Server) Bootstrap(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if cmd.Kind != coordruntime.CommandKindBootstrap || cmd.Bootstrap == nil {
		return coordruntime.State{}, fmt.Errorf("%w: bootstrap requires bootstrap command payload", ErrInvalidServerCommand)
	}
	state, err := s.rt.Bootstrap(ctx, cmd)
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in s.rt.Bootstrap: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	return state, nil
}

func (s *Server) AddNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindAddNode)
}

func (s *Server) BeginDrainNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindBeginDrainNode)
}

func (s *Server) MarkNodeDead(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindMarkNodeDead)
}

func (s *Server) ReportReplicaReady(ctx context.Context, nodeID string, slot int, commandID string) (coordruntime.State, error) {
	pending, ok := s.pending[slot]
	if !ok || pending.Kind != pendingKindReady || pending.NodeID != nodeID {
		return coordruntime.State{}, fmt.Errorf(
			"%w: unexpected ready report for node %q slot %d",
			ErrUnexpectedProgress,
			nodeID,
			slot,
		)
	}
	if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
		return coordruntime.State{}, fmt.Errorf(
			"%w: ready report command %q does not match pending %q",
			ErrUnexpectedProgress,
			commandID,
			pending.CommandID,
		)
	}

	state := s.rt.Current()
	if !slotContainsReplicaInState(state.Cluster, slot, nodeID, coordinator.ReplicaStateJoining) {
		return coordruntime.State{}, fmt.Errorf(
			"%w: node %q slot %d is not joining in current coordinator state",
			ErrStateMismatch,
			nodeID,
			slot,
		)
	}

	progressID := commandID
	if progressID == "" {
		progressID = fmt.Sprintf("server-progress-ready-%s-%d-v%d", nodeID, slot, state.Version)
	}
	if _, err := s.rt.ApplyProgress(ctx, coordruntime.Command{
		ID:              progressID,
		ExpectedVersion: state.Version,
		Kind:            coordruntime.CommandKindProgress,
		Progress: &coordruntime.ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				NodeID: nodeID,
				Slot:   slot,
			},
		},
	}); err != nil {
		return coordruntime.State{}, fmt.Errorf("err in s.rt.ApplyProgress: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	delete(s.pending, slot)

	if err := s.reconcileAndDispatch(ctx); err != nil {
		return coordruntime.State{}, err
	}
	return s.rt.Current(), nil
}

func (s *Server) ReportReplicaRemoved(ctx context.Context, nodeID string, slot int, commandID string) (coordruntime.State, error) {
	pending, ok := s.pending[slot]
	if !ok || pending.Kind != pendingKindRemoved || pending.NodeID != nodeID {
		return coordruntime.State{}, fmt.Errorf(
			"%w: unexpected removed report for node %q slot %d",
			ErrUnexpectedProgress,
			nodeID,
			slot,
		)
	}
	if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
		return coordruntime.State{}, fmt.Errorf(
			"%w: removed report command %q does not match pending %q",
			ErrUnexpectedProgress,
			commandID,
			pending.CommandID,
		)
	}

	state := s.rt.Current()
	if !slotContainsReplicaInState(state.Cluster, slot, nodeID, coordinator.ReplicaStateLeaving) {
		return coordruntime.State{}, fmt.Errorf(
			"%w: node %q slot %d is not leaving in current coordinator state",
			ErrStateMismatch,
			nodeID,
			slot,
		)
	}

	progressID := commandID
	if progressID == "" {
		progressID = fmt.Sprintf("server-progress-removed-%s-%d-v%d", nodeID, slot, state.Version)
	}
	updated, err := s.rt.ApplyProgress(ctx, coordruntime.Command{
		ID:              progressID,
		ExpectedVersion: state.Version,
		Kind:            coordruntime.CommandKindProgress,
		Progress: &coordruntime.ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaRemoved,
				NodeID: nodeID,
				Slot:   slot,
			},
		},
	})
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in s.rt.ApplyProgress: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	delete(s.pending, slot)

	if err := s.reconcileAndDispatch(ctx); err != nil {
		return coordruntime.State{}, err
	}
	return updated, nil
}

func (s *Server) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if _, ok := s.nodes[status.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	current := s.rt.Current()
	currentRecord, hadCurrentRecord := current.NodeLivenessByID[status.NodeID]
	wasDead := currentRecord.State == coordruntime.NodeLivenessStateDead ||
		nodeMarkedDead(current.Cluster, status.NodeID)
	observedAt := s.clock.Now().UnixNano()
	_, err := s.rt.Heartbeat(ctx, coordruntime.Command{
		ID:              fmt.Sprintf("server-heartbeat-%s-%d", status.NodeID, observedAt),
		ExpectedVersion: s.rt.Current().Version,
		Kind:            coordruntime.CommandKindHeartbeat,
		Heartbeat: &coordruntime.HeartbeatCommand{
			Status:             status,
			ObservedAtUnixNano: observedAt,
		},
	})
	if err != nil {
		return fmt.Errorf("err in s.rt.Heartbeat: %w", err)
	}
	s.syncViewsFromRuntime()
	if wasDead && s.liveness[status.NodeID].State != coordruntime.NodeLivenessStateDead {
		deadActionFired := hadCurrentRecord && currentRecord.DeadActionFired
		if nodeMarkedDead(current.Cluster, status.NodeID) {
			deadActionFired = true
		}
		if _, err := s.applyLivenessTransition(ctx, status.NodeID, coordruntime.NodeLivenessStateDead, observedAt, deadActionFired); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) EvaluateLiveness(ctx context.Context) error {
	if s.livenessPolicy.SuspectAfter <= 0 || s.livenessPolicy.DeadAfter <= 0 {
		return nil
	}
	nowUnix := s.clock.Now().UnixNano()

	nodeIDs := make([]string, 0, len(s.liveness))
	for nodeID := range s.liveness {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		record := s.liveness[nodeID]
		age := time.Duration(nowUnix - record.LastHeartbeatUnixNano)
		target := record.State
		switch {
		case age >= s.livenessPolicy.DeadAfter:
			target = coordruntime.NodeLivenessStateDead
		case age >= s.livenessPolicy.SuspectAfter:
			target = coordruntime.NodeLivenessStateSuspect
		default:
			target = coordruntime.NodeLivenessStateHealthy
		}

		if target != record.State {
			updated, err := s.applyLivenessTransition(ctx, nodeID, target, nowUnix, record.DeadActionFired)
			if err != nil {
				return err
			}
			record = updated
		}

		if record.State == coordruntime.NodeLivenessStateDead && !record.DeadActionFired {
			if len(s.pending) > 0 {
				continue
			}
			if nodeMarkedDead(s.rt.Current().Cluster, nodeID) || !isRuntimeInitialized(s.rt.Current()) {
				updated, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true)
				if err != nil {
					return err
				}
				record = updated
				_ = record
				continue
			}

			if _, err := s.MarkNodeDead(ctx, coordruntime.Command{
				ID:              fmt.Sprintf("server-auto-dead-%s-v%d", nodeID, s.rt.Current().Version),
				ExpectedVersion: s.rt.Current().Version,
				Kind:            coordruntime.CommandKindReconfigure,
				Reconfigure: &coordruntime.ReconfigureCommand{
					Events: []coordinator.Event{{
						Kind:   coordinator.EventKindMarkNodeDead,
						NodeID: nodeID,
					}},
					Policy: s.lastPolicy,
				},
			}); err != nil {
				return fmt.Errorf("err in s.MarkNodeDead: %w", err)
			}
			if _, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if _, ok := s.nodes[report.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, report.NodeID)
	}
	if prior, ok := s.lastRecoveryReports[report.NodeID]; ok && reflect.DeepEqual(prior, report) && !s.nodeHasUnavailableSlots(report.NodeID) {
		return nil
	}

	reportSlots := make(map[int]storage.RecoveredReplica, len(report.Replicas))
	s.markUnavailableReplicas(report)

	state := s.rt.Current()
	for _, recovered := range report.Replicas {
		reportSlots[recovered.Assignment.Slot] = recovered
		currentAssignment, ok := currentAssignmentForNode(state, report.NodeID, recovered.Assignment.Slot)
		if !ok {
			if err := s.dropRecoveredReplica(ctx, report.NodeID, recovered.Assignment.Slot); err != nil {
				return err
			}
			continue
		}

		switch {
		case canResumeRecoveredReplica(recovered, currentAssignment):
			if err := s.resumeRecoveredReplica(ctx, report.NodeID, currentAssignment); err != nil {
				return err
			}
		default:
			sourceNodeID, ok := recoverySourceNodeID(state.Cluster.Chains[recovered.Assignment.Slot], report.NodeID)
			if !ok {
				return fmt.Errorf(
					"%w: slot %d node %q has no valid recovery source",
					ErrRecoveryFailed,
					recovered.Assignment.Slot,
					report.NodeID,
				)
			}
			if err := s.recoverReplica(ctx, report.NodeID, currentAssignment, sourceNodeID); err != nil {
				return err
			}
		}
	}

	s.lastRecoveryReports[report.NodeID] = cloneRecoveryReport(report)
	s.rebuildRoutingSnapshot()
	return nil
}

func (s *Server) applyMembershipMutation(
	ctx context.Context,
	cmd coordruntime.Command,
	expectedEvent coordinator.EventKind,
) (coordruntime.State, error) {
	if cmd.Kind != coordruntime.CommandKindReconfigure || cmd.Reconfigure == nil {
		return coordruntime.State{}, fmt.Errorf("%w: mutation requires reconfigure command payload", ErrInvalidServerCommand)
	}
	if len(cmd.Reconfigure.Events) != 1 || cmd.Reconfigure.Events[0].Kind != expectedEvent {
		return coordruntime.State{}, fmt.Errorf(
			"%w: expected exactly one %q event",
			ErrInvalidServerCommand,
			expectedEvent,
		)
	}

	plan, state, err := s.rt.Reconfigure(ctx, cmd)
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in s.rt.Reconfigure: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	s.lastPolicy = cmd.Reconfigure.Policy

	if err := s.dispatchPlan(ctx, state.Version, plan); err != nil {
		return coordruntime.State{}, err
	}
	return state, nil
}

func (s *Server) reconcileAndDispatch(ctx context.Context) error {
	current := s.rt.Current()
	preview, err := coordinator.PlanReconfiguration(current.Cluster, nil, s.lastPolicy)
	if err != nil {
		return fmt.Errorf("err in coordinator.PlanReconfiguration preview: %w", err)
	}
	if len(preview.ChangedSlots) == 0 && reflect.DeepEqual(preview.UpdatedState, current.Cluster) {
		return nil
	}

	cmd := coordruntime.Command{
		ID:              fmt.Sprintf("server-reconcile-v%d", current.Version),
		ExpectedVersion: current.Version,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: nil,
			Policy: s.lastPolicy,
		},
	}
	plan, state, err := s.rt.Reconfigure(ctx, cmd)
	if err != nil {
		return fmt.Errorf("err in s.rt.Reconcile: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	if err := s.dispatchPlan(ctx, state.Version, plan); err != nil {
		return err
	}
	return nil
}

func (s *Server) dispatchPlan(ctx context.Context, chainVersion uint64, plan coordinator.ReconfigurationPlan) error {
	slotPlans := append([]coordinator.SlotPlan(nil), plan.ChangedSlots...)
	sort.Slice(slotPlans, func(i, j int) bool {
		return slotPlans[i].Slot < slotPlans[j].Slot
	})

	for _, slotPlan := range slotPlans {
		stepKinds := distinctStepKinds(slotPlan.Steps)
		switch {
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindAppendTail:
			if err := s.dispatchAppendTail(ctx, chainVersion, slotPlan); err != nil {
				return err
			}
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindMarkLeaving:
			if err := s.dispatchMarkLeaving(ctx, chainVersion, slotPlan); err != nil {
				return err
			}
		default:
			return fmt.Errorf(
				"%w: unsupported slot plan for slot %d",
				ErrDispatchFailed,
				slotPlan.Slot,
			)
		}
	}
	return nil
}

func (s *Server) dispatchAppendTail(ctx context.Context, chainVersion uint64, slotPlan coordinator.SlotPlan) error {
	addedNodeID := ""
	replacedNodeID := ""
	for _, step := range slotPlan.Steps {
		if step.Kind == coordinator.StepKindAppendTail {
			addedNodeID = step.NodeID
			replacedNodeID = step.ReplacedNodeID
			break
		}
	}
	if addedNodeID == "" {
		return fmt.Errorf("%w: slot %d missing append target", ErrDispatchFailed, slotPlan.Slot)
	}
	client, ok := s.nodes[addedNodeID]
	if !ok {
		return fmt.Errorf("%w: %w: %q", ErrDispatchFailed, ErrUnknownNode, addedNodeID)
	}
	if existing, ok := s.pending[slotPlan.Slot]; ok && existing.NodeID != addedNodeID {
		return fmt.Errorf("%w: slot %d already has pending work for node %q", ErrConflictingPending, slotPlan.Slot, existing.NodeID)
	}

	addAssignment, err := assignmentForNode(slotPlan.After, addedNodeID, chainVersion)
	if err != nil {
		return fmt.Errorf("err in assignmentForNode(add): %w", err)
	}
	if err := client.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: addAssignment}); err != nil {
		return fmt.Errorf("%w: err in node[%q].AddReplicaAsTail: %v", ErrDispatchFailed, addedNodeID, err)
	}
	s.pending[slotPlan.Slot] = PendingWork{
		Slot:   slotPlan.Slot,
		NodeID: addedNodeID,
		Kind:   pendingKindReady,
	}

	skipped := map[string]bool{addedNodeID: true}
	servingChain := activeServingChain(slotPlan.After)
	updateNodes := activeAfterNodeIDs(servingChain, skipped)
	for _, nodeID := range updateNodes {
		client, ok := s.nodes[nodeID]
		if !ok {
			return fmt.Errorf("%w: %w: %q", ErrDispatchFailed, ErrUnknownNode, nodeID)
		}
		assignment, err := assignmentForNode(servingChain, nodeID, chainVersion)
		if err != nil {
			return fmt.Errorf("err in assignmentForNode(update append): %w", err)
		}
		if err := client.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{Assignment: assignment}); err != nil {
			return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %v", ErrDispatchFailed, nodeID, err)
		}
	}
	_ = replacedNodeID
	return nil
}

func (s *Server) dispatchMarkLeaving(ctx context.Context, chainVersion uint64, slotPlan coordinator.SlotPlan) error {
	leavingNodeID := ""
	for _, step := range slotPlan.Steps {
		if step.Kind == coordinator.StepKindMarkLeaving {
			leavingNodeID = step.NodeID
			break
		}
	}
	if leavingNodeID == "" {
		return fmt.Errorf("%w: slot %d missing leaving target", ErrDispatchFailed, slotPlan.Slot)
	}
	if existing, ok := s.pending[slotPlan.Slot]; ok && existing.NodeID != leavingNodeID {
		return fmt.Errorf("%w: slot %d already has pending work for node %q", ErrConflictingPending, slotPlan.Slot, existing.NodeID)
	}

	skipped := map[string]bool{leavingNodeID: true}
	servingChain := activeServingChain(slotPlan.After)
	updateNodes := activeAfterNodeIDs(servingChain, skipped)
	for _, nodeID := range updateNodes {
		client, ok := s.nodes[nodeID]
		if !ok {
			return fmt.Errorf("%w: %w: %q", ErrDispatchFailed, ErrUnknownNode, nodeID)
		}
		assignment, err := assignmentForNode(servingChain, nodeID, chainVersion)
		if err != nil {
			return fmt.Errorf("err in assignmentForNode(update leaving): %w", err)
		}
		if err := client.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{Assignment: assignment}); err != nil {
			return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %v", ErrDispatchFailed, nodeID, err)
		}
	}

	client, ok := s.nodes[leavingNodeID]
	if !ok {
		return fmt.Errorf("%w: %w: %q", ErrDispatchFailed, ErrUnknownNode, leavingNodeID)
	}
	if err := client.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{Slot: slotPlan.Slot}); err != nil {
		return fmt.Errorf("%w: err in node[%q].MarkReplicaLeaving: %v", ErrDispatchFailed, leavingNodeID, err)
	}
	s.pending[slotPlan.Slot] = PendingWork{
		Slot:   slotPlan.Slot,
		NodeID: leavingNodeID,
		Kind:   pendingKindRemoved,
	}
	return nil
}

func (s *Server) resumeRecoveredReplica(
	ctx context.Context,
	nodeID string,
	assignment storage.ReplicaAssignment,
) error {
	client, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: %w: %q", ErrRecoveryFailed, ErrUnknownNode, nodeID)
	}
	if err := client.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{Assignment: assignment}); err != nil {
		return fmt.Errorf("%w: err in node[%q].ResumeRecoveredReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, assignment.Slot)
	return nil
}

func (s *Server) recoverReplica(
	ctx context.Context,
	nodeID string,
	assignment storage.ReplicaAssignment,
	sourceNodeID string,
) error {
	client, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: %w: %q", ErrRecoveryFailed, ErrUnknownNode, nodeID)
	}
	if _, ok := s.nodes[sourceNodeID]; !ok {
		return fmt.Errorf("%w: %w: %q", ErrRecoveryFailed, ErrUnknownNode, sourceNodeID)
	}
	if err := client.RecoverReplica(ctx, storage.RecoverReplicaCommand{
		Assignment:   assignment,
		SourceNodeID: sourceNodeID,
	}); err != nil {
		return fmt.Errorf("%w: err in node[%q].RecoverReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, assignment.Slot)
	return nil
}

func (s *Server) dropRecoveredReplica(ctx context.Context, nodeID string, slot int) error {
	client, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: %w: %q", ErrRecoveryFailed, ErrUnknownNode, nodeID)
	}
	if err := client.DropRecoveredReplica(ctx, storage.DropRecoveredReplicaCommand{Slot: slot}); err != nil {
		return fmt.Errorf("%w: err in node[%q].DropRecoveredReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, slot)
	return nil
}

func assignmentForNode(chain coordinator.Chain, nodeID string, chainVersion uint64) (storage.ReplicaAssignment, error) {
	position := -1
	for i, replica := range chain.Replicas {
		if replica.NodeID == nodeID {
			position = i
			break
		}
	}
	if position < 0 {
		return storage.ReplicaAssignment{}, fmt.Errorf("%w: node %q not found in chain %d", ErrStateMismatch, nodeID, chain.Slot)
	}

	role := storage.ReplicaRoleMiddle
	switch len(chain.Replicas) {
	case 1:
		role = storage.ReplicaRoleSingle
	default:
		switch position {
		case 0:
			role = storage.ReplicaRoleHead
		case len(chain.Replicas) - 1:
			role = storage.ReplicaRoleTail
		default:
			role = storage.ReplicaRoleMiddle
		}
	}

	assignment := storage.ReplicaAssignment{
		Slot:         chain.Slot,
		ChainVersion: chainVersion,
		Role:         role,
	}
	if position > 0 {
		assignment.Peers.PredecessorNodeID = chain.Replicas[position-1].NodeID
	}
	if position+1 < len(chain.Replicas) {
		assignment.Peers.SuccessorNodeID = chain.Replicas[position+1].NodeID
	}
	return assignment, nil
}

func activeAfterNodeIDs(chain coordinator.Chain, skipped map[string]bool) []string {
	nodeIDs := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		if skipped[replica.NodeID] {
			continue
		}
		nodeIDs = append(nodeIDs, replica.NodeID)
	}
	return nodeIDs
}

func (s *Server) rebuildRoutingSnapshot() {
	state := s.rt.Current()
	snapshot := RoutingSnapshot{
		Version:   state.Version,
		SlotCount: state.Cluster.SlotCount,
		Slots:     make([]SlotRoute, 0, len(state.Cluster.Chains)),
	}
	for _, chain := range state.Cluster.Chains {
		route := SlotRoute{
			Slot:         chain.Slot,
			ChainVersion: state.SlotVersions[chain.Slot],
		}
		if chainHasUnavailableReplica(chain, s.unavailableReplicas) {
			snapshot.Slots = append(snapshot.Slots, route)
			continue
		}
		for _, replica := range chain.Replicas {
			if replica.State != coordinator.ReplicaStateActive {
				continue
			}
			if s.replicaUnavailable(replica.NodeID, chain.Slot) {
				continue
			}
			if route.HeadNodeID == "" {
				route.HeadNodeID = replica.NodeID
			}
			route.TailNodeID = replica.NodeID
		}
		if route.HeadNodeID != "" {
			route.Writable = !chainHasReplicaState(chain, coordinator.ReplicaStateJoining)
		}
		if route.TailNodeID != "" {
			route.Readable = true
		}
		snapshot.Slots = append(snapshot.Slots, route)
	}
	s.routingSnapshot = snapshot
}

func cloneRoutingSnapshot(snapshot RoutingSnapshot) RoutingSnapshot {
	return RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: snapshot.SlotCount,
		Slots:     append([]SlotRoute(nil), snapshot.Slots...),
	}
}

func activeServingChain(chain coordinator.Chain) coordinator.Chain {
	serving := coordinator.Chain{
		Slot:     chain.Slot,
		Replicas: make([]coordinator.Replica, 0, len(chain.Replicas)),
	}
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		serving.Replicas = append(serving.Replicas, replica)
	}
	return serving
}

func chainHasReplicaState(chain coordinator.Chain, want coordinator.ReplicaState) bool {
	for _, replica := range chain.Replicas {
		if replica.State == want {
			return true
		}
	}
	return false
}

func affectedPeerUpdateNodes(before coordinator.Chain, after coordinator.Chain, skipped map[string]bool) []string {
	beforeAssignments := buildAssignmentMap(before)
	afterAssignments := buildAssignmentMap(after)

	var nodeIDs []string
	for nodeID, afterAssignment := range afterAssignments {
		if skipped[nodeID] {
			continue
		}
		beforeAssignment, ok := beforeAssignments[nodeID]
		if !ok || !reflect.DeepEqual(beforeAssignment, afterAssignment) {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}

	sort.Slice(nodeIDs, func(i, j int) bool {
		return afterAssignments[nodeIDs[i]].position < afterAssignments[nodeIDs[j]].position
	})
	return nodeIDs
}

type chainAssignment struct {
	role     storage.ReplicaRole
	peers    storage.ChainPeers
	position int
}

func buildAssignmentMap(chain coordinator.Chain) map[string]chainAssignment {
	assignments := make(map[string]chainAssignment, len(chain.Replicas))
	for i, replica := range chain.Replicas {
		role := storage.ReplicaRoleMiddle
		switch len(chain.Replicas) {
		case 1:
			role = storage.ReplicaRoleSingle
		default:
			switch i {
			case 0:
				role = storage.ReplicaRoleHead
			case len(chain.Replicas) - 1:
				role = storage.ReplicaRoleTail
			default:
				role = storage.ReplicaRoleMiddle
			}
		}
		assignment := chainAssignment{
			role:     role,
			position: i,
		}
		if i > 0 {
			assignment.peers.PredecessorNodeID = chain.Replicas[i-1].NodeID
		}
		if i+1 < len(chain.Replicas) {
			assignment.peers.SuccessorNodeID = chain.Replicas[i+1].NodeID
		}
		assignments[replica.NodeID] = assignment
	}
	return assignments
}

func slotContainsReplicaInState(
	state coordinator.ClusterState,
	slot int,
	nodeID string,
	wantState coordinator.ReplicaState,
) bool {
	if slot < 0 || slot >= len(state.Chains) {
		return false
	}
	for _, replica := range state.Chains[slot].Replicas {
		if replica.NodeID == nodeID && replica.State == wantState {
			return true
		}
	}
	return false
}

func distinctStepKinds(steps []coordinator.ReconfigurationStep) []coordinator.StepKind {
	seen := make(map[coordinator.StepKind]struct{}, len(steps))
	kinds := make([]coordinator.StepKind, 0, len(steps))
	for _, step := range steps {
		if _, ok := seen[step.Kind]; ok {
			continue
		}
		seen[step.Kind] = struct{}{}
		kinds = append(kinds, step.Kind)
	}
	sort.Slice(kinds, func(i, j int) bool {
		return kinds[i] < kinds[j]
	})
	return kinds
}

func (s *Server) markUnavailableReplicas(report storage.NodeRecoveryReport) {
	slots := s.unavailableReplicas[report.NodeID]
	if slots == nil {
		slots = map[int]bool{}
		s.unavailableReplicas[report.NodeID] = slots
	}
	for _, replica := range report.Replicas {
		slots[replica.Assignment.Slot] = true
	}
	s.rebuildRoutingSnapshot()
}

func (s *Server) clearUnavailable(nodeID string, slot int) {
	slots, ok := s.unavailableReplicas[nodeID]
	if !ok {
		return
	}
	delete(slots, slot)
	if len(slots) == 0 {
		delete(s.unavailableReplicas, nodeID)
	}
	s.rebuildRoutingSnapshot()
}

func (s *Server) replicaUnavailable(nodeID string, slot int) bool {
	slots, ok := s.unavailableReplicas[nodeID]
	if !ok {
		return false
	}
	return slots[slot]
}

func (s *Server) nodeHasUnavailableSlots(nodeID string) bool {
	slots, ok := s.unavailableReplicas[nodeID]
	return ok && len(slots) > 0
}

func chainHasUnavailableReplica(chain coordinator.Chain, unavailable map[string]map[int]bool) bool {
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		if unavailable[replica.NodeID][chain.Slot] {
			return true
		}
	}
	return false
}

func currentAssignmentForNode(
	state coordruntime.State,
	nodeID string,
	slot int,
) (storage.ReplicaAssignment, bool) {
	if slot < 0 || slot >= len(state.Cluster.Chains) {
		return storage.ReplicaAssignment{}, false
	}
	chain := state.Cluster.Chains[slot]
	for _, replica := range chain.Replicas {
		if replica.NodeID != nodeID {
			continue
		}
		assignment, err := assignmentForNode(chain, nodeID, state.SlotVersions[slot])
		if err != nil {
			return storage.ReplicaAssignment{}, false
		}
		return assignment, true
	}
	return storage.ReplicaAssignment{}, false
}

func assignedReplicasForNode(state coordruntime.State, nodeID string) map[int]storage.ReplicaAssignment {
	assignments := map[int]storage.ReplicaAssignment{}
	for _, chain := range state.Cluster.Chains {
		for _, replica := range chain.Replicas {
			if replica.NodeID != nodeID {
				continue
			}
			assignment, err := assignmentForNode(chain, nodeID, state.SlotVersions[chain.Slot])
			if err != nil {
				continue
			}
			assignments[chain.Slot] = assignment
		}
	}
	return assignments
}

func canResumeRecoveredReplica(recovered storage.RecoveredReplica, current storage.ReplicaAssignment) bool {
	return recovered.HasCommittedData &&
		recovered.LastKnownState == storage.ReplicaStateActive &&
		reflect.DeepEqual(recovered.Assignment, current)
}

func recoverySourceNodeID(chain coordinator.Chain, recoveringNodeID string) (string, bool) {
	for i, replica := range chain.Replicas {
		if replica.NodeID != recoveringNodeID {
			continue
		}
		if i > 0 {
			return chain.Replicas[i-1].NodeID, true
		}
		if i+1 < len(chain.Replicas) {
			return chain.Replicas[i+1].NodeID, true
		}
		return "", false
	}
	return "", false
}

func cloneRecoveryReport(report storage.NodeRecoveryReport) storage.NodeRecoveryReport {
	cloned := storage.NodeRecoveryReport{
		NodeID:   report.NodeID,
		Replicas: make([]storage.RecoveredReplica, 0, len(report.Replicas)),
	}
	for _, replica := range report.Replicas {
		cloned.Replicas = append(cloned.Replicas, storage.RecoveredReplica{
			Assignment:               replica.Assignment,
			LastKnownState:           replica.LastKnownState,
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return cloned
}

func (s *Server) applyLivenessTransition(
	ctx context.Context,
	nodeID string,
	state coordruntime.NodeLivenessState,
	evaluatedAtUnixNano int64,
	deadActionFired bool,
) (coordruntime.NodeLivenessRecord, error) {
	current := s.rt.Current()
	if _, err := s.rt.ApplyLiveness(ctx, coordruntime.Command{
		ID:              fmt.Sprintf("server-liveness-%s-%s-%d-%t-v%d", nodeID, state, evaluatedAtUnixNano, deadActionFired, current.Version),
		ExpectedVersion: current.Version,
		Kind:            coordruntime.CommandKindLiveness,
		Liveness: &coordruntime.LivenessCommand{
			NodeID:              nodeID,
			State:               state,
			EvaluatedAtUnixNano: evaluatedAtUnixNano,
			DeadActionFired:     deadActionFired,
		},
	}); err != nil {
		return coordruntime.NodeLivenessRecord{}, fmt.Errorf("err in s.rt.ApplyLiveness: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	return cloneLivenessRecord(s.liveness[nodeID]), nil
}

func (s *Server) syncViewsFromRuntime() {
	current := s.rt.Current()
	s.heartbeats = make(map[string]storage.NodeStatus, len(current.NodeLivenessByID))
	s.liveness = make(map[string]coordruntime.NodeLivenessRecord, len(current.NodeLivenessByID))
	for nodeID, record := range current.NodeLivenessByID {
		s.heartbeats[nodeID] = cloneNodeStatus(record.LastStatus)
		s.liveness[nodeID] = cloneLivenessRecord(record)
	}
}

func validateServerConfig(cfg ServerConfig) error {
	if cfg.LivenessPolicy.SuspectAfter < 0 || cfg.LivenessPolicy.DeadAfter < 0 {
		return fmt.Errorf("%w: liveness durations must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.LivenessPolicy.DeadAfter > 0 &&
		cfg.LivenessPolicy.SuspectAfter > 0 &&
		cfg.LivenessPolicy.DeadAfter < cfg.LivenessPolicy.SuspectAfter {
		return fmt.Errorf("%w: dead timeout must be >= suspect timeout", ErrInvalidServerConfig)
	}
	return nil
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func cloneLivenessRecord(record coordruntime.NodeLivenessRecord) coordruntime.NodeLivenessRecord {
	return coordruntime.NodeLivenessRecord{
		LastHeartbeatUnixNano: record.LastHeartbeatUnixNano,
		State:                 record.State,
		LastStatus:            cloneNodeStatus(record.LastStatus),
		DeadActionFired:       record.DeadActionFired,
	}
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

func nodeMarkedDead(cluster coordinator.ClusterState, nodeID string) bool {
	return cluster.NodeHealthByID[nodeID] == coordinator.NodeHealthDead
}

func isRuntimeInitialized(state coordruntime.State) bool {
	return state.Cluster.SlotCount > 0
}
