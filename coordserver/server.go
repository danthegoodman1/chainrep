package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

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
)

type StorageNodeClient interface {
	AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error
	ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error
	MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error
	RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error
	UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error
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

type Server struct {
	rt              *coordruntime.Runtime
	nodes           map[string]StorageNodeClient
	heartbeats      map[string]storage.NodeStatus
	pending         map[int]PendingWork
	routingSnapshot RoutingSnapshot
	lastPolicy      coordinator.ReconfigurationPolicy
}

func Open(ctx context.Context, store coordruntime.Store, nodes map[string]StorageNodeClient) (*Server, error) {
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
		pending:    map[int]PendingWork{},
	}
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
	s.rebuildRoutingSnapshot()
	delete(s.pending, slot)

	if err := s.reconcileAndDispatch(ctx); err != nil {
		return coordruntime.State{}, err
	}
	return updated, nil
}

func (s *Server) ReportNodeHeartbeat(_ context.Context, status storage.NodeStatus) error {
	if _, ok := s.nodes[status.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	s.heartbeats[status.NodeID] = status
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
		for _, replica := range chain.Replicas {
			if replica.State != coordinator.ReplicaStateActive {
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
