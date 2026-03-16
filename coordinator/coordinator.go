package coordinator

import (
	"errors"
	"fmt"
	"strings"
)

const DefaultSlotCount = 4096

var (
	ErrInvalidConfig       = errors.New("invalid coordinator config")
	ErrInvalidNode         = errors.New("invalid coordinator node")
	ErrPlacementImpossible = errors.New("placement impossible")
)

type Config struct {
	SlotCount         int
	ReplicationFactor int
}

type Node struct {
	ID             string
	FailureDomains map[string]string
}

type Chain struct {
	Slot       int
	ReplicaIDs []string
}

type Placement struct {
	Chains            []Chain
	NodesByID         map[string]Node
	SlotCount         int
	ReplicationFactor int
}

type PlacementError struct {
	Slot            int
	ReplicaPosition int
}

func (e *PlacementError) Error() string {
	return fmt.Sprintf(
		"coordinator: no eligible node for slot %d replica %d",
		e.Slot,
		e.ReplicaPosition,
	)
}

func (e *PlacementError) Is(target error) bool {
	return target == ErrPlacementImpossible
}

type assignmentCounts struct {
	headCount    int
	tailCount    int
	replicaCount int
}

func BuildInitialPlacement(cfg Config, nodes []Node) (*Placement, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	normalizedNodes, nodesByID, err := normalizeNodes(nodes)
	if err != nil {
		return nil, err
	}

	if cfg.ReplicationFactor > len(normalizedNodes) {
		return nil, fmt.Errorf(
			"%w: replication factor %d exceeds node count %d",
			ErrInvalidConfig,
			cfg.ReplicationFactor,
			len(normalizedNodes),
		)
	}

	counts := make(map[string]assignmentCounts, len(normalizedNodes))
	chains := make([]Chain, cfg.SlotCount)
	for slot := 0; slot < cfg.SlotCount; slot++ {
		chain := Chain{
			Slot:       slot,
			ReplicaIDs: make([]string, 0, cfg.ReplicationFactor),
		}

		for replicaPosition := 0; replicaPosition < cfg.ReplicationFactor; replicaPosition++ {
			candidate, ok := selectReplicaCandidate(normalizedNodes, chain, nodesByID, counts)
			if !ok {
				return nil, &PlacementError{
					Slot:            slot,
					ReplicaPosition: replicaPosition,
				}
			}

			appendReplica(&chain, candidate.ID, counts)
		}

		chains[slot] = chain
	}

	return &Placement{
		Chains:            chains,
		NodesByID:         nodesByID,
		SlotCount:         cfg.SlotCount,
		ReplicationFactor: cfg.ReplicationFactor,
	}, nil
}

func validateConfig(cfg Config) error {
	if cfg.SlotCount <= 0 {
		return fmt.Errorf("%w: slot count must be > 0", ErrInvalidConfig)
	}
	if cfg.ReplicationFactor <= 0 {
		return fmt.Errorf("%w: replication factor must be > 0", ErrInvalidConfig)
	}

	return nil
}

func normalizeNodes(nodes []Node) ([]Node, map[string]Node, error) {
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("%w: node list must not be empty", ErrInvalidConfig)
	}

	normalized := make([]Node, 0, len(nodes))
	nodesByID := make(map[string]Node, len(nodes))
	for _, node := range nodes {
		if node.ID == "" {
			return nil, nil, fmt.Errorf("%w: node ID must not be empty", ErrInvalidNode)
		}
		if _, exists := nodesByID[node.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate node ID %q", ErrInvalidNode, node.ID)
		}

		failureDomains := make(map[string]string, len(node.FailureDomains))
		for key, value := range node.FailureDomains {
			if strings.TrimSpace(key) == "" {
				return nil, nil, fmt.Errorf("%w: node %q has empty failure-domain key", ErrInvalidNode, node.ID)
			}
			if strings.TrimSpace(value) == "" {
				return nil, nil, fmt.Errorf(
					"%w: node %q has empty failure-domain value for key %q",
					ErrInvalidNode,
					node.ID,
					key,
				)
			}
			failureDomains[key] = value
		}

		normalizedNode := Node{
			ID:             node.ID,
			FailureDomains: failureDomains,
		}
		normalized = append(normalized, normalizedNode)
		nodesByID[node.ID] = normalizedNode
	}

	return normalized, nodesByID, nil
}

func selectReplicaCandidate(
	nodes []Node,
	chain Chain,
	nodesByID map[string]Node,
	counts map[string]assignmentCounts,
) (Node, bool) {
	var best Node
	found := false

	for _, node := range nodes {
		if !isEligible(node, chain, nodesByID) {
			continue
		}
		if !found || compareNodes(node, best, counts) < 0 {
			best = node
			found = true
		}
	}

	return best, found
}

func isEligible(candidate Node, chain Chain, nodesByID map[string]Node) bool {
	for _, replicaID := range chain.ReplicaIDs {
		if replicaID == candidate.ID {
			return false
		}
		if hasFailureDomainConflict(candidate, nodesByID[replicaID]) {
			return false
		}
	}

	return true
}

func hasFailureDomainConflict(candidate Node, existing Node) bool {
	for key, candidateValue := range candidate.FailureDomains {
		existingValue, ok := existing.FailureDomains[key]
		if ok && existingValue == candidateValue {
			return true
		}
	}

	return false
}

func compareNodes(a Node, b Node, counts map[string]assignmentCounts) int {
	aCounts := counts[a.ID]
	bCounts := counts[b.ID]

	if aCounts.tailCount != bCounts.tailCount {
		if aCounts.tailCount < bCounts.tailCount {
			return -1
		}
		return 1
	}
	if aCounts.headCount != bCounts.headCount {
		if aCounts.headCount < bCounts.headCount {
			return -1
		}
		return 1
	}
	if aCounts.replicaCount != bCounts.replicaCount {
		if aCounts.replicaCount < bCounts.replicaCount {
			return -1
		}
		return 1
	}

	return strings.Compare(a.ID, b.ID)
}

func appendReplica(chain *Chain, nodeID string, counts map[string]assignmentCounts) {
	if len(chain.ReplicaIDs) == 0 {
		chain.ReplicaIDs = append(chain.ReplicaIDs, nodeID)
		nodeCounts := counts[nodeID]
		nodeCounts.headCount++
		nodeCounts.tailCount++
		nodeCounts.replicaCount++
		counts[nodeID] = nodeCounts
		return
	}

	previousTailID := chain.ReplicaIDs[len(chain.ReplicaIDs)-1]
	previousTailCounts := counts[previousTailID]
	previousTailCounts.tailCount--
	counts[previousTailID] = previousTailCounts

	chain.ReplicaIDs = append(chain.ReplicaIDs, nodeID)
	nodeCounts := counts[nodeID]
	nodeCounts.tailCount++
	nodeCounts.replicaCount++
	counts[nodeID] = nodeCounts
}
