package quickstart

import (
	"encoding/json"
	"fmt"
	"os"
)

const DefaultManifestPath = "examples/quickstart/cluster.json"

type Config struct {
	Coordinator Coordinator `json:"coordinator"`
	Nodes       []Node      `json:"nodes"`
}

type Coordinator struct {
	RPCAddress        string `json:"rpc_address"`
	AdminAddress      string `json:"admin_address"`
	SlotCount         int    `json:"slot_count"`
	ReplicationFactor int    `json:"replication_factor"`
}

type Node struct {
	ID             string            `json:"id"`
	RPCAddress     string            `json:"rpc_address"`
	AdminAddress   string            `json:"admin_address"`
	FailureDomains map[string]string `json:"failure_domains"`
}

func Load(path string) (Config, error) {
	if path == "" {
		path = DefaultManifestPath
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read quickstart manifest %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode quickstart manifest %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate quickstart manifest %q: %w", path, err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Coordinator.RPCAddress == "" {
		return fmt.Errorf("coordinator rpc_address must not be empty")
	}
	if c.Coordinator.SlotCount <= 0 {
		return fmt.Errorf("coordinator slot_count must be > 0")
	}
	if c.Coordinator.ReplicationFactor <= 0 {
		return fmt.Errorf("coordinator replication_factor must be > 0")
	}
	seen := map[string]bool{}
	for _, node := range c.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node id must not be empty")
		}
		if seen[node.ID] {
			return fmt.Errorf("duplicate node id %q", node.ID)
		}
		seen[node.ID] = true
		if node.RPCAddress == "" {
			return fmt.Errorf("node %q rpc_address must not be empty", node.ID)
		}
	}
	return nil
}

func (c Config) NodeByID(nodeID string) (Node, bool) {
	for _, node := range c.Nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return Node{}, false
}
