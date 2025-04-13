package nodes

import (
    "fmt"
    "sync"
)


// PipelineNode holds configuration for a single node in the pipeline.
type PipelineNode struct {
    Name        string                 `yaml:"name"`
    Type        string                 `yaml:"type"`        // e.g., importContacts, transform, exportContacts
    Concurrency int                    `yaml:"concurrency"` // number of concurrent workers
    BatchSize   int                    `yaml:"batchSize"`   // batch size for chunking
    Config      map[string]interface{} `yaml:"config"`      // node-specific config
}

// NodeConstructor is a function that knows how to create a Node
// given a name and a config map.
type NodeConstructor func(name string, config map[string]interface{}) Node

var (
    registry   = make(map[string]NodeConstructor)
    registryMu sync.RWMutex
)

// RegisterNode allows any package to register a node constructor for a given type
func RegisterNode(nodeType string, constructor NodeConstructor) {
    registryMu.Lock()
    defer registryMu.Unlock()
    registry[nodeType] = constructor
}

func Register[T Node](nodeType string, constructor func(name string, config map[string]interface{}) T) {
    RegisterNode(nodeType, func(name string, config map[string]interface{}) Node {
        return constructor(name, config)
    })
}

// GetNodeInstance uses the registry to look up a NodeConstructor
// by node type, and instantiate a node.
func GetNodeInstance(nodeCfg PipelineNode) (Node, error) {
    registryMu.RLock()
    constructor, ok := registry[nodeCfg.Type]
    registryMu.RUnlock()

    if !ok {
        return nil, fmt.Errorf("unrecognized node type: %q", nodeCfg.Type)
    }
    return constructor(nodeCfg.Name, nodeCfg.Config), nil
}
