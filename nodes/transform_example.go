package nodes

import (
	"context"
	"log"
	"strings"
)

func init() {
	Register("transformExample", NewTransformNode)
}

// TransformNode - Example node that transforms data in memory.
type TransformNode struct {
	name   string
	config map[string]interface{}
}

func NewTransformNode(name string, config map[string]interface{}) *TransformNode {
	return &TransformNode{name: name, config: config}
}

func (n *TransformNode) Name() string {
	return n.name
}

func (n *TransformNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
	fieldToUpper, _ := n.config["uppercaseField"].(string)
	log.Printf("[%s] Transforming items, uppercase field: %s", n.Name(), fieldToUpper)

	var output []interface{}
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			// If item is not a map, skip or handle differently
			continue
		}
		if val, found := m[fieldToUpper]; found {
			if str, ok := val.(string); ok {
				m[fieldToUpper] = strings.ToUpper(str)
			}
		}
		output = append(output, m)
	}
	return output, nil
}
