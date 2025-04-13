package nodes

import (
	"context"
	"fmt"
	"log"
)

func init() {
	Register("exportContactsExample", NewExportContactsNode)
}

// ExportContactsNode - Example node that exports data to some destination
type ExportContactsNode struct {
	name   string
	config map[string]interface{}
}

func NewExportContactsNode(name string, config map[string]interface{}) *ExportContactsNode {
	return &ExportContactsNode{name: name, config: config}
}

func (n *ExportContactsNode) Name() string {
	return n.name
}

func (n *ExportContactsNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
	endpoint, _ := n.config["endpoint"].(string)
	apiKey, _ := n.config["apiKey"].(string)
	log.Printf("[%s] Exporting %d items to: %s (apiKey=%s)", n.Name(), len(items), endpoint, apiKey)

	// In a real scenario, you'd make an API call or DB write here.
	// For demo, just print them:
	for _, item := range items {
		fmt.Printf("[%s] Export item: %+v\n", n.Name(), item)
	}

	// Return items in case further nodes still want them. (Often the last node doesn’t need to return anything.)
	return items, nil
}
