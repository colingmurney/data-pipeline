package nodes

import (
    "context"
    "log"
)

func init() {
	Register("importContactsExample", NewImportContactsNode)
}

// ImportContactsNode - Example node that “imports” data from some API
type ImportContactsNode struct {
    name   string
    config map[string]interface{}
}

func NewImportContactsNode(name string, config map[string]interface{}) *ImportContactsNode {
    return &ImportContactsNode{name: name, config: config}
}

func (n *ImportContactsNode) Name() string {
    return n.name
}

func (n *ImportContactsNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
    endpoint, _ := n.config["endpoint"].(string)
    apiKey, _ := n.config["apiKey"].(string)
    log.Printf("[%s] Importing contacts from: %s (apiKey=%s)", n.Name(), endpoint, apiKey)

    // Example static data, in real usage you'd call an API
    imported := []interface{}{
        map[string]interface{}{"Name": "Alice", "Email": "alice@example.com"},
        map[string]interface{}{"Name": "Bob", "Email": "bob@example.com"},
        map[string]interface{}{"Name": "Charlie", "Email": "charlie@example.com"},
    }
    return imported, nil
}
