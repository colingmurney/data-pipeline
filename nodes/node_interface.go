package nodes

import "context"

// Node is the interface that all ETL pipeline nodes must implement.
type Node interface {
    Name() string
    // Process receives items from the previous node and returns new items for the next node
    Process(ctx context.Context, items []interface{}) ([]interface{}, error)
}
