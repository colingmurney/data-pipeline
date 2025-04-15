package nodes

import (
	"context"
	"fmt"
	"log"
	"strings"
)

func init() {
	// Register this node type with the factory
	Register("aggregateExample", NewAggregateExampleNode)
}

// AggregateExampleNodeConfig holds configuration for this node.
type AggregateExampleNodeConfig struct {
	GroupByField    string `mapstructure:"groupByField"`    // Field to group by (e.g., "UserID")
	AggregationType string `mapstructure:"aggregationType"` // Type of aggregation (e.g., "count")
}

// AggregateExampleNode performs aggregation on input data.
type AggregateExampleNode struct {
	name   string
	config AggregateExampleNodeConfig
}

// NewAggregateExampleNode creates a new instance of the node.
func NewAggregateExampleNode(name string, config map[string]interface{}) *AggregateExampleNode {
	nodeConfig := AggregateExampleNodeConfig{
		// Provide defaults if necessary
		AggregationType: "count", // Default to count
	}

	// Extract config
	if groupBy, ok := config["groupByField"].(string); ok && groupBy != "" {
		nodeConfig.GroupByField = groupBy
	}
	if aggType, ok := config["aggregationType"].(string); ok && aggType != "" {
		nodeConfig.AggregationType = strings.ToLower(aggType) // Normalize to lowercase
	}

	// Validate mandatory fields
	if nodeConfig.GroupByField == "" {
		log.Printf("[%s] Warning: 'groupByField' not specified in config. Node might not function correctly.", name)
		// Depending on requirements, you might want to return an error here
	}

	log.Printf("[%s] Initialized. Grouping by '%s', Aggregation: '%s'", name, nodeConfig.GroupByField, nodeConfig.AggregationType)

	return &AggregateExampleNode{
		name:   name,
		config: nodeConfig,
	}
}

// Name returns the node's name.
func (n *AggregateExampleNode) Name() string {
	return n.name
}

// Process performs the aggregation based on the node's configuration.
func (n *AggregateExampleNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
	logPrefix := fmt.Sprintf("[%s]", n.Name())

	if n.config.GroupByField == "" {
		return nil, fmt.Errorf("%s 'groupByField' is not configured", logPrefix)
	}

	log.Printf("%s Aggregating %d items by '%s' using '%s'", logPrefix, len(items), n.config.GroupByField, n.config.AggregationType)

	if len(items) == 0 {
		log.Printf("%s No items to aggregate.", logPrefix)
		return []interface{}{}, nil
	}

	var results []interface{}
	var err error

	switch n.config.AggregationType {
	case "count":
		results, err = n.aggregateCount(logPrefix, items)
	// Add cases for other aggregation types like "sum", "average" etc.
	// case "sum":
	// 	results, err = n.aggregateSum(logPrefix, items)
	default:
		err = fmt.Errorf("%s unsupported aggregation type: '%s'", logPrefix, n.config.AggregationType)
	}

	if err != nil {
		return nil, err
	}

	log.Printf("%s Aggregation complete. Produced %d result items.", logPrefix, len(results))
	return results, nil
}

// aggregateCount performs a count aggregation.
func (n *AggregateExampleNode) aggregateCount(logPrefix string, items []interface{}) ([]interface{}, error) {
	counts := make(map[interface{}]int) // Map to store counts for each group key

	for i, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			log.Printf("%s Warning: Skipping item %d as it's not a map[string]interface{}", logPrefix, i)
			continue
		}

		groupKey, keyFound := itemMap[n.config.GroupByField]
		if !keyFound {
			log.Printf("%s Warning: Skipping item %d as groupByField '%s' not found", logPrefix, i, n.config.GroupByField)
			continue
		}

		// Group key must be comparable (string, number, bool etc.) to be used as a map key.
		// If the key could be complex (slice, map), this would need more sophisticated handling.
		counts[groupKey]++
	}

	// Convert the counts map into the output slice format
	var output []interface{}
	for key, count := range counts {
		output = append(output, map[string]interface{}{
			n.config.GroupByField: key,   // The field we grouped by and its value
			"count":               count, // The result of the count
		})
	}

	return output, nil
}

// --- Placeholder for other aggregation functions ---
// func (n *AggregateExampleNode) aggregateSum(logPrefix string, items []interface{}) ([]interface{}, error) {
// 	// Implementation for sum aggregation
//  return nil, fmt.Errorf("sum aggregation not yet implemented")
// }