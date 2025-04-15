package nodes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func init() {
	// Register this node type with the factory
	Register("importAnalyticsExample", NewImportAnalyticsNode)
}

// ImportAnalyticsNodeConfig holds configuration for this node.
type ImportAnalyticsNodeConfig struct {
	SourceFile string `mapstructure:"sourceFile"` // Path to the log file
}

// ImportAnalyticsNode reads event data from a file.
type ImportAnalyticsNode struct {
	name   string
	config ImportAnalyticsNodeConfig
}

// NewImportAnalyticsNode creates a new instance of the node.
func NewImportAnalyticsNode(name string, config map[string]interface{}) *ImportAnalyticsNode {
	nodeConfig := ImportAnalyticsNodeConfig{
		SourceFile: "./data/events.log", // Default path
	}

	// Extract config using mapstructure or manually
	if sourceFile, ok := config["sourceFile"].(string); ok && sourceFile != "" {
		nodeConfig.SourceFile = sourceFile
	}

	log.Printf("[%s] Initialized. Source file: %s", name, nodeConfig.SourceFile)

	return &ImportAnalyticsNode{
		name:   name,
		config: nodeConfig,
	}
}

// Name returns the node's name.
func (n *ImportAnalyticsNode) Name() string {
	return n.name
}

// Process reads the configured file line by line, assuming JSON Lines format.
// It ignores the input 'items' as it's an import node.
func (n *ImportAnalyticsNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
	logPrefix := fmt.Sprintf("[%s]", n.Name())
	log.Printf("%s Reading events from %s", logPrefix, n.config.SourceFile)

	file, err := os.Open(n.config.SourceFile)
	if err != nil {
		return nil, fmt.Errorf("%s failed to open source file %s: %w", logPrefix, n.config.SourceFile, err)
	}
	defer file.Close()

	var importedEvents []interface{}
	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes() // Use Bytes for efficiency with json.Unmarshal

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal(line, &event); err != nil {
			log.Printf("%s Warning: Skipping line %d due to JSON parsing error: %v", logPrefix, lineNumber, err)
			continue // Skip malformed lines
		}
		importedEvents = append(importedEvents, event)

		// Check context cancellation periodically if file reading is long
		select {
		case <-ctx.Done():
			log.Printf("%s Context cancelled during file read.", logPrefix)
			return nil, ctx.Err()
		default:
			// Continue processing
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s error reading source file %s: %w", logPrefix, n.config.SourceFile, err)
	}

	log.Printf("%s Successfully imported %d events from %s", logPrefix, len(importedEvents), n.config.SourceFile)
	return importedEvents, nil
}