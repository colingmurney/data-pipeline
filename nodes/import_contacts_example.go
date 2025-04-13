package nodes

import (
	"context"
	"data-pipeline/helpers"
	"fmt"
	"log"
	"time"
)

func init() {
	Register("importContactsExample", NewImportContactsNode)
}

// ImportContactsNode - Example node that “imports” data from some API
type ImportContactsNode struct {
    name   string
    config map[string]interface{}
    cache     *helpers.FileCache // Add cache field
	cacheFile string             // Add cache file path fiel    
}

func NewImportContactsNode(name string, config map[string]interface{}) *ImportContactsNode {
    var cacheFilePath string
	// Check if 'cacheFilePath' is provided in the node's config section
	if pathConfig, ok := config["cacheFilePath"].(string); ok && pathConfig != "" {
		cacheFilePath = pathConfig // Use path from config
	} else {
		// Fallback to default path if not provided or empty
		cacheFilePath = fmt.Sprintf("./cache/%s_cache.json", name)
	}
    
	cache, err := helpers.NewFileCache(cacheFilePath)
	if err != nil {
		// Log the error but potentially continue without caching
		// Or you might want to make this fatal depending on requirements
		log.Printf("Warning: Could not initialize cache for node %s: %v", name, err)
		cache = nil // Ensure cache is nil if initialization failed
	}

	return &ImportContactsNode{
		name:      name,
		config:    config,
		cache:     cache,
		cacheFile: cacheFilePath,
	}}

func (n *ImportContactsNode) Name() string {
    return n.name
}

func (n *ImportContactsNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
    endpoint, _ := n.config["endpoint"].(string)
    apiKey, _ := n.config["apiKey"].(string)
    log.Printf("[%s] Importing contacts from: %s (apiKey=%s)", n.Name(), endpoint, apiKey)


    // --- Cache Usage Example ---
	lastImportDate := "N/A"
	if n.cache != nil {
		cachedDate, found := n.cache.Get("last_import_date")
		if found {
			lastImportDate = cachedDate
		}
	}
	log.Printf("[%s] Last import date from cache (%s): %s", n.Name(), n.cacheFile, lastImportDate)
	// --- End Cache Usage Example ---

    // Example static data, in real usage you'd call an API
    imported := []interface{}{
        map[string]interface{}{"Name": "Alice", "Email": "alice@example.com"},
        map[string]interface{}{"Name": "Bob", "Email": "bob@example.com"},
        map[string]interface{}{"Name": "Charlie", "Email": "charlie@example.com"},
    }


	// --- Cache Update Example ---
	// After successful import, update the cache
	if n.cache != nil {
		currentTime := time.Now().Format(time.RFC3339) // Use a standard time format
		err := n.cache.Set("last_import_date", currentTime)
		if err != nil {
			log.Printf("[%s] Warning: Failed to set last_import_date in cache: %v", n.Name(), err)
		} else {
			log.Printf("[%s] Updated last_import_date in cache (%s) to: %s", n.Name(), n.cacheFile, currentTime)
		}
	}
	// --- End Cache Update Example ---

    return imported, nil
}
