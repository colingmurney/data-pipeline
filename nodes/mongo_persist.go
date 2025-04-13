// etl-pipeline/nodes/mongo_persist.go
package nodes

import (
	"context"
	"encoding/json" // For pretty printing the documents, remove when implementing real DB logic
	// "fmt" // Uncomment when implementing real DB logic
	"log"
	// "go.mongodb.org/mongo-driver/mongo"          // Uncomment when implementing real DB logic
	// "go.mongodb.org/mongo-driver/mongo/options" // Uncomment when implementing real DB logic
)

func init() {
	Register("mongoPersist", NewMongoPersistNode)
}

// MongoPersistNodeConfig holds configuration specific to the MongoDB node.
type MongoPersistNodeConfig struct {
	URI        string `mapstructure:"uri"`        // e.g., "mongodb://localhost:27017"
	Database   string `mapstructure:"database"`   // e.g., "etl_data"
	Collection string `mapstructure:"collection"` // e.g., "imported_contacts"
}

// MongoPersistNode persists data to a MongoDB collection.
type MongoPersistNode struct {
	name   string
	config MongoPersistNodeConfig
	// client *mongo.Client // Uncomment when implementing real DB logic
}

// NewMongoPersistNode creates a new instance of the MongoDB persistence node.
func NewMongoPersistNode(name string, config map[string]interface{}) *MongoPersistNode {
	nodeConfig := MongoPersistNodeConfig{
		// Provide default values if desired
	}

	// Basic configuration extraction (consider using a library like mapstructure for robustness)
	if uri, ok := config["uri"].(string); ok {
		nodeConfig.URI = uri
	}
	if db, ok := config["database"].(string); ok {
		nodeConfig.Database = db
	}
	if coll, ok := config["collection"].(string); ok {
		nodeConfig.Collection = coll
	}

	// --- Real MongoDB Client Initialization (Keep commented out for now) ---
	/*
		clientOptions := options.Client().ApplyURI(nodeConfig.URI)
		client, err := mongo.Connect(context.TODO(), clientOptions) // Use appropriate context
		if err != nil {
			log.Printf("[%s] Warning: Failed to connect to MongoDB at %s: %v", name, nodeConfig.URI, err)
			// Depending on requirements, you might return nil or handle this error differently
			return &MongoPersistNode{
				name:   name,
				config: nodeConfig,
				client: nil, // Ensure client is nil if connection failed
			}
		}

		// Optional: Ping the primary
		if err := client.Ping(context.TODO(), nil); err != nil {
			log.Printf("[%s] Warning: Failed to ping MongoDB at %s: %v", name, nodeConfig.URI, err)
			// Handle error, potentially disconnect and set client to nil
			// client.Disconnect(context.TODO())
			// client = nil
		} else {
			log.Printf("[%s] Connected to MongoDB: %s, Database: %s, Collection: %s", name, nodeConfig.URI, nodeConfig.Database, nodeConfig.Collection)
		}
	*/
	// --- End Real MongoDB Client Initialization ---

	log.Printf("[%s] Initialized (simulation mode). Target DB: %s, Collection: %s at %s", name, nodeConfig.Database, nodeConfig.Collection, nodeConfig.URI)

	return &MongoPersistNode{
		name:   name,
		config: nodeConfig,
		// client: client, // Uncomment when implementing real DB logic
	}
}

func (n *MongoPersistNode) Name() string {
	return n.name
}

// Process simulates inserting items into MongoDB and returns the original items.
func (n *MongoPersistNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
	if len(items) == 0 {
		log.Printf("[%s] No items received, skipping persistence.", n.Name())
		return items, nil
	}

	// --- Real MongoDB Insertion Logic (Keep commented out for now) ---
	/*
		if n.client == nil {
			log.Printf("[%s] MongoDB client not initialized, skipping persistence.", n.Name())
			// Depending on requirements, might return an error here
			// return items, fmt.Errorf("MongoDB client not available")
			return items, nil // Or just pass through if simulation/non-critical
		}

		collection := n.client.Database(n.config.Database).Collection(n.config.Collection)

		// Use InsertMany for efficiency if items are compatible
		_, err := collection.InsertMany(ctx, items) // Assumes items are suitable for direct insertion
		if err != nil {
			log.Printf("[%s] Error inserting documents into %s.%s: %v", n.Name(), n.config.Database, n.config.Collection, err)
			// Handle error appropriately - retry, log, return error?
			return items, fmt.Errorf("failed to insert documents: %w", err)
		}

		log.Printf("[%s] Successfully inserted %d documents into %s.%s", n.Name(), len(items), n.config.Database, n.config.Collection)
	*/
	// --- End Real MongoDB Insertion Logic ---

	// --- Simulation Logic ---
	log.Printf("[%s] Simulating MongoDB Insert to %s.%s for %d item(s):", n.Name(), n.config.Database, n.config.Collection, len(items))
	for i, item := range items {
		// Attempt to marshal to JSON for readable output, fallback to default print
		docBytes, err := json.MarshalIndent(item, "  ", "  ") // Indent for readability
		if err != nil {
			log.Printf("[%s]   Document %d (raw): %+v", n.Name(), i+1, item)
		} else {
			log.Printf("[%s]   Document %d (JSON):\n  %s", n.Name(), i+1, string(docBytes))
		}
	}
	log.Printf("[%s] Simulation complete.", n.Name())
	// --- End Simulation Logic ---

	// Return the original items for the next node
	return items, nil
}

// Optional: Implement cleanup logic if needed (e.g., disconnect client)
// func (n *MongoPersistNode) Cleanup() {
// 	if n.client != nil {
// 		log.Printf("[%s] Disconnecting from MongoDB.", n.Name())
// 		err := n.client.Disconnect(context.TODO()) // Use appropriate context
// 		if err != nil {
// 			log.Printf("[%s] Error disconnecting from MongoDB: %v", n.Name(), err)
// 		}
// 	}
// }