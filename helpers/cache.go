package helpers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// FileCache implements a simple key-value cache stored in a JSON file.
type FileCache struct {
	filePath string
	data     map[string]string
	mu       sync.RWMutex
}

// NewFileCache creates or loads a cache from the specified file path.
func NewFileCache(filePath string) (*FileCache, error) {
	cache := &FileCache{
		filePath: filePath,
		data:     make(map[string]string),
	}
	if err := cache.load(); err != nil && !os.IsNotExist(err) {
		// Return error only if it's not a "file not found" error (we can create it)
		return nil, err
	}
	return cache, nil
}

// load reads the cache data from the JSON file.
func (c *FileCache) load() error {
	c.mu.RLock()
	filePath := c.filePath // Read filePath under read lock
	c.mu.RUnlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err // Handle os.IsNotExist in the caller
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Unmarshal(data, &c.data)
}

// save writes the current cache data to the JSON file.
func (c *FileCache) save() error {
	c.mu.RLock()
	// Read data and filePath under read lock
	filePath := c.filePath
	dataCopy := make(map[string]string)
	for k, v := range c.data {
		dataCopy[k] = v
	}
	c.mu.RUnlock() // Release read lock before potentially slow I/O

	data, err := json.MarshalIndent(c.data, "", "  ")
	if err != nil {
		return err
	}

	// Get the directory part of the file path
	dir := filepath.Dir(filePath)
	// Create the directory structure if it doesn't exist
	// os.MkdirAll doesn't return an error if the path already exists.
	if err := os.MkdirAll(dir, 0755); err != nil { // Use appropriate permissions
		return err
	}

	return os.WriteFile(c.filePath, data, 0644) // Use appropriate file permissions
}

// Get retrieves a value from the cache by key.
func (c *FileCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.data[key]
	return val, ok
}

// Set adds or updates a key-value pair in the cache and saves it to the file.
func (c *FileCache) Set(key, value string) error {
	c.mu.Lock()
	c.data[key] = value
	c.mu.Unlock() // Unlock before saving to allow reads during potentially slow I/O

	// Save the updated cache to the file
	// Note: Frequent saves can be inefficient. Consider batching saves or saving on shutdown
	// for more performance-critical applications.
	return c.save()
}

// Delete removes a key from the cache and saves the change.
func (c *FileCache) Delete(key string) error {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
	return c.save()
}