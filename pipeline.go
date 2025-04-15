// pipeline.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"data-pipeline/nodes"
)

// RunPipeline orchestrates a single named pipeline.
func RunPipeline(ctx context.Context, pipelineName string, pipelineNodes []nodes.PipelineNode) error {
	// currentData is the evolving dataset for *this specific pipeline run*
	var currentData []interface{}
	currentData = []interface{}{} // Start fresh for each pipeline run

	log.Printf("[%s] Starting execution.", pipelineName)

	if len(pipelineNodes) == 0 {
		log.Printf("[%s] Pipeline has no nodes defined.", pipelineName)
		return nil // Or return an error if empty pipelines are invalid
	}

	for i, nodeCfg := range pipelineNodes {
		nodeLogPrefix := fmt.Sprintf("[%s | Node %d: %s]", pipelineName, i+1, nodeCfg.Name) // Add pipeline name to logs

		nodeInstance, err := nodes.GetNodeInstance(nodeCfg)
		if err != nil {
			return fmt.Errorf("%s failed to instantiate: %w", nodeLogPrefix, err)
		}

		// Pass the enhanced log prefix down to runNode
		out, err := runNode(ctx, nodeInstance, currentData, nodeCfg.Concurrency, nodeCfg.BatchSize, nodeLogPrefix)
		if err != nil {
			// Error already includes node name/prefix from runNode
			return fmt.Errorf("pipeline '%s' failed at node '%s': %w", pipelineName, nodeCfg.Name, err)
		}

		// The output of the current node is the input to the next node
		currentData = out
	}

	log.Printf("[%s] Pipeline complete. Final data length: %d", pipelineName, len(currentData))
	return nil
}

// runNode executes a single node, now accepts a logPrefix.
func runNode(ctx context.Context, node nodes.Node, items []interface{}, concurrency, batchSize int, logPrefix string) ([]interface{}, error) {
	start := time.Now()

	// --- Input handling ---
	inputItemCount := len(items)
	if batchSize < 1 {
		// Default to processing all items if batchSize isn't set or invalid
		if inputItemCount > 0 {
			batchSize = inputItemCount
		} else {
			batchSize = 1 // Avoid division by zero if items is empty
		}
	}
	if concurrency < 1 {
		concurrency = 1
	}

	// Special case: If input is empty, still call Process once for nodes that generate data (like importers)
	if inputItemCount == 0 {
		log.Printf("%s processing 0 input items.", logPrefix)
		out, err := node.Process(ctx, []interface{}{}) // Call with empty slice
		if err != nil {
			return nil, fmt.Errorf("%s processing error: %w", logPrefix, err)
		}
		log.Printf("%s finished in %v. Processed 0 items -> %d items.",
			logPrefix, time.Since(start), len(out))
		return out, nil
	}

	// --- Batching and Concurrency ---
	batches := chunkItems(items, batchSize)
	numBatches := len(batches)
	log.Printf("%s processing %d items in %d batches (batchSize=%d, concurrency=%d)...", logPrefix, inputItemCount, numBatches, batchSize, concurrency)

	var combinedOutput []interface{}
	var wg sync.WaitGroup
	var mu sync.Mutex                       // Mutex to protect combinedOutput slice
	errChan := make(chan error, numBatches) // Buffered channel for errors

	if concurrency == 1 {
		// --- Sequential Execution ---
		for i, batch := range batches {
			batchLogPrefix := fmt.Sprintf("%s Batch %d/%d", logPrefix, i+1, numBatches)
			log.Printf("%s processing %d items...", batchLogPrefix, len(batch))
			out, err := node.Process(ctx, batch)
			if err != nil {
				errChan <- fmt.Errorf("%s error: %w", batchLogPrefix, err)
				break // Stop processing further batches on error
			}
			combinedOutput = append(combinedOutput, out...)
			log.Printf("%s processed %d items -> %d output items.", batchLogPrefix, len(batch), len(out))
		}
	} else {
		// --- Concurrent Execution ---
		batchesChan := make(chan []interface{}, numBatches)
		for _, b := range batches {
			batchesChan <- b
		}
		close(batchesChan) // Close channel once all batches are sent

		worker := func(workerID int) {
			defer wg.Done()
			batchCounter := 0 // Optional: for more detailed logging per worker
			for batch := range batchesChan {
				batchCounter++
				batchLogPrefix := fmt.Sprintf("%s Worker %d Batch %d", logPrefix, workerID, batchCounter) // Log worker ID
				log.Printf("%s processing %d items...", batchLogPrefix, len(batch))
				select {
				case <-ctx.Done(): // Check if context was cancelled
					errChan <- fmt.Errorf("%s context cancelled", batchLogPrefix)
					return
				default:
					out, err := node.Process(ctx, batch) // Pass context to node
					if err != nil {
						// Send error and potentially stop processing more items
						errChan <- fmt.Errorf("%s error: %w", batchLogPrefix, err)
						// Note: Other workers might still be running. Consider context cancellation for faster shutdown.
						return // Stop this worker
					}
					mu.Lock()
					combinedOutput = append(combinedOutput, out...)
					mu.Unlock()
					log.Printf("%s processed %d items -> %d output items.", batchLogPrefix, len(batch), len(out))
				}

			}
		}

		// Start workers
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go worker(i + 1) // Pass worker ID (1-based)
		}

		// Wait for all workers to finish
		wg.Wait()
	}

	// --- Error Handling & Logging ---
	close(errChan) // Close error channel after waiting for workers
	// Check for errors
	if err := <-errChan; err != nil { // Read the first error, if any
		return nil, err
	}

	// Final log for the node
	log.Printf("%s finished in %v. Processed %d items -> %d items.",
		logPrefix, time.Since(start), inputItemCount, len(combinedOutput))

	return combinedOutput, nil
}

// chunkItems splits a slice into smaller slices (batches) of size n.
func chunkItems(items []interface{}, n int) [][]interface{} {
	if n <= 0 {
		// Treat n <= 0 as a single batch containing all items, even if empty.
		return [][]interface{}{items}
	}
	if len(items) == 0 {
		// If items is empty, return a slice containing one empty slice,
		// so the calling logic still iterates once if needed (e.g., for import nodes).
		// Or return [][]interface{}{} if nodes should never be called with empty input.
		// Let's stick to the previous behavior for now:
		return [][]interface{}{}
	}
	var chunks [][]interface{}
	for i := 0; i < len(items); i += n {
		end := i + n
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}
