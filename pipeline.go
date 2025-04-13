package main

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    "data-pipeline/nodes"
)

// RunPipeline orchestrates the entire pipeline in linear order.
func RunPipeline(ctx context.Context, cfg *PipelineConfig) error {
    // currentData is the evolving dataset as we move from node to node
    var currentData []interface{}

    // In many ETL cases, the first node might "import" data, so we start with an empty slice.
    currentData = []interface{}{}

    for _, nodeCfg := range cfg.Pipeline {
        nodeInstance, err := nodes.GetNodeInstance(nodeCfg)
        if err != nil {
            return fmt.Errorf("failed to instantiate node %q: %v", nodeCfg.Name, err)
        }

        out, err := runNode(ctx, nodeInstance, currentData, nodeCfg.Concurrency, nodeCfg.BatchSize)
        if err != nil {
            return fmt.Errorf("pipeline error in node %q: %v", nodeCfg.Name, err)
        }

        // The output of the current node is the input to the next node
        currentData = out
    }

    // Final node data is here if needed
    log.Printf("Pipeline complete. Final data length: %d", len(currentData))
    return nil
}

// runNode executes a single node in the pipeline, optionally in concurrent batches.
func runNode(ctx context.Context, node nodes.Node, items []interface{}, concurrency, batchSize int) ([]interface{}, error) {
    start := time.Now()

    if concurrency < 1 {
        concurrency = 1
    }
    if batchSize < 1 {
        batchSize = len(items)
    }

    // Create chunked batches
    batches := chunkItems(items, batchSize)

    if concurrency == 1 {
        // No concurrency: process each batch sequentially
        var combinedOutput []interface{}
        for _, batch := range batches {
            out, err := node.Process(ctx, batch)
            if err != nil {
                return nil, fmt.Errorf("node %q processing error: %v", node.Name(), err)
            }
            combinedOutput = append(combinedOutput, out...)
        }

        log.Printf("Node %q finished in %v. Processed %d items -> %d items.",
            node.Name(), time.Since(start), len(items), len(combinedOutput))
        return combinedOutput, nil
    }

    // Concurrent execution
    var mu sync.Mutex
    var combinedOutput []interface{}
    var wg sync.WaitGroup
    errChan := make(chan error, len(batches))

    // Channel to feed batches to goroutines
    batchesChan := make(chan []interface{}, len(batches))
    for _, b := range batches {
        batchesChan <- b
    }
    close(batchesChan)

    worker := func() {
        defer wg.Done()
        for batch := range batchesChan {
            out, err := node.Process(ctx, batch)
            if err != nil {
                errChan <- fmt.Errorf("node %q processing error: %v", node.Name(), err)
                return
            }
            mu.Lock()
            combinedOutput = append(combinedOutput, out...)
            mu.Unlock()
        }
    }

    // Start the worker goroutines
    for i := 0; i < concurrency; i++ {
        wg.Add(1)
        go worker()
    }

    // Wait for workers
    wg.Wait()
    close(errChan)

    if len(errChan) > 0 {
        return nil, <-errChan
    }

    log.Printf("Node %q finished in %v. Processed %d items -> %d items (concurrent).",
        node.Name(), time.Since(start), len(items), len(combinedOutput))
    return combinedOutput, nil
}

// chunkItems splits a slice into smaller slices (batches) of size n.
func chunkItems(items []interface{}, n int) [][]interface{} {
    if n <= 0 || len(items) == 0 {
        return [][]interface{}{items}
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
