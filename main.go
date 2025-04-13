package main

import (
    "context"
    "log"
    "os"
    "gopkg.in/yaml.v3"
    "etl-pipeline/nodes"
)

// PipelineConfig is the top-level config struct for YAML parsing.
type PipelineConfig struct {
    Pipeline []nodes.PipelineNode `yaml:"pipeline"`
}

// loadConfig reads the config YAML file from disk.
func loadConfig(path string) (*PipelineConfig, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }

    var cfg PipelineConfig
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        return nil, err
    }
    return &cfg, nil
}

func main() {
    // Load pipeline configuration from config.yaml
    cfg, err := loadConfig("config.yaml")
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    // Create a context for the pipeline
    ctx := context.Background()

    // Run the pipeline
    if err := RunPipeline(ctx, cfg); err != nil {
        log.Fatalf("Pipeline run failed: %v", err)
    }
}

