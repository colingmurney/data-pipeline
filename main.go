// main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"data-pipeline/nodes"
	"gopkg.in/yaml.v3"
)

// AppConfig is the top-level config struct for YAML parsing.
type AppConfig struct {
	Pipelines map[string][]nodes.PipelineNode `yaml:"pipelines"`
}

// loadConfig reads the config YAML file from disk.
func loadConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml config %s: %w", path, err)
	}

	if len(cfg.Pipelines) == 0 {
		return nil, fmt.Errorf("no pipelines defined in config file %s", path)
	}

	return &cfg, nil
}

func main() {
	// Define command-line flags
	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	// Use a comma-separated string for pipeline names, or potentially multiple flags
	pipelineNamesRaw := flag.String("pipelines", "", "Comma-separated names of pipelines to run (runs all if empty)")

	flag.Parse() // Parse the command-line flags

	// Load application configuration
	cfg, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Determine which pipelines to run
	var pipelinesToRun map[string][]nodes.PipelineNode
	if *pipelineNamesRaw == "" {
		log.Printf("No specific pipelines requested, running all %d pipelines.", len(cfg.Pipelines))
		pipelinesToRun = cfg.Pipelines
	} else {
		pipelinesToRun = make(map[string][]nodes.PipelineNode)
		requestedNames := strings.Split(*pipelineNamesRaw, ",")
		log.Printf("Requested pipelines: %v", requestedNames)
		for _, name := range requestedNames {
			trimmedName := strings.TrimSpace(name)
			if pipelineConfig, ok := cfg.Pipelines[trimmedName]; ok {
				pipelinesToRun[trimmedName] = pipelineConfig
			} else {
				log.Printf("Warning: Pipeline named '%s' not found in config, skipping.", trimmedName)
			}
		}
		if len(pipelinesToRun) == 0 {
			log.Fatalf("None of the requested pipelines (%s) were found in the configuration.", *pipelineNamesRaw)
		}
	}

	// Create a context for the pipelines
	ctx := context.Background()
	var runErrors []string

	// Run the selected pipelines
	log.Printf("Starting execution for %d selected pipeline(s)...", len(pipelinesToRun))
	for name, pipelineCfg := range pipelinesToRun {
		log.Printf("--- Running Pipeline: %s ---", name)
		// Pass the pipeline name and its specific configuration
		if err := RunPipeline(ctx, name, pipelineCfg); err != nil {
			errMsg := fmt.Sprintf("Pipeline '%s' failed: %v", name, err)
			log.Printf("ERROR: %s", errMsg)
			runErrors = append(runErrors, errMsg)
		} else {
			log.Printf("--- Pipeline '%s' completed successfully ---", name)
		}
		log.Println("-----------------------------------------") // Separator
	}

	// Report final status
	if len(runErrors) > 0 {
		log.Fatalf("One or more pipelines failed:\n- %s", strings.Join(runErrors, "\n- "))
	} else {
		log.Println("All selected pipelines completed successfully.")
	}
}