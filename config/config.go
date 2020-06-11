package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GIFTS-fs/GIFTS/policy"
)

// GIFTSDefaultConfigPath in current directory
func GIFTSDefaultConfigPath() string {
	return filepath.Join("config.json")
}

var (
	config     *Config
	configOnce sync.Once
)

// Config holds all configuration data for the system
type Config struct {
	MasterRebalanceIntervalSec  time.Duration
	TrafficDecayCounterHalfLife float64
	GiftsBlockSize              int
	Storages                    []string
	Master                      string
	DynamicReplicationEnabled   bool
	MaglevHashingMultipler      int
	BlockPlacementPolicy        policy.BlockPlacementPolicy
	ReplicaPlacementPolicy      policy.ReplicaPlacementPolicy
}

// Load the system configuration from the config file
func Load(path string) error {
	file, _ := os.Open(path)
	defer file.Close()

	newConfig := new(Config)

	err := json.NewDecoder(file).Decode(&newConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load(%q): %v\n", path, err)
		return err
	}

	config = newConfig
	return nil
}

// Get a reference to the system configuration
func Get() *Config {
	if config == nil {
		panic("No config loaded")
	}
	return config
}

// LoadGet loads if not already loaded, it's concurrency safe and ensures singleton
func LoadGet(path string) (*Config, error) {
	var err error
	if config == nil {
		configOnce.Do(func() { err = Load(path) })
	}
	return config, err
}
