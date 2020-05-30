package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config holds all configuration data for the system
type Config struct {
	GiftsBlockSize int
}

var config *Config

// Load loads the system configuration from the config file
func Load(path string) {
	file, _ := os.Open(path)
	defer file.Close()

	decoder := json.NewDecoder(file)
	config = new(Config)
	err := decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}
}

// Get returns a reference to the system configuration
// Note that it assumes the configuration has been loaded
func Get() *Config {
	return config
}
