package main

import (
	"flag"
	"log"

	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
)

var (
	configPath = flag.String("conf", config.GIFTSDefaultConfigPath(), "config file")
	verbose    = flag.Bool("v", false, "verbose logging")
	readyAddr  = flag.String("ready", "", "ready notification address")
)

func main() {
	flag.Parse()

	conf, err := config.LoadGet(*configPath)
	if err != nil {
		log.Fatalf("Config loading failed: %v\n", err)
	}

	if len(conf.Storages) <= 0 {
		log.Printf("Warning: no storage found\n")
	}

	if conf.Master == "" {
		log.Fatalf("Where is my Master: %v\n", conf)
	}

	log.Printf("Starting Master at address %q\n", conf.Master)
	m := master.NewMaster(conf.Storages, conf)
	m.Logger.Enabled = *verbose
	master.ServeRPCBlock(m, conf.Master, nil)
}
