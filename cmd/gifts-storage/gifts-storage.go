package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/storage"
)

var (
	configPath  = flag.String("conf", config.GIFTSDefaultConfigPath(), "config file")
	verbose     = flag.Bool("v", false, "verbose logging")
	readyAddr   = flag.String("ready", "", "ready notification address")
	iStorage    = flag.Int("s", -1, "The index of the Storage instance to start")
	statEnabled = flag.Bool("stat", false, "stat collecting enable")
	statPrefix  = flag.String("prefix", "", "stat file prefix")
)

func main() {
	flag.Parse()

	conf, err := config.LoadGet(*configPath)
	if err != nil {
		log.Fatalf("Config loading failed: %v\n", err)
	}

	if len(conf.Storages) <= 0 {
		log.Fatalf("No storage found\n")
	}

	if conf.Master == "" {
		log.Fatalf("Where is my Master: %v\n", conf)
	}

	if *iStorage < 0 || *iStorage >= len(conf.Storages) {
		if *iStorage == -1 {
			log.Fatalf("Please provide a storage index")
		}
		log.Fatalf("Invalid storage index %d", *iStorage)
	}

	addr := conf.Storages[*iStorage]
	log.Printf("Starting Storage at address %q\n", addr)
	s := storage.NewStorage()
	s.Logger.Enabled = *verbose

	// TODO: instead of awkward signal handling
	// can easily write to disk per 1 sec in non-critical path
	// but slowing the system down?
	if *statEnabled {
		s.StatEnabled = true

		sigsChan := make(chan os.Signal, 1)
		signal.Notify(sigsChan, syscall.SIGINT, syscall.SIGTERM)

		done := make(chan bool, 1)
		go s.TrapSignal(*statPrefix, sigsChan, done)
		go storage.ServeRPCBlock(s, addr, nil)
		go s.CollectStat()
		<-done
	} else {
		storage.ServeRPCBlock(s, addr, nil)
	}
}
