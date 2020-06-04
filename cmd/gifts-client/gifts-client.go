package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
)

const (
	// ActionRead a file
	ActionRead = "read"
	// ActionStore a file
	ActionStore = "store"
)

var (
	configPath = flag.String("conf", config.GIFTSDefaultConfigPath(), "config file")
	verbose    = flag.Bool("v", false, "verbose logging")
	readyAddr  = flag.String("ready", "", "ready notification address")
	action     = flag.String("action", "", "action: read, store")
	filePath   = flag.String("path", "", "File path, for Store")
	fileName   = flag.String("file", "", "File name")
	rfactor    = flag.Uint("rfactor", 0, "replication factor")
)

func main() {
	flag.Parse()

	conf, err := config.LoadGet(*configPath)
	if err != nil {
		log.Fatalf("Config loading failed: %v\n", err)
	}

	if conf.Master == "" {
		log.Fatalf("Where is my Master: %v\n", conf)
	}

	c := client.NewClient([]string{conf.Master}, conf)
	c.Logger.Enabled = *verbose

	if *action == ActionRead {
		log.Printf("Reading: %q\n", *fileName)
		data, err := c.Read(*fileName)
		if err != nil {
			log.Fatalf("Read failed: %v\n", err)
		}
		fmt.Println(data)
		fmt.Println(string(data))
	} else if *action == ActionStore {
		data, err := ioutil.ReadFile(*filePath)
		if err != nil {
			log.Fatalf("ReadFile (%q) failed: %v\n", *filePath, err)
		}
		err = c.Store(*fileName, *rfactor, data)
		if err != nil {
			log.Fatalf("Store (%q) failed: %v\n", *fileName, err)
		}
	} else {
		log.Printf("No action specified. Exiting...\n")
	}

}
