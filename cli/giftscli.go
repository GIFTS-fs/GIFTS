package main

import (
	"flag"
	"fmt"

	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
)

func main() {
	c, _ := config.LoadGet("../config/config.json")

	if len(c.Storages) < 1 {
		panic("You must provide at least one Storage")
	}

	if c.Master == "" {
		panic("You must provide exactly one Master")
	}

	nStorage := flag.Int("s", -1, "The index of the Storage instance to start")
	flag.Parse()

	if *nStorage != -1 {
		if *nStorage < 0 || *nStorage >= len(c.Storages) {
			panic(fmt.Sprintf("Invalid storage index %d", *nStorage))
		}

		fmt.Printf("Starting Storage at address %q\n", c.Storages[*nStorage])
		go func() {
			s := storage.NewStorage()
			s.Logger.Enabled = false
			storage.ServeRPCSync(s, c.Storages[*nStorage])
		}()
	} else {
		fmt.Printf("Starting Master at address %q\n", c.Master)
		go func() {
			m := master.NewMaster(c.Storages, c)
			m.Logger.Enabled = false
			master.ServeRPCSync(m, c.Master)
		}()
	}

	// Stop the process from exiting
	done := make(chan bool, 1)
	<-done
}
