package main

import (
	"flag"
	"fmt"
	"path/filepath"

	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
)

func main() {
	c, _ := config.LoadGet(filepath.Join("..", "config", "config.json"))

	if len(c.Storages) < 1 {
		panic("You must provide at least one Storage")
	}

	if c.Master == "" {
		panic("You must provide exactly one Master")
	}

	iStorage := flag.Int("s", -1, "The index of the Storage instance to start")
	flag.Parse()

	if *iStorage != -1 {
		if *iStorage < 0 || *iStorage >= len(c.Storages) {
			panic(fmt.Sprintf("Invalid storage index %d", *iStorage))
		}

		fmt.Printf("Starting Storage at address %q\n", c.Storages[*iStorage])
		s := storage.NewStorage()
		storage.ServeRPCBlock(s, c.Storages[*iStorage], nil)
	} else {
		fmt.Printf("Starting Master at address %q\n", c.Master)
		m := master.NewMaster(c.Storages, c)
		master.ServeRPCBlock(m, c.Master, nil)
	}
}
