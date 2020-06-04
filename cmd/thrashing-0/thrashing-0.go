package main

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
)

var (
	cmdMaster  = filepath.Join(".", "gifts-master")
	cmdStorage = filepath.Join(".", "gifts-storage")
	cmdClient  = filepath.Join(".", "gifts-client")
	configPath = filepath.Join(".", "thrashing-0.json")
)

const (
	fSize = 1024
	nRead = 10
)

func main() {
	var standardProcAttr os.ProcAttr
	standardProcAttr.Files = []*os.File{os.Stdin, os.Stdout, os.Stderr}

	procMaster, err := os.StartProcess(cmdMaster, []string{cmdMaster, "-conf", configPath, "-v"}, &standardProcAttr)
	if err != nil {
		log.Fatalf("Master start failed: %v\n", err)
	}
	defer procMaster.Kill()

	conf, _ := config.LoadGet(configPath)

	for i := 0; i < len(conf.Storages); i++ {
		procStorage, err := os.StartProcess(cmdStorage, []string{cmdStorage, "-conf", configPath, "-s", strconv.FormatInt(int64(i), 10), "-v"}, &standardProcAttr)
		if err != nil {
			log.Fatalf("Storage %v start failed: %v\n", i, err)
		}
		defer procStorage.Kill()
	}

	// ugly waiting
	time.Sleep(2 * time.Second)

	// generate data
	ge := generate.NewGenerate()
	data := make([]byte, fSize)
	ge.Read(data)

	// create 2 files
	c := client.NewClient([]string{conf.Master}, conf)
	c.Store("f1", 1, data)
	c.Store("f2", 1, data)

	// read f1 nRead times
	for i := 0; i < nRead; i++ {
		time.Sleep(1 * time.Second)
		c.Read("f1")
	}
}
