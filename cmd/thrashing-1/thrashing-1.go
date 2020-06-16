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
	configPath = filepath.Join(".", "thrashing-1.json")
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
		procStorage, err := os.StartProcess(cmdStorage, []string{cmdStorage, "-conf", configPath, "-s", strconv.FormatInt(int64(i), 10)}, &standardProcAttr)
		if err != nil {
			log.Fatalf("Storage %v start failed: %v\n", i, err)
		}
		defer procStorage.Kill()
	}

	// ugly waiting
	time.Sleep(2 * time.Second)

	c := client.NewClient([]string{conf.Master}, conf)

	// generate data
	ge := generate.NewGenerate()
	data := make([]byte, fSize)
	ge.Read(data)

	// generate plenty of data
	plentyData := make([]byte, conf.GiftsBlockSize*(len(conf.Storages)*conf.MaglevHashingMultipler+10))
	ge.Read(plentyData)

	// create a plenty file
	c.Store("f-plenty", 3, plentyData)
}
