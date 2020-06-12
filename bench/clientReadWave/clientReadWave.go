package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/GIFTS-fs/GIFTS/bench"
)

const (
	benchName       = "clientReadWave"
	benchMsg        = "Running benchmark: a long wave for reading"
	nReplicas       = 1
	runTime         = 120
	nFileOneReaders = 5
	nFileTwoReaders = 5
)

var (
	configPath = flag.String("conf", bench.DefaultConfigPathClient, "config file")
)

func main() {
	fmt.Println(benchMsg)

	flag.Parse()
	// config, err := config.LoadGet(*configPath)
	// bench.ExitUnless(err == nil, fmt.Sprintf("Error loading config: %v", err))

	file, err := os.Create(fmt.Sprintf("results-%d.csv", time.Now().UnixNano()))
	bench.ExitUnless(err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	writer.WriteString("Time,MB/s")

	// g := generate.NewGenerate()

	// fileSize := config.GiftsBlockSize

	// TODO
	// Run nRounds rounds
	// For each round:
	//   iRound*k_create files are created (k_create is a hard-coded const)
	//   For each half of the reders:
	//     share a global random seed (hard-code), pick from (0,1,2,3,4,5)
	//     For readPeriod seconds
	//       hotspot: read the same random file to read if >1
	//       normal read: randomly pick a file to read if 1
	//       idle: do nothing if 0

}
