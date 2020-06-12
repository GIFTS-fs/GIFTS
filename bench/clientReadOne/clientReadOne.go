package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/GIFTS-fs/GIFTS/bench"
	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
	"gonum.org/v1/gonum/stat"
)

const (
	benchName = "clientReadOne"
	benchMsg  = "Running benchmark: create one file and clients continusouly read it"
	nRuns     = int64(10)
	runTime   = float64(10)
	// nReaders = 50
)

var (
	configPath = flag.String("conf", bench.DefaultConfigPathClient "config file")
)

func main() {
	fmt.Println(benchMsg)

	flag.Parse()
	config, err := config.LoadGet(*configPath)
	bench.ExitUnless(err == nil, fmt.Sprintf("Error loading config: %v", err))

	file, err := os.Create(fmt.Sprintf("results-%d.csv", time.Now().UnixNano()))
	bench.ExitUnless(err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	msg := "Block Size (bytes), File Size (bytes), # of Clients, # of Replicas, Average Throughput (MBps), STD (MBps), %%\n"
	fmt.Printf(msg)
	writer.WriteString(msg)

	g := generate.NewGenerate()

	var blockSize int = config.GiftsBlockSize

	for fileSize := blockSize; fileSize <= blockSize*len(config.Storages); fileSize += blockSize { // For each file size
		for nReplicas := 1; nReplicas <= len(config.Storages); nReplicas++ { // For the number of replicas
			c := client.NewClient([]string{config.Master}, config)

			// Create a set of blocks to read
			fName := fmt.Sprintf("file_%d_%d_%d", blockSize, fileSize, nReplicas)
			data := make([]byte, fileSize)
			g.Read(data)
			c.Store(fName, uint(nReplicas), data)

			for nReaders := 40; nReaders <= 40; nReaders++ {

				// For nRuns
				fmt.Printf("Starting test with file size %d and %d replicas\n", fileSize, nReplicas)
				done := make(chan float64, nReaders)
				runResults := make([]float64, 0)
				for run := int64(0); run < nRuns; run++ {
					fmt.Printf("\tRun %d\n", run)
					for reader := 0; reader < nReaders; reader++ {
						go func() {
							client := client.NewClient([]string{config.Master}, config)
							nReads := 0
							// data := make([]byte, fileSize)

							startTime := time.Now()
							defer func() {
								done <- float64(nReads*fileSize) / time.Since(startTime).Seconds() / 1000000
							}()
							for time.Since(startTime).Seconds() < runTime {
								_, err = client.Read(fName)
								bench.ExitUnless(err == nil, fmt.Sprintf("Client.Read failed: %v", err))
								nReads++
							}

							// fmt.Println(len(data))
						}()
					}

					var testResults float64 = 0
					for reader := 0; reader < nReaders; reader++ {
						testResults += <-done
					}

					runResults = append(runResults, testResults)

				}

				mean := stat.Mean(runResults, nil)
				stddev := stat.StdDev(runResults, nil)
				msg := fmt.Sprintf("%d, %d, %d, %d, %f, %f, %.1f%%\n", blockSize, fileSize, nReaders, nReplicas, mean, stddev, 100*stddev/mean)
				fmt.Println(msg)
				writer.WriteString(msg)
				writer.Flush()

			}
		}
	}

}
