package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
	"gonum.org/v1/gonum/stat"
)

func exitUnless(cond bool, msg string) {
	if !cond {
		log.Fatalf(msg)
	}
}

const (
	benchName = "client_onefile"
)

func main() {
	file, err := os.Create(fmt.Sprintf("%v-results-%d.csv", benchName, time.Now().UnixNano()))
	exitUnless(err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	defer file.Close()

	msg := "Block Size (bytes), File Size (bytes), # of Clients, # of Replicas, Average Throughput (MBps), STD (MBps), %"
	log.Printf(msg)
	writer.WriteString(msg + "\n")

	config, err := config.LoadGet(filepath.Join("..", "config", "config.json"))
	exitUnless(err == nil, fmt.Sprintf("Error loading config: %v", err))

	g := generate.NewGenerate()
	var nRuns int64 = 5
	var runTime float64 = 5
	// var nReaders int = 50
	var blockSize int = config.GiftsBlockSize

	for fileSize := blockSize; fileSize <= blockSize*len(config.Storages); fileSize += blockSize { // For each file size
		for nReplicas := 1; nReplicas <= len(config.Storages); nReplicas++ { // For the number of replicas

			// Create a set of blocks to read
			c := client.NewClient([]string{config.Master}, config)
			fName := fmt.Sprintf("file_%d_%d_%d", blockSize, fileSize, nReplicas)
			data := make([]byte, fileSize)
			g.Read(data)
			c.Store(fName, uint(nReplicas), data)

			for nReaders := 1500; nReaders <= 1500; nReaders++ {

				// For nRuns
				done := make(chan float64, nReaders)
				runResults := make([]float64, 0)
				for run := int64(0); run < nRuns; run++ {
					for reader := 0; reader < nReaders; reader++ {
						go func() {
							client := client.NewClient([]string{config.Master}, config)
							nReads := 0
							data := make([]byte, fileSize)

							startTime := time.Now()
							defer func() {
								done <- float64(nReads*fileSize) / time.Since(startTime).Seconds() / 1000000
							}()
							for time.Since(startTime).Seconds() < runTime {
								data, err = client.Read(fName)
								exitUnless(err == nil, fmt.Sprintf("Client.Read failed: %v", err))
								nReads++
							}

							log.Println(len(data))
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
				msg := fmt.Sprintf("%d, %d, %d, %d, %f, %f, %.1f%%", blockSize, fileSize, nReaders, nReplicas, mean, stddev, 100*stddev/mean)
				log.Printf(msg)
				writer.WriteString(msg + "\n")
				writer.Flush()

			}
		}
	}
}
