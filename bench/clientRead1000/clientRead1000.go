package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/GIFTS-fs/GIFTS/bench"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
	"gonum.org/v1/gonum/stat"
)

const (
	benchName = "clientRead1000"
	benchMsg  = "Running benchmark: create 1,000 files and clients continusouly read them"
	nRuns     = int64(10)
	runtTime  = float64(3)
)

func main() {
	fmt.Println(benchMsg)

	config, err := config.LoadGet("config.json")
	bench.ExitUnless(err == nil, fmt.Sprintf("Error loading config: %v", err))

	file, err := os.Create(fmt.Sprintf("results-%d.csv", time.Now().UnixNano()))
	bench.ExitUnless(err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// blockSize, nReaders, stat.Mean(runResults, nil), stat.StdDev(runResults, nil)
	msg := "Block Size (bytes), # of Readers, Average Throughput (MBps), STD (MBps), %"
	fmt.Println(msg)
	writer.WriteString(msg + "\n")

	g := generate.NewGenerate()

	// For block size
	for blockSize := int64(config.GiftsBlockSize); blockSize <= int64(config.GiftsBlockSize); blockSize *= 2 {
		for nReaders := 40; nReaders <= 40; nReaders++ { // Create a set of blocks to read
			c := NewClient([]string{config.Master}, config)

			// Create a set of blocks to read
			fNames := make([]string, 1000)
			for n := int64(0); n < 1000; n++ {
				fName := fmt.Sprintf("file_%d", n)
				fNames[n] = fName

				data := make([]byte, blockSize)
				g.Read(data)

				err := c.Store(fName, 1, data)
				bench.ExitUnless(err == nil, fmt.Sprintf("Client.Store failed: %v", err))
			}

			// For nRuns
			done := make(chan float64, nReaders)
			runResults := make([]float64, 0)
			for run := int64(0); run < nRuns; run++ {
				fmt.Printf("\tRun %d\n", run)
				for reader := 0; reader < nReaders; reader++ {
					go func() {
						client := NewClient([]string{config.Master}, config)
						var nReads int64 = 0
						data := make([]byte, blockSize)

						startTime := time.Now()
						for time.Since(startTime).Seconds() < runTime {
							data, err = client.Read(fNames[nReads%1000])
							nReads++
						}

						done <- float64(nReads*blockSize) / time.Since(startTime).Seconds() / 1000000
						t.Log(len(data))
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
			msg := fmt.Sprintf("%d, %d, %f, %f, %.1f%%", blockSize, nReaders, mean, stddev, 100*stddev/mean)
			fmt.Println(msg)
			writer.WriteString(msg + "\n")
			writer.Flush()
		}
	}
}
