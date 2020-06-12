package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/GIFTS-fs/GIFTS/bench"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
)

const (
	benchName       = "clientReadTwo"
	benchMsg        = "Running benchmark: a small wave for reading"
	nReplicas       = 1
	runTime         = 120
	nFileOneReaders = 5
	nFileTwoReaders = 5
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

	writer.WriteString("Time,MB/s")

	g := generate.NewGenerate()

	fileSize := config.GiftsBlockSize

	// Create two files
	fmt.Println("Creating files")
	for i := 0; i < 2; i++ {
		c := NewClient([]string{config.Master}, config)
		fName := fmt.Sprintf("file_%d", i)

		data := make([]byte, fileSize)
		g.Read(data)
		c.Store(fName, uint(nReplicas), data)
	}

	done := make(chan []int, nFileOneReaders+nFileTwoReaders)

	// Create 5 readers for file one (read every 10ms)
	fmt.Println("Creating readers for file one")
	for i := 0; i < nFileOneReaders; i++ {
		fName := "file_0"

		go func() {
			client := NewClient([]string{config.Master}, config)
			data := make([]byte, fileSize)

			nReads := 0
			readArr := make([]int, 0, runTime)
			timer := 1.0

			defer func() { done <- readArr }()
			for startTime := time.Now(); time.Since(startTime).Hours() < 2; {
				data, err = client.Read(fName)
				bench.ExitUnless(err == nil, fmt.Sprintf("Client.Read failed: %v", err))
				nReads++

				if time.Since(startTime).Seconds() > timer {
					readArr = append(readArr, nReads)
					nReads = 0
					timer++
				}

				if timer < 60 {
					time.Sleep(100 * time.Millisecond)
				}
			}

			// fmt.Println(len(data))
		}()
	}

	// Create 35 readers for file two
	fmt.Println("Creating readers for file two")
	for i := 0; i < nFileTwoReaders; i++ {
		fName := "file_1"

		go func() {
			client := NewClient([]string{config.Master}, config)
			data := make([]byte, fileSize)

			nReads := 0
			readArr := make([]int, 0, runTime)
			timer := 1.0

			defer func() { done <- readArr }()
			for startTime := time.Now(); time.Since(startTime).Seconds() < float64(runTime); {
				data, err = client.Read(fName)
				bench.ExitUnless(err == nil, fmt.Sprintf("Client.Read failed: %v", err))
				nReads++

				if time.Since(startTime).Seconds() > timer {
					readArr = append(readArr, nReads)
					nReads = 0
					timer++
				}

				if timer >= 60 {
					time.Sleep(100 * time.Millisecond)
				}
			}

			// fmt.Println(len(data))
		}()
	}

	// Wait for results
	results := make([]int, runTime+5)
	for i := 0; i < nFileOneReaders+nFileTwoReaders; i++ {
		clientResult := <-done

		for i := range clientResult {
			results[i] += clientResult[i]
		}
	}

	for i := range results {
		writer.WriteString(fmt.Sprintf("%d,%d\n", i, results[i]))
	}
}
