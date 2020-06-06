package bench

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
	"gonum.org/v1/gonum/stat"
)

func TestBenchmarkMaster_Lookup(t *testing.T) {
	file, err := os.Create(fmt.Sprintf("./results-%d.csv", time.Now().UnixNano()))
	test.AF(t, err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	defer file.Close()

	msg := "# of Readers, Average Throughput (requests/s), STD (requests/s), %"
	t.Log(msg)
	writer.WriteString(msg + "\n")

	var nRuns int = 2
	var runTime float64 = 2

	// Load config
	config, err := config.LoadGet("../config/config.json")
	test.AF(t, err == nil, fmt.Sprintf("Error loading config: %v", err))

	// Create files
	c := client.NewClient([]string{config.Master}, config)
	c.Logger.Enabled = false
	fNames := make([]string, 1000)
	for n := int64(0); n < 1000; n++ {
		fName := fmt.Sprintf("file_%d", n)
		fNames[n] = fName

		req := structure.FileCreateReq{
			Fname:   fName,
			Fsize:   config.GiftsBlockSize,
			Rfactor: 1,
		}
		a := make([]structure.BlockAssign, 0)
		m.Create(&req, &a)
	}

	for nReaders := 1; nReaders <= 100; nReaders++ {

		// For nRuns
		runResults := make([]float64, 0)
		done := make(chan float64, nReaders)
		for run := 0; run < nRuns; run++ {
			for reader := 0; reader < nReaders; reader++ {
				go func() {
					m := master.NewConn(config.Master)
					var nReads int = 0

					defer func() { done <- float64(nReads) }()
					startTime := time.Now()
					for time.Since(startTime).Seconds() < runTime {
						m.Lookup(fNames[nReads%1000])
						nReads++
					}
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
		msg := fmt.Sprintf("%d, %f, %f, %.1f%%", nReaders, mean, stddev, 100*stddev/mean)
		t.Log(msg)
		writer.WriteString(msg + "\n")
		writer.Flush()
	}
}
