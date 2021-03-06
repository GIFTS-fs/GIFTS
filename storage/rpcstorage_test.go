package storage

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
	"gonum.org/v1/gonum/stat"
)

func TestRPCStorage_Set(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	ServeRPC(s, "localhost:3000")

	// Set new data
	rpcs := NewRPCStorage("localhost:3000")
	t.Log("TestRPCStorage_Set: Starting test #1")
	kv := &structure.BlockKV{ID: "id1", Data: gifts.Block("data 1")}
	err := rpcs.Set(kv)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		data, _ := s.blocks.Load("id1")
		actual := data.(gifts.Block)[i]
		test.AF(t, kv.Data[i] == actual, fmt.Sprintf("Expected %c but found %c", kv.Data[i], actual))
	}

	// Overwrite old data
	t.Log("TestRPCStorage_Set: Starting test #2")
	kv.Data = gifts.Block("new data2")
	err = rpcs.Set(kv)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		data, _ := s.blocks.Load("id1")
		actual := data.(gifts.Block)[i]
		test.AF(t, kv.Data[i] == actual, fmt.Sprintf("Expected %c but found %c", kv.Data[i], actual))
	}

	// Parallel sets
	t.Log("TestRPCStorage_Set: Starting test #3")
	nSets := 100
	done := make(chan bool, nSets)
	for i := 0; i < nSets; i++ {
		go func(i int) {
			kv := new(structure.BlockKV)
			kv.ID = fmt.Sprintf("id_%d", i)
			kv.Data = gifts.Block(fmt.Sprintf("data_%d", i))

			err := rpcs.Set(kv)
			test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
			done <- true
		}(i)
	}

	for i := 0; i < nSets; i++ {
		<-done
	}

	for i := 0; i < nSets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))

		for j := range data {
			expected := data[j]
			actualData, _ := s.blocks.Load(id)
			actual := actualData.(gifts.Block)[j]
			test.AF(t, expected == actual, fmt.Sprintf("ID %s: Expected %c but found %c", id, expected, actual))
		}
	}
}

func TestRPCStorage_Get(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	ServeRPC(s, "localhost:3001")

	// Attempt to get a missing ID
	t.Log("TestStorage_Get: Starting test #1")
	rpcs := NewRPCStorage("localhost:3001")
	data := new(gifts.Block)
	err := rpcs.Get("fake_id", data)
	test.AF(t, err != nil, "Storage.Set: Expected non-nil error")

	// Get empty data
	t.Log("TestStorage_Get: Starting test #2")
	s.blocks.Store("id1", gifts.Block(""))
	err = rpcs.Get("id1", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(*data) == 0, fmt.Sprintf("Expected empty data, found %q", *data))

	// Get some data
	t.Log("TestStorage_Get: Starting test #3")
	s.blocks.Store("id2", gifts.Block("some data"))
	err = rpcs.Get("id2", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(*data) == "some data", fmt.Sprintf("Expected \"some data\", found %q", *data))

	// Parallel get
	t.Log("TestStorage_Set: Starting test #4")
	nBlocks := 10
	for i := 0; i < nBlocks; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks.Store(id, data)
	}

	nGets := 100
	done := make(chan bool, nGets)
	for i := 0; i < nGets; i++ {
		go func(i int) {
			index := i % nBlocks
			expected := fmt.Sprintf("data_%d", index)
			id := fmt.Sprintf("id_%d", index)

			actual := new(gifts.Block)
			err := rpcs.Get(id, actual)
			test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
			test.AF(t, string(*actual) == expected, fmt.Sprintf("Expected %q, found %q", expected, *data))

			done <- true
		}(i)
	}

	for i := 0; i < nGets; i++ {
		<-done
	}
}

func TestRPCStorage_Replicate(t *testing.T) {
	t.Parallel()
	var kv structure.ReplicateKV
	var err error

	s1 := NewStorage()
	s1.blocks.Store("valid_id", gifts.Block("Hello World"))
	ServeRPC(s1, "localhost:3200")
	rs := NewRPCStorage("localhost:3200")

	s2 := NewStorage()
	ServeRPC(s2, "localhost:3201")

	// Invalid block ID
	t.Logf("TestStorage_Replicate: Starting test #1")
	kv.ID = "Invalid ID"
	kv.Dest = "localhost:3201"
	err = rs.Replicate(&kv)
	test.AF(t, err != nil, "Invalid block ID should fail")

	// Invalid destination
	t.Logf("TestStorage_Replicate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3300"
	err = rs.Replicate(&kv)
	test.AF(t, err != nil, "Invalid destination should fail")

	// Valid ID and destination
	t.Logf("TestStorage_Replicate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3201"
	err = rs.Replicate(&kv)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Replicate failed: %v", err))

	expected, _ := s1.blocks.Load("valid_id")
	actual, _ := s2.blocks.Load("valid_id")
	test.AF(t, string(expected.(gifts.Block)) == string(actual.(gifts.Block)), fmt.Sprintf("Expected %q, found %q", expected, actual))
}

func TestRPCStorage_Unset(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	ServeRPC(s, "localhost:3002")

	// Missing ID
	t.Log("TestStorage_Set: Starting test #1")
	rpcs := NewRPCStorage("localhost:3002")
	err := rpcs.Unset("id1", nil)
	test.AF(t, err != nil, "Expected non-nil error")

	// Unset data
	t.Log("TestStorage_Set: Starting test #2")
	s.blocks.Store("id1", gifts.Block("data 1"))
	err = rpcs.Unset("id1", nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
	actual, found := s.blocks.Load("id1")
	test.AF(t, !found, "Expected no data")
	test.AF(t, actual == nil, fmt.Sprintf("Expected no data, found %q", actual))

	// Parallel unset
	t.Log("TestStorage_Set: Starting test #3")
	nUnsets := 100
	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks.Store(id, data)
	}

	done := make(chan bool, nUnsets)
	for i := 0; i < nUnsets; i++ {
		go func(i int) {
			err := rpcs.Unset(fmt.Sprintf("id_%d", i), nil)
			test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
			done <- true
		}(i)
	}

	for i := 0; i < nUnsets; i++ {
		<-done
	}

	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		actual, found := s.blocks.Load(id)
		test.AF(t, !found, "Expected no data")
		test.AF(t, actual == nil, fmt.Sprintf("Expected no data, found %q", actual))
	}
}

func TestBenchmarkRPCStorage_Set(t *testing.T) {
	t.Skip()
	g := generate.NewGenerate()
	nRuns := int64(10)
	nTestsPerRun := int64(50000)

	s := NewStorage()
	ServeRPC(s, "localhost:4000")
	s.Logger.Enabled = false

	rpcs := NewRPCStorage("localhost:4000")
	rpcs.Logger.Enabled = false

	for blockSize := int64(2); blockSize <= 65536; blockSize *= 2 {
		runElapsed := int64(0)
		for i := int64(0); i < nRuns; i++ {

			testElapsed := int64(0)
			for n := int64(0); n < nTestsPerRun; n++ {
				id := fmt.Sprintf("id_%d", n)
				kv := structure.BlockKV{ID: id, Data: gifts.Block(make([]byte, blockSize))}
				g.Read(kv.Data)

				startTime := time.Now()
				rpcs.Set(&kv)
				testElapsed += time.Since(startTime).Nanoseconds()
			}
			runElapsed += (testElapsed / nTestsPerRun)
		}
		t.Logf("Block size %d: %d", blockSize, runElapsed/nRuns)
	}
}

func TestBenchmarkRPCStorage_Get(t *testing.T) {
	t.Skip()
	file, err := os.Create(fmt.Sprintf("./results-%d.csv", time.Now().UnixNano()))
	test.AF(t, err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	defer file.Close()

	// blockSize, nReaders, stat.Mean(runResults, nil), stat.StdDev(runResults, nil)
	msg := "Block Size (bytes), # of Readers, Average Throughput (MBps), STD (MBps), %"
	fmt.Println(msg)
	writer.WriteString(msg + "\n")

	g := generate.NewGenerate()
	nRuns := int64(10)
	runTime := float64(3)
	// nReaders := 50
	nBlocks := int64(1000)

	config, err := config.LoadGet("../config/config.json")
	test.AF(t, err == nil, fmt.Sprintf("Error loading config: %v", err))

	// For block size
	for blockSize := int64(1); blockSize <= 1048576; blockSize *= 2 {
		for nReaders := 20; nReaders <= 20; nReaders++ { // Create a set of blocks to read
			fmt.Printf("Starting test for block size %d in 5s\n", blockSize)
			time.Sleep(5 * time.Second)

			rpcs := NewRPCStorage(config.Storages[0])
			rpcs.Logger.Enabled = false
			ids := make([]string, nBlocks)
			for n := int64(0); n < nBlocks; n++ {
				id := fmt.Sprintf("id_%d", n)
				ids[n] = id

				kv := structure.BlockKV{ID: id, Data: gifts.Block(make([]byte, blockSize))}
				g.Read(kv.Data)
				err := rpcs.Set(&kv)
				test.AF(t, err == nil, fmt.Sprintf("RPCStorage.Set failed: %v", err))
			}

			// For nRuns
			done := make(chan float64, nReaders)
			runResults := make([]float64, 0)
			for run := int64(0); run < nRuns; run++ {
				fmt.Printf("\tRun %d\n", run)
				for reader := 0; reader < nReaders; reader++ {
					go func() {
						rs := NewRPCStorage(config.Storages[0])
						rs.Logger.Enabled = false
						data := new(gifts.Block)
						nReads := int64(0)

						startTime := time.Now()
						for time.Since(startTime).Seconds() < runTime {
							rs.Get(ids[nReads%nBlocks], data)
							nReads++
						}

						done <- float64(nReads*blockSize) / time.Since(startTime).Seconds() / 1000000
						t.Log(len(*data))
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
