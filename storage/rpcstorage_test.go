package storage

import (
	"fmt"
	"testing"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/generate"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestRPCStorage_Set(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	ServeRPC(s, "localhost:3000")

	// Set new data
	rpcs := NewRPCStorage("localhost:3000")
	t.Log("TestRPCStorage_Set: Starting test #1")
	kv := &structure.BlockKV{ID: "id1", Data: []byte("data 1")}
	err := rpcs.Set(kv)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		test.AF(t, kv.Data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", kv.Data[i], s.blocks["id1"]))
	}

	// Overwrite old data
	t.Log("TestRPCStorage_Set: Starting test #2")
	kv.Data = gifts.Block("new data2")
	err = rpcs.Set(kv)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		test.AF(t, kv.Data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", kv.Data[i], s.blocks["id1"]))
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
			actual := s.blocks[id][j]
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
	s.blocks["id1"] = make([]byte, 0)
	err = rpcs.Get("id1", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(*data) == 0, fmt.Sprintf("Expected empty data, found %q", *data))

	// Get some data
	t.Log("TestStorage_Get: Starting test #3")
	s.blocks["id2"] = []byte("some data")
	err = rpcs.Get("id2", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(*data) == "some data", fmt.Sprintf("Expected \"some data\", found %q", *data))

	// Parallel get
	t.Log("TestStorage_Set: Starting test #4")
	nBlocks := 10
	for i := 0; i < nBlocks; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks[id] = []byte(data)
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
	s.blocks["id1"] = []byte("data 1")
	err = rpcs.Unset("id1", nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
	test.AF(t, len(s.blocks["id1"]) == 0, fmt.Sprintf("Expected no data, found %q", s.blocks["id1"]))

	// Parallel unset
	t.Log("TestStorage_Set: Starting test #3")
	nUnsets := 100
	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks[id] = data
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
		test.AF(t, len(s.blocks[id]) == 0, fmt.Sprintf("Expected no data, found %q", s.blocks[id]))
	}
}

func TestBenchmarkRPCStorage_Set(t *testing.T) {
	g := generate.NewGenerate()
	nRuns := int64(10)
	nTestsPerRun := int64(50000)

	s := NewStorage()
	ServeRPC(s, "localhost:4000")
	s.logger.Enabled = false

	rpcs := NewRPCStorage("localhost:4000")
	rpcs.logger.Enabled = false

	for blockSize := int64(1); blockSize <= 65536; blockSize *= 2 {
		runElapsed := int64(0)
		for i := int64(0); i < nRuns; i++ {

			testElapsed := int64(0)
			for n := int64(0); n < nTestsPerRun; n++ {
				id := fmt.Sprintf("id_%d", n)
				kv := structure.BlockKV{ID: id, Data: make([]byte, blockSize)}
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
	g := generate.NewGenerate()
	nRuns := int64(10)
	nTestsPerRun := int64(1000)

	s := NewStorage()
	s.logger.Enabled = false

	// For block size
	for blockSize := int64(1024); blockSize <= 1024; blockSize *= 2 {

		// For number of readers
		for nReaders := 50; nReaders <= 100; nReaders++ {
			for n := int64(0); n < nTestsPerRun; n++ {
				id := fmt.Sprintf("id_%d", n)
				kv := structure.BlockKV{ID: id, Data: make([]byte, blockSize)}
				g.Read(kv.Data)
				s.Set(&kv, nil)
			}

			// For nRuns
			done := make(chan float32, nReaders)
			runThroughput := float32(0)
			for run := int64(0); run < nRuns; run++ {

				for reader := 0; reader < nReaders; reader++ {
					go func() {
						// For nTestsPerRun
						testElapsed := int64(0)
						data := new(gifts.Block)
						rpcs := NewRPCStorage("localhost:4000")
						rpcs.logger.Enabled = false
						for testRun := int64(0); testRun < nTestsPerRun; testRun++ {
							id := fmt.Sprintf("id_%d", testRun)

							startTime := time.Now()
							rpcs.Get(id, data)
							testElapsed += time.Since(startTime).Nanoseconds()
						}

						done <- 1000 * float32(blockSize) / float32(testElapsed/nTestsPerRun)
					}()
				}

				for reader := 0; reader < nReaders; reader++ {
					runThroughput += <-done
				}

			}

			t.Logf("Block size (%d), readers(%d): %.2f", blockSize, nReaders, runThroughput/float32(nRuns))
		}
	}
}
