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

func TestStorage_Set(t *testing.T) {
	t.Parallel()

	// Set new data
	t.Logf("TestStorage_Set: Starting test #1")
	s := NewStorage()
	kv := &structure.BlockKV{ID: "id1", Data: gifts.Block("data 1")}
	err := s.Set(kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		actualData, _ := s.blocks.Load("id1")
		actual := actualData.(gifts.Block)[i]
		test.AF(t, kv.Data[i] == actual, fmt.Sprintf("Expected %c but found %c", kv.Data[i], actual))
	}

	// Overwrite old data
	t.Logf("TestStorage_Set: Starting test #2")
	kv.Data = gifts.Block("new data2")
	err = s.Set(kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		actualData, _ := s.blocks.Load("id1")
		actual := actualData.(gifts.Block)[i]
		test.AF(t, kv.Data[i] == actual, fmt.Sprintf("Expected %c but found %c", kv.Data[i], actual))
	}

	// Parallel sets
	t.Logf("TestStorage_Set: Starting test #3")
	nSets := 100
	done := make(chan bool, nSets)
	for i := 0; i < nSets; i++ {
		go func(i int) {
			kv := new(structure.BlockKV)
			kv.ID = fmt.Sprintf("id_%d", i)
			kv.Data = gifts.Block(fmt.Sprintf("data_%d", i))

			err := s.Set(kv, nil)
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
			actual, _ := s.blocks.Load(id)
			test.AF(t, expected == actual.(gifts.Block)[j], fmt.Sprintf("ID %s: Expected %c but found %c", id, expected, actual))
		}
	}
}

func TestStorage_Get(t *testing.T) {
	t.Parallel()

	// Attempt to get a missing ID
	t.Logf("TestStorage_Get: Starting test #1")
	s := NewStorage()
	data := new(gifts.Block)
	err := s.Get("fake_id", data)
	test.AF(t, err != nil, "Storage.Set: Expected non-nil error")

	// Get empty data
	t.Logf("TestStorage_Get: Starting test #2")
	s.blocks.Store("id1", gifts.Block(""))
	err = s.Get("id1", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(*data) == 0, fmt.Sprintf("Expected empty data, found %q", *data))

	// Get some data
	t.Logf("TestStorage_Get: Starting test #3")
	s.blocks.Store("id2", gifts.Block("some data"))
	err = s.Get("id2", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(*data) == "some data", fmt.Sprintf("Expected \"some data\", found %q", *data))

	// Parallel get
	t.Logf("TestStorage_Set: Starting test #4")
	nBlocks := 10
	for i := 0; i < nBlocks; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks.Store(id, gifts.Block(data))
	}

	nGets := 100
	done := make(chan bool, nGets)
	for i := 0; i < nGets; i++ {
		go func(i int) {
			index := i % nBlocks
			expected := fmt.Sprintf("data_%d", index)
			id := fmt.Sprintf("id_%d", index)

			actual := new(gifts.Block)
			err := s.Get(id, actual)
			test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
			test.AF(t, string(*actual) == expected, fmt.Sprintf("Expected %q, found %q", expected, *data))

			done <- true
		}(i)
	}

	for i := 0; i < nGets; i++ {
		<-done
	}
}

func TestStorage_Migrate(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	s.blocks.Store("valid_id", gifts.Block("Hello World"))
	var kv structure.MigrateKV
	var err error

	rs := NewStorage()
	ServeRPC(rs, "localhost:3100")

	// Invalid block ID
	t.Logf("TestStorage_Migrate: Starting test #1")
	kv.ID = "Invalid ID"
	kv.Dest = "localhost:3100"
	err = s.Migrate(&kv, nil)
	test.AF(t, err != nil, "Invalid block ID should fail during migrate")

	// Invalid destination
	t.Logf("TestStorage_Migrate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3101"
	err = s.Migrate(&kv, nil)
	test.AF(t, err != nil, "Invalid destination should fail during migrate")

	// Valid ID and destination
	t.Logf("TestStorage_Migrate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3100"
	err = s.Migrate(&kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Migrate failed: %v", err))

	expected, _ := s.blocks.Load("valid_id")
	actual, _ := rs.blocks.Load("valid_id")
	test.AF(t, string(expected.(gifts.Block)) == string(actual.(gifts.Block)), fmt.Sprintf("Expected %q, found %q", expected, actual))
}

func TestStorage_Unset(t *testing.T) {
	t.Parallel()

	// Missing ID
	t.Logf("TestStorage_Set: Starting test #1")
	s := NewStorage()
	err := s.Unset("id1", nil)
	test.AF(t, err != nil, "Expected non-nil error")

	// Unset data
	t.Logf("TestStorage_Set: Starting test #2")
	s.blocks.Store("id1", gifts.Block("data 1"))
	err = s.Unset("id1", nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
	actual, found := s.blocks.Load("id1")
	test.AF(t, !found, "Expected no data")
	test.AF(t, actual == nil, fmt.Sprintf("Expected no data, found %q", actual))

	// Parallel unsets
	t.Logf("TestStorage_Set: Starting test #3")
	nUnsets := 100
	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks.Store(id, data)
	}

	done := make(chan bool, nUnsets)
	for i := 0; i < nUnsets; i++ {
		go func(i int) {
			err := s.Unset(fmt.Sprintf("id_%d", i), nil)
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

func TestBenchmarkStorage_Set(t *testing.T) {
	t.Skip()
	g := generate.NewGenerate()
	nRuns := int64(20)
	nTestsPerRun := int64(100000)

	for blockSize := int64(1); blockSize <= 32768; blockSize *= 2 {
		runElapsed := int64(0)
		for i := int64(0); i < nRuns; i++ {
			s := NewStorage()
			s.logger.Enabled = false

			testElapsed := int64(0)
			for n := int64(0); n < nTestsPerRun; n++ {
				id := fmt.Sprintf("id_%d", n)
				kv := structure.BlockKV{ID: id, Data: gifts.Block(make([]byte, blockSize))}
				g.Read(kv.Data)

				startTime := time.Now()
				s.Set(&kv, nil)
				testElapsed += time.Since(startTime).Nanoseconds()
			}
			runElapsed += (testElapsed / nTestsPerRun)
		}
		t.Logf("Block size %d: %d", blockSize, runElapsed/nRuns)
	}
}

func TestBenchmarkStorage_Get(t *testing.T) {
	t.Skip()
	g := generate.NewGenerate()
	nRuns := int64(10)
	nTestsPerRun := int64(1000)

	// For block size
	for blockSize := int64(1); blockSize <= 4096; blockSize *= 2 {

		// For number of readers
		for nReaders := 1; nReaders <= 100; nReaders++ {
			s := NewStorage()
			s.logger.Enabled = false
			for n := int64(0); n < nTestsPerRun; n++ {
				id := fmt.Sprintf("id_%d", n)
				kv := structure.BlockKV{ID: id, Data: gifts.Block(make([]byte, blockSize))}
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
						for testRun := int64(0); testRun < nTestsPerRun; testRun++ {
							id := fmt.Sprintf("id_%d", testRun)

							startTime := time.Now()
							s.Get(id, data)
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
