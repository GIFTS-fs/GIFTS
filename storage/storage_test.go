package storage

import (
	"fmt"
	"testing"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestStorage_Set(t *testing.T) {
	t.Parallel()

	// Set new data
	t.Logf("TestStorage_Set: Starting test #1")
	s := NewStorage()
	kv := &structure.BlockKV{ID: "id1", Data: []byte("data 1")}
	err := s.Set(kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		test.AF(t, kv.Data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", kv.Data[i], s.blocks["id1"]))
	}

	// Overwrite old data
	t.Logf("TestStorage_Set: Starting test #2")
	kv.Data = gifts.Block("new data2")
	err = s.Set(kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range kv.Data {
		test.AF(t, kv.Data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", kv.Data[i], s.blocks["id1"]))
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
			actual := s.blocks[id][j]
			test.AF(t, expected == actual, fmt.Sprintf("ID %s: Expected %c but found %c", id, expected, actual))
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
	s.blocks["id1"] = make([]byte, 0)
	err = s.Get("id1", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(*data) == 0, fmt.Sprintf("Expected empty data, found %q", *data))

	// Get some data
	t.Logf("TestStorage_Get: Starting test #3")
	s.blocks["id2"] = []byte("some data")
	err = s.Get("id2", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(*data) == "some data", fmt.Sprintf("Expected \"some data\", found %q", *data))

	// Parallel get
	t.Logf("TestStorage_Set: Starting test #4")
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

func TestStorage_Replicate(t *testing.T) {
	t.Parallel()
	s := NewStorage()
	s.blocks["valid_id"] = []byte("Hello World")
	var kv structure.ReplicateKV
	var err error

	rs := NewStorage()
	ServeRPC(rs, "localhost:3100")

	// Invalid block ID
	t.Logf("TestStorage_Replicate: Starting test #1")
	kv.ID = "Invalid ID"
	kv.Dest = "localhost:3100"
	err = s.Replicate(&kv, nil)
	test.AF(t, err != nil, "Invalid block ID should fail")

	// Invalid destination
	t.Logf("TestStorage_Replicate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3101"
	err = s.Replicate(&kv, nil)
	test.AF(t, err != nil, "Invalid destination should fail")

	// Valid ID and destination
	t.Logf("TestStorage_Replicate: Starting test #2")
	kv.ID = "valid_id"
	kv.Dest = "localhost:3100"
	err = s.Replicate(&kv, nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Replicate failed: %v", err))

	expected := string(s.blocks["valid_id"])
	actual := string(rs.blocks["valid_id"])
	test.AF(t, expected == actual, fmt.Sprintf("Expected %q, found %q", expected, actual))
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
	s.blocks["id1"] = []byte("data 1")
	err = s.Unset("id1", nil)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
	test.AF(t, len(s.blocks["id1"]) == 0, fmt.Sprintf("Expected no data, found %q", s.blocks["id1"]))

	// Parallel unsets
	t.Logf("TestStorage_Set: Starting test #3")
	nUnsets := 100
	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks[id] = data
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
		test.AF(t, len(s.blocks[id]) == 0, fmt.Sprintf("Expected no data, found %q", s.blocks[id]))
	}
}
