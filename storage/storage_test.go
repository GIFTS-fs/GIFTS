package storage

import (
	"fmt"
	"log"
	"testing"

	gifts "GIFTS"
	"GIFTS/test"
)

func TestStorage_Set(t *testing.T) {
	t.Parallel()

	// Set new data
	log.Println("TestStorage_Set: Starting test #1")
	s := NewStorage()
	data := gifts.Block("data1")
	err := s.Set("id1", &data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range data {
		test.AF(t, data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", data[i], s.blocks["id1"]))
	}

	// Overwrite old data
	log.Println("TestStorage_Set: Starting test #2")
	data = gifts.Block("new data2")
	err = s.Set("id1", &data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
	for i := range data {
		test.AF(t, data[i] == s.blocks["id1"][i], fmt.Sprintf("Expected %c but found %c", data[i], s.blocks["id1"]))
	}

	// Parallel sets
	log.Println("TestStorage_Set: Starting test #3")
	nSets := 100
	done := make(chan bool, nSets)
	for i := 0; i < nSets; i++ {
		go func(i int) {
			data := gifts.Block(fmt.Sprintf("data_%d", i))
			err := s.Set(fmt.Sprintf("id_%d", i), &data)
			test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))
			done <- true
		}(i)
	}

	for i := 0; i < nSets; i++ {
		<-done
	}

	for i := 0; i < nSets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data = gifts.Block(fmt.Sprintf("data_%d", i))

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
	log.Println("TestStorage_Get: Starting test #1")
	s := NewStorage()
	data := new(gifts.Block)
	err := s.Get("fake_id", data)
	test.AF(t, err != nil, "Storage.Set: Expected non-nil error")

	// Get empty data
	log.Println("TestStorage_Get: Starting test #2")
	s.blocks["id1"] = make([]byte, 0)
	err = s.Get("id1", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(*data) == 0, fmt.Sprintf("Expected empty data, found %q", *data))

	// Get some data
	log.Println("TestStorage_Get: Starting test #3")
	s.blocks["id2"] = []byte("some data")
	err = s.Get("id2", data)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(*data) == "some data", fmt.Sprintf("Expected \"some data\", found %q", *data))

	// Parallel get
	log.Println("TestStorage_Set: Starting test #4")
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

func TestStorage_Unset(t *testing.T) {
	t.Parallel()

	// Missing ID
	log.Println("TestStorage_Set: Starting test #1")
	s := NewStorage()
	err := s.Unset("id1")
	test.AF(t, err != nil, "Expected non-nil error")

	// Unset data
	log.Println("TestStorage_Set: Starting test #2")
	s.blocks["id1"] = []byte("data 1")
	err = s.Unset("id1")
	test.AF(t, err == nil, fmt.Sprintf("Storage.Unset failed: %v", err))
	test.AF(t, len(s.blocks["id1"]) == 0, fmt.Sprintf("Expected no data, found %q", s.blocks["id1"]))

	// Parallel unsets
	log.Println("TestStorage_Set: Starting test #3")
	nUnsets := 100
	for i := 0; i < nUnsets; i++ {
		id := fmt.Sprintf("id_%d", i)
		data := gifts.Block(fmt.Sprintf("data_%d", i))
		s.blocks[id] = data
	}

	done := make(chan bool, nUnsets)
	for i := 0; i < nUnsets; i++ {
		go func(i int) {
			err := s.Unset(fmt.Sprintf("id_%d", i))
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
