package client

import (
	"fmt"
	"log"
	"strings"
	"testing"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestClient_Store(t *testing.T) {
	t.Parallel()

	c := NewClient([]string{"master"})

	addr1 := "localhost:3003"
	addr2 := "localhost:3004"
	s1 := storage.NewStorage()
	s2 := storage.NewStorage()
	storage.ServeRPC(s1, addr1)
	storage.ServeRPC(s2, addr2)

	var data []byte

	// Empty file  name
	log.Println("TestClient_Store: Starting test #1")
	data = []byte("")
	err := c.Store("", 1, &data)
	test.AF(t, err != nil, "Expected non-nil error")

	// rfactor is 0
	log.Println("TestClient_Store: Starting test #2")
	data = []byte("")
	err = c.Store("filename", 0, &data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call but Master returns incorrect number of blocks
	log.Println("TestClient_Store: Starting test #3")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: "ID", Replicas: []string{"r1"}}
		return []structure.BlockAssign{block, block}, nil
	}
	data = []byte("Hello World")
	err = c.Store("filename_1", 1, &data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Master failure
	log.Println("TestClient_Store: Starting test #4")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		return nil, fmt.Errorf("Master error")
	}
	data = []byte("Hello World")
	err = c.Store("filename_1", 1, &data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call with no data
	log.Println("TestClient_Store: Starting test #5")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		return []structure.BlockAssign{}, nil
	}
	data = []byte("")
	err = c.Store("filename_1", 1, &data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	// Valid call with less than one block of data and one replica
	log.Println("TestClient_Store: Starting test #6")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: fname, Replicas: []string{addr1}}
		return []structure.BlockAssign{block}, nil
	}

	expected := "Hello World"
	data = []byte(expected)
	err = c.Store("filename_1", 1, &data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	ret := gifts.Block{}
	err = s1.Get("filename_1", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(ret) == expected, fmt.Sprintf("Expected %q but found %q", expected, ret))

	// Valid call with more than one block of data and one replica
	log.Println("TestClient_Store: Starting test #7")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block1 := structure.BlockAssign{BlockID: fname + "_1", Replicas: []string{addr1}}
		block2 := structure.BlockAssign{BlockID: fname + "_2", Replicas: []string{addr1}}
		return []structure.BlockAssign{block1, block2}, nil
	}

	expected = strings.Repeat("test string", 1+(gifts.GiftsBlockSize/len("test string")))
	data = []byte(expected)
	err = c.Store("filename_2", 1, &data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	err = s1.Get("filename_2_1", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(ret) == gifts.GiftsBlockSize, fmt.Sprintf("Expected %d bytes but found %d", gifts.GiftsBlockSize, len(ret)))
	test.AF(t, string(ret) == expected[:gifts.GiftsBlockSize], fmt.Sprintf("Expected %q but found %q", expected, ret))

	err = s1.Get("filename_2_2", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(ret) == expected[gifts.GiftsBlockSize:], fmt.Sprintf("Expected %q but found %q", expected, ret))

	// Valid call with more than one block of data and more than one replica
	log.Println("TestClient_Store: Starting test #8")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block1 := structure.BlockAssign{BlockID: fname + "_1", Replicas: []string{addr1, addr2}}
		block2 := structure.BlockAssign{BlockID: fname + "_2", Replicas: []string{addr1, addr2}}
		return []structure.BlockAssign{block1, block2}, nil
	}

	expected = strings.Repeat("test string 2", 1+(gifts.GiftsBlockSize/len("test string")))
	data = []byte(expected)
	err = c.Store("filename_3", 1, &data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	for _, s := range []*storage.Storage{s1, s2} {
		err = s.Get("filename_3_1", &ret)
		test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
		test.AF(t, len(ret) == gifts.GiftsBlockSize, fmt.Sprintf("Expected %d bytes but found %d", gifts.GiftsBlockSize, len(ret)))
		test.AF(t, string(ret) == expected[:gifts.GiftsBlockSize], fmt.Sprintf("Expected %q but found %q", expected, ret))

		err = s.Get("filename_3_2", &ret)
		test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
		test.AF(t, string(ret) == expected[gifts.GiftsBlockSize:], fmt.Sprintf("Expected %q but found %q", expected, ret))
	}
}

func TestClient_Read(t *testing.T) {
	t.Parallel()

	c := NewClient([]string{"master"})

	addr1 := "localhost:3005"
	addr2 := "localhost:3006"
	s1 := storage.NewStorage()
	s2 := storage.NewStorage()
	storage.ServeRPC(s1, addr1)
	storage.ServeRPC(s2, addr2)

	var data, ret []byte

	// File does not exist
	log.Println("TestClient_Read: Starting test #1")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		return nil, fmt.Errorf("%q does not exist", fname)
	}
	err := c.Read("Invalid file", &ret)
	test.AF(t, err != nil, "Expected non-nil error")

	// Master fails
	log.Println("TestClient_Read: Starting test #2")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		return nil, fmt.Errorf("Master failed")
	}
	err = c.Read("filename", &ret)
	test.AF(t, err != nil, "Expected non-nil error")

	// Master returns incorrect number of assignments
	log.Println("TestClient_Read: Starting test #3")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		ret := structure.FileBlocks{Fsize: gifts.GiftsBlockSize * 2, Assignments: []structure.BlockAssign{}}
		return &ret, nil
	}
	err = c.Read("filename", &ret)
	test.AF(t, err != nil, "Expected non-nil error")

	// Master returns incorrect number of Storage nodes for each block
	log.Println("TestClient_Read: Starting test #4")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "id1", Replicas: []string{"r1", "r2"}}
		ret := structure.FileBlocks{Fsize: 1, Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}
	err = c.Read("filename", &ret)
	test.AF(t, err != nil, "Expected non-nil error")

	// Storage node fails
	log.Println("TestClient_Read: Starting test #5")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "id1", Replicas: []string{"r1"}}
		ret := structure.FileBlocks{Fsize: 1, Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}
	err = c.Read("filename", &ret)
	test.AF(t, err != nil, "Expected non-nil error")

	// Empty file
	log.Println("TestClient_Read: Starting test #6")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		ret := structure.FileBlocks{Fsize: 0, Assignments: []structure.BlockAssign{}}
		return &ret, nil
	}
	err = c.Read("emptyfile", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, len(ret) == 0, fmt.Sprintf("Expected 0 bytes, found %q", ret))

	// File with one block
	log.Println("TestClient_Read: Starting test #7")
	data = []byte("Hello World")
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "file_1_1", Replicas: []string{addr1}}
		ret := structure.FileBlocks{Fsize: uint64(len(data)), Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}

	kv := structure.BlockKV{ID: "file_1_1", Data: gifts.Block(data)}
	err = s1.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	err = c.Read("filename", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, string(ret) == string(data), fmt.Sprintf("Expected %q, found %q", data, ret))

	// File with multiple blocks
	log.Println("TestClient_Read: Starting test #8")
	expected := strings.Repeat("test string", 1+(gifts.GiftsBlockSize/len("test string")))
	c.master.Read = func(fname string) (*structure.FileBlocks, error) {
		block1 := structure.BlockAssign{BlockID: "file_2_1", Replicas: []string{addr1}}
		block2 := structure.BlockAssign{BlockID: "file_2_2", Replicas: []string{addr2}}
		fsize := uint64(len(expected))

		ret := structure.FileBlocks{Fsize: fsize, Assignments: []structure.BlockAssign{block1, block2}}
		return &ret, nil
	}

	kv = structure.BlockKV{ID: "file_2_1", Data: gifts.Block(expected[:gifts.GiftsBlockSize])}
	err = s1.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	kv = structure.BlockKV{ID: "file_2_2", Data: gifts.Block(expected[gifts.GiftsBlockSize:])}
	err = s2.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	err = c.Read("file_2", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, string(ret) == expected, fmt.Sprintf("Expected %q, found %q", expected, ret))
}
