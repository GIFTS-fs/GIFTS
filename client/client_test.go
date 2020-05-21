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

	// Empty file  name
	log.Println("TestClient_Store: Starting test #1")
	err := c.Store("", 1, []byte(""))
	test.AF(t, err != nil, "Expected non-nil error")

	// rfactor is 0
	log.Println("TestClient_Store: Starting test #2")
	err = c.Store("filename", 0, []byte(""))
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call but Master returns incorrect number of blocks
	log.Println("TestClient_Store: Starting test #3")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: "ID", Replicas: []string{"r1"}}
		return []structure.BlockAssign{block, block}, nil
	}
	err = c.Store("filename_1", 1, []byte("Hello World"))
	test.AF(t, err != nil, "Expected non-nil error")

	// Master failure
	log.Println("TestClient_Store: Starting test #4")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		return nil, fmt.Errorf("Master error")
	}
	err = c.Store("filename_1", 1, []byte("Hello World"))
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call with no data
	log.Println("TestClient_Store: Starting test #5")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		return []structure.BlockAssign{}, nil
	}
	err = c.Store("filename_1", 1, []byte(""))
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	// Valid call with less than one block of data and one replica
	log.Println("TestClient_Store: Starting test #6")
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: fname, Replicas: []string{addr1}}
		return []structure.BlockAssign{block}, nil
	}

	expected := "Hello World"
	err = c.Store("filename_1", 1, []byte(expected))
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
	err = c.Store("filename_2", 1, []byte(expected))
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
	err = c.Store("filename_3", 1, []byte(expected))
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
