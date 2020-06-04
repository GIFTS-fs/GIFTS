package client

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestMain(m *testing.M) {
	dir, _ := os.Getwd()
	config.LoadGet(filepath.Join(dir, "..", "config", "config.json"))
	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func TestClient_Store(t *testing.T) {
	t.Parallel()

	c := NewClient([]string{"master"}, config.Get())

	addr1 := "localhost:3003"
	addr2 := "localhost:3004"
	s1 := storage.NewStorage()
	s2 := storage.NewStorage()
	storage.ServeRPC(s1, addr1)
	storage.ServeRPC(s2, addr2)

	var data []byte

	// Empty file name
	t.Logf("TestClient_Store: Starting test #1")
	data = []byte("")
	err := c.Store("", 1, data)
	test.AF(t, err != nil, "Expected non-nil error")

	// rfactor is 0
	t.Logf("TestClient_Store: Starting test #2")
	data = []byte("")
	err = c.Store("filename", 0, data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call but Master returns incorrect number of blocks
	t.Logf("TestClient_Store: Starting test #3")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: "ID", Replicas: []string{"r1"}}
		return []structure.BlockAssign{block, block}, nil
	}
	data = []byte("Hello World")
	err = c.Store("filename_1", 1, data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Master failure
	t.Logf("TestClient_Store: Starting test #4")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		return nil, fmt.Errorf("Master error")
	}
	data = []byte("Hello World")
	err = c.Store("filename_1", 1, data)
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call with no data
	t.Logf("TestClient_Store: Starting test #5")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		return []structure.BlockAssign{}, nil
	}
	data = []byte("")
	err = c.Store("filename_1", 1, data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	// Valid call with less than one block of data and one replica
	t.Logf("TestClient_Store: Starting test #6")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: fname, Replicas: []string{addr1}}
		return []structure.BlockAssign{block}, nil
	}

	expected := "Hello World"
	data = []byte(expected)
	err = c.Store("filename_1", 1, data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	ret := gifts.Block{}
	err = s1.Get("filename_1", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(ret) == expected, fmt.Sprintf("Expected %q but found %q", expected, ret))

	// Valid call with more than one block of data and one replica
	t.Logf("TestClient_Store: Starting test #7")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		block1 := structure.BlockAssign{BlockID: fname + "_1", Replicas: []string{addr1}}
		block2 := structure.BlockAssign{BlockID: fname + "_2", Replicas: []string{addr1}}
		return []structure.BlockAssign{block1, block2}, nil
	}

	expected = strings.Repeat("test string", 1+(c.config.GiftsBlockSize/len("test string")))
	data = []byte(expected)
	err = c.Store("filename_2", 1, data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	err = s1.Get("filename_2_1", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, len(ret) == c.config.GiftsBlockSize, fmt.Sprintf("Expected %d bytes but found %d", c.config.GiftsBlockSize, len(ret)))
	test.AF(t, string(ret) == expected[:c.config.GiftsBlockSize], fmt.Sprintf("Expected %q but found %q", expected, ret))

	err = s1.Get("filename_2_2", &ret)
	test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
	test.AF(t, string(ret) == expected[c.config.GiftsBlockSize:], fmt.Sprintf("Expected %q but found %q", expected, ret))

	// Valid call with more than one block of data and more than one replica
	t.Logf("TestClient_Store: Starting test #8")
	c.master.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		block1 := structure.BlockAssign{BlockID: fname + "_1", Replicas: []string{addr1, addr2}}
		block2 := structure.BlockAssign{BlockID: fname + "_2", Replicas: []string{addr1, addr2}}
		return []structure.BlockAssign{block1, block2}, nil
	}

	expected = strings.Repeat("test string 2", 1+(c.config.GiftsBlockSize/len("test string")))
	data = []byte(expected)
	err = c.Store("filename_3", 1, data)
	test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: \"%v\"", err))

	for _, s := range []*storage.Storage{s1, s2} {
		err = s.Get("filename_3_1", &ret)
		test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
		test.AF(t, len(ret) == c.config.GiftsBlockSize, fmt.Sprintf("Expected %d bytes but found %d", c.config.GiftsBlockSize, len(ret)))
		test.AF(t, string(ret) == expected[:c.config.GiftsBlockSize], fmt.Sprintf("Expected %q but found %q", expected, ret))

		err = s.Get("filename_3_2", &ret)
		test.AF(t, err == nil, fmt.Sprintf("Storage.Get failed: %v", err))
		test.AF(t, string(ret) == expected[c.config.GiftsBlockSize:], fmt.Sprintf("Expected %q but found %q", expected, ret))
	}
}

func TestClient_Read(t *testing.T) {
	t.Parallel()

	c := NewClient([]string{"master"}, config.Get())

	addr1 := "localhost:3005"
	addr2 := "localhost:3006"
	s1 := storage.NewStorage()
	s2 := storage.NewStorage()
	storage.ServeRPC(s1, addr1)
	storage.ServeRPC(s2, addr2)

	var data []byte

	// File does not exist
	t.Logf("TestClient_Read: Starting test #1")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		return nil, fmt.Errorf("%q does not exist", fname)
	}
	ret, err := c.Read("Invalid file")
	test.AF(t, err != nil, "Expected non-nil error")

	// Master fails
	t.Logf("TestClient_Read: Starting test #2")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		return nil, fmt.Errorf("Master failed")
	}
	ret, err = c.Read("filename")
	test.AF(t, err != nil, "Expected non-nil error")

	// Master returns incorrect number of assignments
	t.Logf("TestClient_Read: Starting test #3")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		ret := structure.FileBlocks{Fsize: c.config.GiftsBlockSize * 2, Assignments: []structure.BlockAssign{}}
		return &ret, nil
	}
	ret, err = c.Read("filename")
	test.AF(t, err != nil, "Expected non-nil error")

	// Master returns incorrect number of Storage nodes for each block
	t.Logf("TestClient_Read: Starting test #4")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "id1", Replicas: []string{}}
		ret := structure.FileBlocks{Fsize: 1, Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}
	ret, err = c.Read("filename")
	test.AF(t, err != nil, "Expected non-nil error")

	// Storage node fails
	t.Logf("TestClient_Read: Starting test #5")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "id1", Replicas: []string{"r1"}}
		ret := structure.FileBlocks{Fsize: 1, Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}
	ret, err = c.Read("filename")
	test.AF(t, err != nil, "Expected non-nil error")

	// Empty file
	t.Logf("TestClient_Read: Starting test #6")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		ret := structure.FileBlocks{Fsize: 0, Assignments: []structure.BlockAssign{}}
		return &ret, nil
	}
	ret, err = c.Read("emptyfile")
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, len(ret) == 0, fmt.Sprintf("Expected 0 bytes, found %q", ret))

	// File with one block
	t.Logf("TestClient_Read: Starting test #7")
	data = []byte("Hello World")
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		block := structure.BlockAssign{BlockID: "file_1_1", Replicas: []string{addr1}}
		ret := structure.FileBlocks{Fsize: len(data), Assignments: []structure.BlockAssign{block}}
		return &ret, nil
	}

	kv := structure.BlockKV{ID: "file_1_1", Data: gifts.Block(data)}
	err = s1.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	ret, err = c.Read("filename")
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, string(ret) == string(data), fmt.Sprintf("Expected %q, found %q", data, ret))

	// File with multiple blocks
	t.Logf("TestClient_Read: Starting test #8")
	expected := strings.Repeat("test string", 1+(c.config.GiftsBlockSize/len("test string")))
	c.master.Lookup = func(fname string) (*structure.FileBlocks, error) {
		block1 := structure.BlockAssign{BlockID: "file_2_1", Replicas: []string{addr1}}
		block2 := structure.BlockAssign{BlockID: "file_2_2", Replicas: []string{addr2}}
		fsize := len(expected)

		ret := structure.FileBlocks{Fsize: fsize, Assignments: []structure.BlockAssign{block1, block2}}
		return &ret, nil
	}

	kv = structure.BlockKV{ID: "file_2_1", Data: gifts.Block(expected[:c.config.GiftsBlockSize])}
	err = s1.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	kv = structure.BlockKV{ID: "file_2_2", Data: gifts.Block(expected[c.config.GiftsBlockSize:])}
	err = s2.Set(&kv, new(bool))
	test.AF(t, err == nil, fmt.Sprintf("Storage.Set failed: %v", err))

	ret, err = c.Read("file_2")
	test.AF(t, err == nil, fmt.Sprintf("Client.Read failed: %v", err))
	test.AF(t, string(ret) == expected, fmt.Sprintf("Expected %q, found %q", expected, ret))
}

func TestBenchmarkClient_ReadOneFile(t *testing.T) {
	t.Skip()
	file, err := os.Create("./results.csv")
	test.AF(t, err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	writer := bufio.NewWriter(file)
	defer file.Close()

	var testTime float64 = 2
	var nTests float64 = 1

	config, err := config.LoadGet("../config/config.json")
	test.AF(t, err == nil, fmt.Sprintf("Error loading config: %v", err))

	for _, addr := range config.Storages {
		s := storage.NewStorage()
		storage.ServeRPC(s, addr)
	}

	m := master.NewMaster(config.Storages, config)
	master.ServeRPC(m, config.Master)

	for blockSize := 1048576; blockSize <= 1048576; blockSize *= 2 { // For each block size
		config.GiftsBlockSize = blockSize

		for fileSize := blockSize; fileSize <= blockSize*len(config.Storages); fileSize *= 2 { // For each file size
			data := make([]byte, fileSize)
			generate.NewGenerate().Read(data)

			for nReplicas := 1; nReplicas <= len(config.Storages); nReplicas++ { // For the number of replicas
				fName := fmt.Sprintf("file_%d_%d_%d", blockSize, fileSize, nReplicas)
				err := NewClient([]string{config.Master}, config).Store(fName, uint(nReplicas), data)
				test.AF(t, err == nil, fmt.Sprintf("Client.Store failed: %v", err))

				for nClients := 50; nClients < 51; nClients++ { // For the number of concurrent clients

					var nTotalReads int64 = 0
					for test := 0; test < int(nTests); test++ { // For the number of tests per run
						done := make(chan int64, nClients)

						for nClient := 0; nClient < nClients; nClient++ {
							go func() {
								c := NewClient([]string{config.Master}, config)
								var nReads int64 = 0

								for startTime := time.Now(); time.Since(startTime).Seconds() < testTime; {
									c.Read(fName)
									nReads++
								}

								done <- nReads
							}()
						}

						for nClient := 0; nClient < nClients; nClient++ {
							nTotalReads += <-done
						}

					}

					bytesRead := nTotalReads * int64(fileSize)
					throughputBPS := float64(bytesRead) / (testTime * nTests)
					throughputMBPS := throughputBPS / 1000000
					result := fmt.Sprintf("%d,%d,%d,%d: %.2fMB/s", blockSize, fileSize, nReplicas, nClients, throughputMBPS)
					writer.WriteString(result + "\n")
					writer.Flush()
				}
			}
		}
	}
}
