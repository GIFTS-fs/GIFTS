package test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
)

func TestMain(m *testing.M) {
	dir, _ := os.Getwd()
	config.LoadGet(filepath.Join(dir, "..", "config", "config.json"))
	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func TestIntergrationHelloWorld(t *testing.T) {
	addrMaster := "localhost:22321"
	addrStorage1 := "localhost:22322"
	addrStorage2 := "localhost:22323"
	addrStorages := []string{addrStorage1, addrStorage2}

	m := master.NewMaster(addrStorages, config.Get())
	if master.ServeRPC(m, addrMaster) != nil {
		t.Errorf("Failed to serv master %v", m)
	}

	s1 := storage.NewStorage()
	if storage.ServeRPC(s1, addrStorage1) != nil {
		t.Errorf("Failed to serv storage %v", s1)
	}

	s2 := storage.NewStorage()
	if storage.ServeRPC(s2, addrStorage2) != nil {
		t.Errorf("Failed to serv storage %v", s2)
	}

	c := client.NewClient([]string{addrMaster}, config.Get())

	f1Name := "helloWorld"
	f1Rfactor := uint(2)
	f1Data := []byte(strings.Repeat("Hello World!", 1024))

	if c.Store(f1Name, f1Rfactor, f1Data) != nil {
		t.Errorf("Failed to store %v", f1Data)
	}

	dataRead, err := c.Read(f1Name)
	if err != nil {
		t.Errorf("Failed to read %v", f1Name)
	}

	if bytes.Compare(dataRead, f1Data) != 0 {
		t.Errorf("Data mismatch: want %v got %v", f1Data, dataRead)
	}
}
