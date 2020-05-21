package client

import (
	"testing"

	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestClient_Store(t *testing.T) {
	t.Parallel()

	c := NewClient([]string{"master"})

	// Empty file  name
	err := c.Store("", 1, []byte(""))
	test.AF(t, err != nil, "Expected non-nil error")

	// rfactor is 0
	err = c.Store("filename", 0, []byte(""))
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call but Master returns incorrect number of blocks
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: "ID", Replicas: []string{"r1"}}
		return []structure.BlockAssign{block, block}, nil
	}
	err = c.Store("filename_1", 1, []byte("Hello World"))
	test.AF(t, err != nil, "Expected non-nil error")

	// Valid call with no data
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		return []structure.BlockAssign{}, nil
	}
	err = c.Store("filename_1", 1, []byte(""))
	test.AF(t, err == nil, "Expected non-nil error")

	// Valid call with less than one block of data and one replica
	c.master.Create = func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error) {
		block := structure.BlockAssign{BlockID: "ID", Replicas: []string{"r1"}}
		return []structure.BlockAssign{block}, nil
	}
	err = c.Store("filename_1", 1, []byte("Hello World"))
	test.AF(t, err == nil, "Expected non-nil error")

	// Valid call with more than one block of data and one replica

	// Valid call with more than one block of data and more than one replica

	// Duplicate file name

}
