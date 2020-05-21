package client

import (
	"fmt"
	"log"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Client is the client of GIFTS
type Client struct {
	master   *master.Conn
	storages sync.Map
}

// NewClient creates a new GIFTS client
func NewClient(masters []string) *Client {
	c := Client{}
	c.master = master.NewConn(masters[0]) // WARN: hard-code for single master
	return &c
}

// Store stores a file with the specified file name, replication factor, and
// data.  Note that the replication factor is only a hint: we may allocate
// fewer replicas depending on the number of Storage nodes available.
// It returns an error if:
//		- A file with the specified file name already exists
//		- The Master does not give us enough blocks in which to store the data
//		- There is a network error (this is fatal and cannot be recovered from)
func (c *Client) Store(fname string, rfactor uint, data *[]byte) error {
	// Make sure file name is not empty
	if fname == "" {
		msg := "File name cannot be empty"
		log.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	// Make sure rfactor is not 0
	if rfactor <= 0 {
		msg := "Replication factor must be positive"
		log.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	fsize := uint64(len(*data))

	// Get block assignments from Master.
	// The result is a list where the ith element of the list is another list
	// that specifies the Storage nodes at which to replicate the ith block of
	// the file.
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		log.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
		return err
	}

	// Determine number of blocks needed to hold data
	nBlocks := fsize / gifts.GiftsBlockSize
	if fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}

	// Verify that the master gave us the correct number of Storage nodes to
	// write to.
	if nBlocks != uint64(len(assignments)) {
		msg := fmt.Sprintf("Need %d blocks but the Master gave us %d", nBlocks, len(assignments))
		log.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %q", fname, rfactor, fsize, msg)
		return fmt.Errorf(msg)
	}

	// Split data into blocks
	for i := uint64(0); i < nBlocks; i++ {
		startIndex := i * gifts.GiftsBlockSize
		endIndex := (i + 1) * gifts.GiftsBlockSize
		if endIndex > fsize {
			endIndex = fsize
		}

		var b gifts.Block = (*data)[startIndex:endIndex]

		// Write each block to the specified Storage nodes
		// TODO: Parallelize this with go statements
		for _, addr := range assignments[i].Replicas {
			rpcs, _ := c.storages.LoadOrStore(addr, storage.NewRPCStorage(addr))
			if err := rpcs.(*storage.RPCStorage).Set(&structure.BlockKV{ID: assignments[i].BlockID, Data: b}); err != nil {
				log.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
				return err
			}
		}
	}

	log.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => success", fname, rfactor, fsize)
	return nil
}

// Read reads a file with the specified file name from the remote Storage.
// It returns an error if:
//		- The file does not exist
// 		- The Master fails or returns inconsistent metadata
//		- There is a network error
func (c *Client) Read(fname string, ret *[]byte) error {
	// Clear return slice
	*ret = make([]byte, 0)

	// Get location of each block of the file from the Master
	fb, err := c.master.Read(fname)
	if err != nil {
		log.Printf("Client.Read(fname=%q) => %v", fname, err)
		return err
	}

	// Verify metadata from Master
	nBlocks := fb.Fsize / gifts.GiftsBlockSize
	if fb.Fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}
	if uint64(len(fb.Assignments)) != nBlocks {
		msg := fmt.Sprintf("Master returned %d blocks for a file with %d bytes", len(fb.Assignments), fb.Fsize)
		log.Printf("Client.Read(fname=%q) => %q", fname, msg)
		return fmt.Errorf(msg)
	}
	for _, replica := range fb.Assignments {
		if len(replica.Replicas) != 1 {
			msg := fmt.Sprintf("Master returned too many replicas: %v", replica)
			log.Printf("Client.Read(fname=%q) => %q", fname, msg)
			return fmt.Errorf(msg)
		}
	}

	// Handle empty files as a special case
	if fb.Fsize == 0 {
		log.Printf("Client.Read(fname=%q) => 0 bytes", fname)
		return nil
	}

	// Loop over every block
	// TODO: parallelize this with go routines
	*ret = make([]byte, fb.Fsize)
	temp := gifts.Block{}
	for i, block := range fb.Assignments {
		id := block.BlockID
		replica := block.Replicas[0]

		// Load block from remote Storage
		rpcs, _ := c.storages.LoadOrStore(replica, storage.NewRPCStorage(replica))
		if err := rpcs.(*storage.RPCStorage).Get(id, &temp); err != nil {
			log.Printf("Client.Read(fname=%q) => %v", fname, err)
			return err
		}

		startIndex := i * gifts.GiftsBlockSize
		endIndex := uint64((i + 1) * gifts.GiftsBlockSize)
		if endIndex > fb.Fsize {
			endIndex = fb.Fsize
		}
		copy((*ret)[startIndex:endIndex], temp)
	}

	log.Printf("Client.Read(fname=%q) => %d bytes", fname, fb.Fsize)
	return nil
}
