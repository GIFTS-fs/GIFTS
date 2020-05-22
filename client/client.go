package client

import (
	"fmt"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Client is the client of GIFTS
type Client struct {
	logger   *gifts.Logger // PRODUCTION: banish this
	master   *master.Conn
	storages sync.Map
}

// NewClient creates a new GIFTS client
func NewClient(masters []string) *Client {
	c := Client{}
	c.logger = gifts.NewLogger("Client", "end-user", true) // PRODUCTION: banish this
	c.master = master.NewConn(masters[0])                  // WARN: hard-code for single master
	return &c
}

// Store stores a file with the specified file name, replication factor, and
// data. Note that the replication factor is only a hint: we may allocate
// fewer replicas depending on the number of Storage nodes available.
// Store() must not modify data[] or keep a copy of it.
// For current versoin, max file size is 9223372036854775807, the max len of Golang slice.
//
// It returns an error if:
//		- A file with the specified file name already exists
//
//		- The Master does not give us enough blocks in which to store the data
//
//		- There is a network error (this is fatal and cannot be recovered from)
func (c *Client) Store(fname string, rfactor uint, data []byte) error {
	// PRODUCTION: banish all logs

	// Make sure file name is not empty
	if fname == "" {
		// return fmt.Errorf("File name cannot be empty")
		msg := "File name cannot be empty"
		c.logger.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	// Make sure rfactor is not 0
	if rfactor <= 0 {
		msg := "Replication factor must be positive"
		c.logger.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	fsize := len(data)

	// Get block assignments from Master.
	// The result is a list where the ith element of the list is another list
	// that specifies the Storage nodes at which to replicate the ith block of
	// the file.
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		c.logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
		return err
	}

	// Determine number of blocks needed to hold data
	nBlocks := fsize / gifts.GiftsBlockSize
	if fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}

	// Verify that the master gave us the correct number of Storage nodes to
	// write to.
	if nBlocks != len(assignments) {
		msg := fmt.Sprintf("Need %d blocks but the Master gave us %d", nBlocks, len(assignments))
		c.logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %q", fname, rfactor, fsize, msg)
		return fmt.Errorf(msg)
	}

	var wg sync.WaitGroup

	// For each block of data
	for i, assignment := range assignments {
		startIndex := i * gifts.GiftsBlockSize
		endIndex := (i + 1) * gifts.GiftsBlockSize
		if endIndex > fsize {
			endIndex = fsize
		}

		var b gifts.Block = data[startIndex:endIndex]

		// Write to replicas
		for _, addr := range assignment.Replicas {
			rpcs, ok := c.storages.Load(addr)
			if !ok {
				rpcs = storage.NewRPCStorage(addr)
				a, loaded := c.storages.LoadOrStore(addr, rpcs)
				if loaded {
					rpcs = a
				}
			}

			wg.Add(1)

			// spawned threads will stop on first (detected) error
			go func(id string) {
				// WARN: no sync on err, and threads stop when err is not nil
				// data racing expected
				defer wg.Done()
				if err != nil {
					return
				}

				if err = rpcs.(*storage.RPCStorage).Set(&structure.BlockKV{ID: id, Data: b}); err != nil {
					c.logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
				}
			}(assignment.BlockID)

		}
	}

	wg.Wait()

	// if err == nil {
	// 	c.logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => success", fname, rfactor, fsize)
	// }
	return err
}

// Read reads a file with the specified file name from the remote Storage.
// It returns an error if:
//		- The file does not exist
// 		- The Master fails or returns inconsistent metadata
//		- There is a network error
func (c *Client) Read(fname string) ([]byte, error) {
	// PRODUCTION: banish all the logs

	// Get location of each block of the file from the Master
	fb, err := c.master.Read(fname)
	if err != nil {
		c.logger.Printf("Client.Read(fname=%q) => %v", fname, err)
		return []byte{}, err
	}

	// Verify metadata from Master
	nBlocks := fb.Fsize / gifts.GiftsBlockSize
	if fb.Fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}
	if len(fb.Assignments) != nBlocks {
		msg := fmt.Sprintf("Master returned %d blocks for a file with %d bytes", len(fb.Assignments), fb.Fsize)
		c.logger.Printf("Client.Read(fname=%q) => %q", fname, msg)
		return []byte{}, fmt.Errorf(msg)
	}

	var wg sync.WaitGroup

	// Loop over every block
	bytesRead := make([]byte, fb.Fsize)
	var blockRead gifts.Block
	for i, block := range fb.Assignments {
		// return error if not enough replicas provided
		if len(block.Replicas) < 1 {
			wg.Wait()
			msg := fmt.Sprintf("Master returned too few replicas: %v", block)
			c.logger.Printf("Client.Read(fname=%q) => %q", fname, msg)
			return []byte{}, fmt.Errorf(msg)
		}

		// WARN: hard-code only one
		replica := block.Replicas[0]

		// Load block from remote Storage

		rpcs, ok := c.storages.Load(replica)
		if !ok {
			rpcs = storage.NewRPCStorage(replica)
			a, loaded := c.storages.LoadOrStore(replica, rpcs)
			if loaded {
				rpcs = a
			}
		}

		startIndex := i * gifts.GiftsBlockSize
		endIndex := (i + 1) * gifts.GiftsBlockSize
		if endIndex > fb.Fsize {
			endIndex = fb.Fsize
		}

		wg.Add(1)

		go func(id string) {
			defer wg.Done()
			if err != nil {
				return
			}

			if err = rpcs.(*storage.RPCStorage).Get(id, &blockRead); err != nil {
				c.logger.Printf("Client.Read(fname=%q) => %v", fname, err)
			}

			copy(bytesRead[startIndex:endIndex], blockRead)
		}(block.BlockID)

	}

	wg.Wait()

	if err != nil {
		return []byte{}, err
	}

	c.logger.Printf("Client.Read(fname=%q) => %d bytes", fname, fb.Fsize)
	return bytesRead, nil
}
