package client

import (
	"fmt"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Client is the client of GIFTS
type Client struct {
	Logger   *gifts.Logger // PRODUCTION: banish this
	config   *config.Config
	master   *master.Conn
	storages sync.Map
}

// NewClient creates a new GIFTS client
func NewClient(masters []string, config *config.Config) *Client {
	c := Client{}
	c.Logger = gifts.NewLogger("Client", "end-user", false) // PRODUCTION: banish this
	c.config = config
	c.master = master.NewConn(masters[0]) // WARN: hard-code for single master
	return &c
}

// Store stores a file with the specified file name, replication factor, and
// data. Note that the replication factor is only a hint: we may allocate
// fewer replicas depending on the number of Storage nodes available.
// Store() must not modify data[] or keep a copy of it.
// For current version, we just use whatever the Golang slice has:
// len() with int type
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
		msg := "File name cannot be empty"
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	// Make sure rfactor is not 0
	if rfactor <= 0 {
		msg := "Replication factor must be positive"
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	fsize := len(data)

	// Get block assignments from Master.
	// The result is a list where the ith element of the list is another list
	// that specifies the Storage nodes at which to replicate the ith block of
	// the file.
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
		return err
	}

	nBlocks := gifts.NBlocks(c.config.GiftsBlockSize, fsize)

	// Verify that the master gave us the correct number of Storage nodes to
	// write to.
	if nBlocks != len(assignments) {
		msg := fmt.Sprintf("Need %d blocks but the Master gave us %d", nBlocks, len(assignments))
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %q", fname, rfactor, fsize, msg)
		return fmt.Errorf(msg)
	}

	// For each block of data
	var wg sync.WaitGroup
	var terr error = nil
	for i, assignment := range assignments {
		startIndex := i * c.config.GiftsBlockSize
		endIndex := (i + 1) * c.config.GiftsBlockSize
		if endIndex > fsize {
			endIndex = fsize
		}

		var b gifts.Block = data[startIndex:endIndex]

		// Write to replicas
		for _, addr := range assignment.Replicas {

			// Get connection to Storage node.  If one doesn't already exist,
			// create one.  Note that a failed Load + LoadOrStore is ~14x
			// faster than a single LoadOrStore for the common scenario (write
			// once, read many), so this logic is on purpose.
			rpcs, ok := c.storages.Load(addr)
			if !ok {
				rpcs, _ = c.storages.LoadOrStore(addr, storage.NewRPCStorage(addr))
			}

			// Spawned go routines will stop on first (detected) error
			wg.Add(1)
			go func(id string, b gifts.Block) {
				defer wg.Done()

				// Another Set already failed so there's no point in doing this Set
				if terr != nil {
					return
				}

				if err := rpcs.(*storage.RPCStorage).Set(&structure.BlockKV{ID: id, Data: b}); err != nil {
					terr = err
				}
			}(assignment.BlockID, b)

		}
	}

	wg.Wait()

	if terr == nil {
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => success", fname, rfactor, fsize)
	} else {
		c.Logger.Printf("Client.Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, terr)
	}
	return terr
}

// Read reads a file with the specified file name from the remote Storage.
// It returns an error if:
//		- The file does not exist
// 		- The Master fails or returns inconsistent metadata
//		- There is a network error
func (c *Client) Read(fname string) ([]byte, error) {
	// PRODUCTION: banish all the logs

	// Get location of each block of the file from the Master
	fb, err := c.master.Lookup(fname)
	if err != nil {
		c.Logger.Printf("Client.Read(fname=%q) => %v", fname, err)
		return []byte{}, err
	}

	// Verify metadata from Master
	nBlocks := gifts.NBlocks(c.config.GiftsBlockSize, fb.Fsize)
	if len(fb.Assignments) != nBlocks {
		msg := fmt.Sprintf("Master returned %d blocks for a file with %d bytes", len(fb.Assignments), fb.Fsize)
		c.Logger.Printf("Client.Read(fname=%q) => %q", fname, msg)
		return []byte{}, fmt.Errorf(msg)
	}

	var wg sync.WaitGroup

	// Loop over every block
	bytesRead := make([]byte, fb.Fsize)
	var terr error = nil
	for i, block := range fb.Assignments {
		// return error if not enough replicas provided
		if len(block.Replicas) < 1 {
			terr = fmt.Errorf("Master didn't return any replicas: %v", block)
			break
		}

		// WARN: hard-code only one
		replica := block.Replicas[0]

		// Get connection to Storage node.  If one doesn't already exist,
		// create one.  Note that a failed Load + LoadOrStore is ~14x
		// faster than a single LoadOrStore for the common scenario (write
		// once, read many), so this logic is on purpose.
		rpcs, ok := c.storages.Load(replica)
		if !ok {
			rpcs, _ = c.storages.LoadOrStore(replica, storage.NewRPCStorage(replica))
		}

		startIndex := i * c.config.GiftsBlockSize
		endIndex := (i + 1) * c.config.GiftsBlockSize
		if endIndex > fb.Fsize {
			endIndex = fb.Fsize
		}

		// Spawned go routines will stop on first (detected) error
		wg.Add(1)
		go func(id string, start, end int) {
			defer wg.Done()
			// Another Get already failed so there's no point in doing this Get
			if terr != nil {
				return
			}

			var blockRead gifts.Block
			if err := rpcs.(*storage.RPCStorage).Get(id, &blockRead); err != nil {
				terr = err
			}

			copy(bytesRead[start:end], blockRead)
		}(block.BlockID, startIndex, endIndex)

	}

	wg.Wait()

	if terr != nil {
		c.Logger.Printf("Client.Read(fname=%q) => %v", fname, terr)
		return []byte{}, terr
	}

	c.Logger.Printf("Client.Read(fname=%q) => %d bytes", fname, fb.Fsize)
	return bytesRead, nil
}
