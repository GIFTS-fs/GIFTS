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

// NewClient creates a new GIFTS's client
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
func (c *Client) Store(fname string, rfactor uint, data []byte) error {
	// Make sure file name is not empty
	if fname == "" {
		msg := "File name cannot be empty"
		log.Printf("Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	// Make sure rfactor is not 0
	if rfactor <= 0 {
		msg := "Replication factor must be positive"
		log.Printf("Store(fname=%q, rfactor=%d) => %q", fname, rfactor, msg)
		return fmt.Errorf(msg)
	}

	fsize := uint64(len(data))

	// Get block assignments from Master.
	// The result is a list where the ith element of the list is another list
	// that specifies the Storage nodes at which to replicate the ith block of
	// the file.
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		log.Printf("Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
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
		log.Printf("Store(fname=%q, rfactor=%d, fsize=%d) => %q", fname, rfactor, fsize, msg)
		return fmt.Errorf(msg)
	}

	// Split data into blocks
	for i := uint64(0); i < nBlocks; i++ {
		startIndex := i * gifts.GiftsBlockSize
		endIndex := (i + 1) * gifts.GiftsBlockSize
		if endIndex > fsize-1 {
			endIndex = fsize - 1
		}

		var b gifts.Block = data[startIndex:endIndex]

		// Write each block to the specified Storage nodes
		// TODO: Parallelize this with go statements
		for _, addr := range assignments[i].Replicas {
			rpcs := storage.NewRPCStorage(addr)
			if err := rpcs.Set(&structure.BlockKV{ID: assignments[i].BlockID, Data: b}); err != nil {
				log.Printf("Store(fname=%q, rfactor=%d, fsize=%d) => %v", fname, rfactor, fsize, err)
				return err
			}
		}
	}

	log.Printf("Store(fname=%q, rfactor=%d, fsize=%d) => success", fname, rfactor, fsize)
	return nil
}

func (c *Client) Read(fname string) ([]byte, error) {
	// fblks, err := c.master.Read(fname)
	// if err != nil {
	// 	return nil, err
	// }

	// if fblks.Fsize == 0 {
	// 	return []byte{}, nil
	// }

	// // get a multiple of gifts.GiftsBlockSize
	// paddingSize := fblks.Fsize % gifts.GiftsBlockSize
	// bufSize := fblks.Fsize + paddingSize

	// data := make([]byte, bufSize)
	// for i, assignment := range fblks.Assignments {
	// 	// TODO: works?????????
	// 	err := c.storageConn(assignment.Replicas[0]).Get(assignment.BlockID, data[i*gifts.GiftsBlockSize:])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// // truncate the padding
	// // TODO: off by one????
	// return data[:bufSize-paddingSize], nil

	panic("TODO")
}
