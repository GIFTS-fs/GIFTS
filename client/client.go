package client

import (
	"fmt"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/master"
	"github.com/GIFTS-fs/GIFTS/storage"
)

// Client is the client of GIFTS
type Client struct {
	master   *master.Conn
	storages sync.Map
}

// NewClient is the constrctor for GiFTS's client
func NewClient(masters []string) *Client {
	c := Client{}
	c.master = master.NewConn(masters[0]) // WARN: hard-code for single master
	return &c
}

func (c *Client) storageConn(addr string) *storage.Conn {
	conn, ok := c.storages.Load(addr)
	if !ok || conn == nil {
		conn = storage.NewConn(addr)
		c.storages.Store(addr, conn)
	}
	return conn.(*storage.Conn)
}

// Store a file with file name, file size, and replication factor.
// Return error if fname already exists, network failure etc.
func (c *Client) Store(fname string, fsize uint64, rfactor int, data []byte) error {
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		return err
	}

	nBlocks := fsize / gifts.GiftsBlockSize
	if fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}

	// TODO: safe casting?
	if nBlocks != uint64(len(assignments)) {
		// TODO: remove this
		panic("wrong number of blocks!!")
	}

	// TODO: safe casting?
	for i := uint64(0); i < nBlocks; i++ {
		// TODO: figure out padding and so forth
		var b gifts.Block = data[i*gifts.GiftsBlockSize : (i+1)*gifts.GiftsBlockSize]
		for _, rAddr := range assignments[i].Replicas {
			succ, err := c.storageConn(rAddr).Set(assignments[i].BlockID, b)
			if err != nil {
				return err
			} else if !succ {
				return fmt.Errorf("Set Failed for BlockID %v: %v", assignments[i].BlockID, b)
			}
		}
	}

	return nil
}

func (c *Client) Read(fname string) ([]byte, error) {
	fblks, err := c.master.Read(fname)
	if err != nil {
		return nil, err
	}

	if fblks.Fsize == 0 {
		return []byte{}, nil
	}

	// get a multiple of gifts.GiftsBlockSize
	paddingSize := fblks.Fsize % gifts.GiftsBlockSize
	bufSize := fblks.Fsize + paddingSize

	data := make([]byte, bufSize)
	for i, assignment := range fblks.Assignments {
		// TODO: works?????????
		err := c.storageConn(assignment.Replicas[0]).Get(assignment.BlockID, data[i*gifts.GiftsBlockSize:])
		if err != nil {
			return nil, err
		}
	}

	// truncate the padding
	// TODO: off by one????
	return data[:bufSize-paddingSize], nil
}
