package client

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/master"
)

// Client is the client of GIFTS
type Client struct {
	master *master.Conn
	rand   *rand.Rand
}

// NewClient is the constrctor for GiFTS's client
func NewClient(masters []string) *Client {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, time.Now().UnixNano())
	h.Write(h.Sum())

	c := Client{}
	c.conn = master.NewConn(masters[0]) // hard-code for single master
	c.rand = rand.New(rand.NewSource(h.Sum64()))
	return &c
}

// Create a file with file name, file size, and replication factor.
// Return error if fname already exists, network failure etc.
func (c *Client) Create(fname string, fsize uint64, rfactor int) error {
	assignments, err := c.master.Create(fname, fsize, rfactor)
	if err != nil {
		return err
	}

	nBlocks := fsize / gifts.GiftsBlockSize
	if fsize%gifts.GiftsBlockSize != 0 {
		nBlocks++
	}

	if nBlocks != len(assignments) {
		// TODO: remove this
		panic("wrong number of blocks!!")
	}

	// WARN: for this system, all "files" are populated by random bytes here,
	// no actual writes supported (yet)

	var byteBlocks [][]byte
	for i := 0; i < nBlocks; i++ {
		blk := make([]byte, gifts.GiftsBlockSize)
		c.rand.Read(blk)
		byteBlocks = append(byteBlocks, blk)
	}

	for i := 0; i < nBlocks; i++ {
		for _, rAddr := range assignments[i].Replicas {
			// IN PROGRESS: need a connection to Storages
		}
	}
}
