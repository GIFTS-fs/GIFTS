package storage

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// RPCPathStorage the path that Storage listens to
	RPCPathStorage = "/_gifts_storage_"
)

// Storage is a concurrency-safe key-value store.
type Storage struct {
	logger     *gifts.Logger // PRODUCTION: banish this
	blocks     map[string]gifts.Block
	blocksLock sync.RWMutex
	rpc        sync.Map
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	return &Storage{
		blocks: make(map[string]gifts.Block),
		logger: gifts.NewLogger("Storage", "storage", true), // PRODUCTION: banish this
	}
}

// ServeRPC makes the raw Storage accessible via RPC at the specified IP
// address and port.
func ServeRPC(s *Storage, addr string) (err error) {
	server := rpc.NewServer()

	err = server.Register(s)
	if err != nil {
		s.logger.Printf("ServeRPC(%q) => %v", addr, err)
		return
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Printf("ServeRPC(%q) => %v", addr, err)
		return
	}

	mux := http.NewServeMux()
	mux.Handle(RPCPathStorage, server)

	s.logger.Printf("ServeRPC(%q) => success", addr)

	go http.Serve(listener, mux)
	return
}

// Set sets the data associated with the block's ID
func (s *Storage) Set(kv *structure.BlockKV, ignore *bool) error {
	s.logger.Printf("Storage.Set(%q, %d bytes)", kv.ID, len(kv.Data))

	// Store data into block
	s.blocksLock.Lock()
	s.blocks[kv.ID] = make([]byte, len(kv.Data))
	copy(s.blocks[kv.ID], kv.Data)
	s.blocksLock.Unlock()

	return nil
}

// Get gets the data associated with the block's ID
func (s *Storage) Get(id string, ret *gifts.Block) error {
	// Clear the return value
	*ret = make([]byte, 0)

	// Load block
	s.blocksLock.RLock()
	block, found := s.blocks[id]
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", id)
		s.logger.Printf("Storage.Get(%q) => %q", id, err)
		return err
	}

	// Copy data
	*ret = make([]byte, len(block))
	copy(*ret, block)

	s.logger.Printf("Storage.Get(%q) => %d bytes", id, len(block))
	return nil
}

// Migrate copies the specified block to the destination Storage node
func (s *Storage) Migrate(kv *structure.MigrateKV, ignore *bool) error {
	// Load block
	s.blocksLock.RLock()
	block, found := s.blocks[kv.ID]
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", kv.ID)
		s.logger.Printf("Storage.Migrate(%q, %q) => %q", kv.ID, kv.Dest, err)
		return err
	}

	// Start an RPC session with the destination and copy the block
	rs, _ := s.rpc.LoadOrStore(kv.Dest, NewRPCStorage(kv.Dest))
	blockKV := structure.BlockKV{ID: kv.ID, Data: block}
	if err := rs.(*RPCStorage).Set(&blockKV); err != nil {
		s.logger.Printf("Storage.Migrate(%q, %q) => %v", kv.ID, kv.Dest, err)
		return err
	}

	s.logger.Printf("Storage.Migrate(%q, %q) => success", kv.ID, kv.Dest)
	return nil
}

// Unset deletes the data associated with the block's ID
func (s *Storage) Unset(id string, ignore *bool) error {
	// Load block
	s.blocksLock.RLock()
	_, found := s.blocks[id]
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", id)
		s.logger.Printf("Storage.Unset(%q) => %q", id, err)
		return err
	}

	// Delete block
	s.blocksLock.Lock()
	delete(s.blocks, id)
	s.blocksLock.Unlock()

	s.logger.Printf("Storage.Unset(%q) => success", id)
	return nil
}
