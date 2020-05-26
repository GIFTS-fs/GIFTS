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
	logger *gifts.Logger // PRODUCTION: banish this
	blocks sync.Map
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	return &Storage{
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
	s.blocks.Store(kv.ID, kv.Data)

	return nil
}

// Get gets the data associated with the block's ID
func (s *Storage) Get(id string, ret *gifts.Block) error {
	// Clear the return value
	*ret = make([]byte, 0)

	// Load block
	value, found := s.blocks.Load(id)

	// Check if ID exists
	if !found {
		msg := fmt.Sprintf("Block with ID %s does not exist", id)
		s.logger.Printf("Storage.Get(%q) => %q", id, msg)
		return fmt.Errorf(msg)
	}

	// Copy data
	block := value.(gifts.Block)
	*ret = make([]byte, len(block))
	copy(*ret, block)

	s.logger.Printf("Storage.Get(%q) => %d bytes", id, len(block))
	return nil
}

// Unset deletes the data associated with the block's ID
func (s *Storage) Unset(id string, ignore *bool) error {
	// Load block
	_, found := s.blocks.Load(id)

	// Check if ID exists
	if !found {
		msg := fmt.Sprintf("Block with ID %s does not exist", id)
		s.logger.Printf("Storage.Unset(%q) => %q", id, msg)
		return fmt.Errorf(msg)
	}

	// Delete block
	s.blocks.Delete(id)

	s.logger.Printf("Storage.Unset(%q) => success", id)
	return nil
}
