package storage

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Storage is a concurrency-safe key-value store.
type Storage struct {
	blocks     map[string]gifts.Block
	blocksLock sync.RWMutex
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	return &Storage{blocks: make(map[string]gifts.Block)}
}

// ServeRPC makes the raw Storage accessible via RPC at the specified IP
// address and port.
func ServeRPC(s *Storage, addr string) error {
	server := rpc.NewServer()
	server.Register(s)

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	http.DefaultServeMux = oldMux

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("ServeRPC(%q) => %v", addr, err)
		return err
	}

	log.Printf("ServeRPC(%q) => success", addr)
	go http.Serve(listener, mux)
	return nil
}

// Set sets the data associated with the block's ID
func (s *Storage) Set(kv *structure.BlockKV, ignore *bool) error {
	log.Printf("Storage.Set(%q, %q)", kv.ID, kv.Data)

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
	*ret = (*ret)[:0]

	// Load block
	s.blocksLock.RLock()
	block, found := s.blocks[id]
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		msg := fmt.Sprintf("Block with ID %s does not exist", id)
		log.Printf("Storage.Get(%q) => %q", id, msg)
		return fmt.Errorf(msg)
	}

	// Copy data
	*ret = make([]byte, len(block))
	copy(*ret, block)

	log.Printf("Storage.Get(%q) => %d bytes", id, len(block))
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
		msg := fmt.Sprintf("Block with ID %s does not exist", id)
		log.Printf("Storage.Unset(%q) => %q", id, msg)
		return fmt.Errorf(msg)
	}

	// Delete block
	s.blocksLock.Lock()
	delete(s.blocks, id)
	s.blocksLock.Unlock()

	log.Printf("Storage.Unset(%q) => success", id)
	return nil
}
