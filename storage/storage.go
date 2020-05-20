package storage

import (
	"fmt"
	"log"
	"sync"

	gifts "GIFTS"
)

// Storage is a concurrency-safe key-value storage.
type Storage struct {
	blocks     map[string]gifts.Block
	blocksLock sync.RWMutex
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	return &Storage{blocks: make(map[string]gifts.Block)}
}

// Set sets the data associated with the block's ID
func (s *Storage) Set(id string, data *gifts.Block) error {
	log.Printf("Storage.Set(%q, %q)", id, *data)

	// Store data into block
	s.blocksLock.Lock()
	s.blocks[id] = make([]byte, len(*data))
	copy(s.blocks[id], *data)
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
		return fmt.Errorf("Block with ID %s does not exist", id)
	}

	// Copy data
	*ret = make([]byte, len(block))
	copy(*ret, block)
	return nil
}

// Unset deletes the data associated with the block's ID
func (s *Storage) Unset(id string) error {
	// Load block
	s.blocksLock.RLock()
	_, found := s.blocks[id]
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		return fmt.Errorf("Block with ID %s does not exist", id)
	}

	// Delete block
	s.blocksLock.Lock()
	delete(s.blocks, id)
	s.blocksLock.Unlock()

	return nil
}
