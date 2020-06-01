package master

import (
	"sync"

	"github.com/GIFTS-fs/GIFTS/storage"
)

// assignedBlocks of the file to a storage
type assignedBlocks struct {
	fm       *fileMeta
	blockIDs map[string]bool // a storage can store multiple blocks for one file
}

func (ab *assignedBlocks) addBlock(blockID string) {
	ab.blockIDs[blockID] = true
}

func (ab *assignedBlocks) rmBlock(blockID string) {
	delete(ab.blockIDs, blockID)
}

func (ab *assignedBlocks) hasBlock(blockID string) bool {
	b, ok := ab.blockIDs[blockID]
	return ok && b
}

// nBlocks number of blocks stored for this file
func (ab *assignedBlocks) nBlocks() int {
	return len(ab.blockIDs)
}

type storeMeta struct {
	Addr string
	rpc  *storage.RPCStorage

	assignmentLock sync.Mutex
	nBlocks        int // number of blocks assigned
	storedFiles    map[string]assignedBlocks
}

func newStoreMeta(addr string) *storeMeta {
	s := &storeMeta{
		Addr:        addr,
		rpc:         storage.NewRPCStorage(addr),
		storedFiles: make(map[string]assignedBlocks),
	}
	return s
}

// func (sm *storeMeta) enlist(em *enlistment) {
// }
