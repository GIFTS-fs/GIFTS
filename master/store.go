package master

import (
	"sync"

	"github.com/GIFTS-fs/GIFTS/storage"
)

// blockFile of the file to a storage
type blockFile struct {
	fm       *fileMeta
	blockIDs map[string]bool // a storage can store multiple blocks for one file
}

func newBlockFile(fm *fileMeta) *blockFile {
	return &blockFile{fm: fm, blockIDs: make(map[string]bool)}
}

func (ab *blockFile) addBlock(blockID string) {
	ab.blockIDs[blockID] = true
}

func (ab *blockFile) rmBlock(blockID string) {
	delete(ab.blockIDs, blockID)
}

func (ab *blockFile) hasBlock(blockID string) bool {
	b, ok := ab.blockIDs[blockID]
	return ok && b
}

// nBlocks number of blocks stored for this file
func (ab *blockFile) nBlocks() int {
	return len(ab.blockIDs)
}

type storeMeta struct {
	Addr string
	rpc  *storage.RPCStorage

	assignmentLock sync.Mutex
	nBlocks        int // number of blocks assigned
	storedFiles    map[string]blockFile
}

func newStoreMeta(addr string) *storeMeta {
	s := &storeMeta{
		Addr:        addr,
		rpc:         storage.NewRPCStorage(addr),
		storedFiles: make(map[string]blockFile),
	}
	return s
}
