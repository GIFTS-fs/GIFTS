package master

import (
	"sync"

	"github.com/GIFTS-fs/GIFTS/storage"
)

type assignBlock struct {
	fm       *fileMeta
	blockIDs map[string]bool // a storage can store multiple blocks for one file
}

// nBlocks number of blocks stored for this file
func (ab *assignBlock) nBlocks() int {
	return len(ab.blockIDs)
}

type storeMeta struct {
	rpc *storage.RPCStorage

	assignmentLock sync.Mutex
	nBlocks        int // number of blocks assigned
	storedFiles    map[string]assignBlock
}

func newStoreMeta(addr string) *storeMeta {
	s := &storeMeta{
		rpc:         storage.NewRPCStorage(addr),
		storedFiles: make(map[string]assignBlock),
	}
	return s
}
