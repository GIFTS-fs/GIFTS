package master

import (
	"sync"

	"github.com/GIFTS-fs/GIFTS/storage"
)

type assignBlock struct {
	fm       *fMeta
	blockIDs []string // a storage can store multiple blocks for one file
}

type storageMeta struct {
	rpc *storage.RPCStorage

	assignmentLock sync.Mutex
	nBlocks        int // number of blocks assigned
	storedFiles    map[string]assignBlock
}
