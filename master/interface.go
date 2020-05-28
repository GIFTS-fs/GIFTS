package master

import "github.com/GIFTS-fs/GIFTS/structure"

const (
	// RPCPathMaster the path that NameNode listens to
	RPCPathMaster = "/_gifts_master_"
	// RPCMethodCreate the RPC method name
	RPCMethodCreate = "Master.Create"
	// RPCMethodLookup the RPC method name
	RPCMethodLookup = "Master.Lookup"
)

// CreateFunc is the function signature for Master.Create()
type CreateFunc func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error)

// LookupFunc is the function signature for Master.Lookup()
type LookupFunc func(fname string) (*structure.FileBlocks, error)
