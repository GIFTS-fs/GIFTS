package master

import "github.com/GIFTS-fs/GIFTS/structure"

const (
	// RPCPathMaster the path that NameNode listens to
	RPCPathMaster = "/_gifts_master_"
	// RPCMethodCreate the RPC method name
	RPCMethodCreate = "Master.Create"
	// RPCMethodRead the RPC method name
	RPCMethodRead = "Master.Read"
)

// CreateFunc is the function signature for Master.Create()
type CreateFunc func(fname string, fsize uint, rfactor uint) ([]structure.BlockAssign, error)

// ReadFunc is the function signature for Master.Read()
type ReadFunc func(fname string) (*structure.FileBlocks, error)
