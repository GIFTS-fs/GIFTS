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

// WARN: 2 below are **NOT tested or used**, just here to inspire
// CreateRPC is the ideal RPC signature
type CreateRPC func(req *structure.FileCreateReq, ret *[]structure.BlockAssign)

// ReadRPC is the ideal RPC signature
type ReadRPC func(fname string, ret *structure.FileBlocks)

// CreateFunc is the function signature for Master.Create()
type CreateFunc func(fname string, fsize uint64, rfactor uint) ([]structure.BlockAssign, error)

// ReadFunc is the function signature for Master.Read()
type ReadFunc func(fname string) (*structure.FileBlocks, error)
