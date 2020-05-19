package master

import "github.com/GIFTS-fs/GIFTS/structure"

// WARN: 2 below are not tested or used, just here to inspire
// CreateRPC is the ideal RPC signature
type CreateRPC func(req structure.FileCreateReq) []structure.BlockAssign

// ReadRPC is the ideal RPC signature
type ReadRPC func(fname string) structure.FileBlocks

// CreateFunc is the function signature for Master.Create()
type CreateFunc func(fname string, fsize uint64, rfactor int) ([]structure.BlockAssign, error)

// ReadFunc is the function signature for Master.Read()
type ReadFunc func(fname string) (structure.FileBlocks, error)
