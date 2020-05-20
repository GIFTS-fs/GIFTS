package storage

import (
	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// RPCPathNameNode the path that NameNode listens to
	RPCPathStorage = "/_gifts_storage_"
	// RPCMethodSet the RPC method name for Storage.Set
	RPCMethodSet = "Storage.Set"
	// RPCMethodGet the RPC method name for Storage.Get
	RPCMethodGet = "Storage.Get"
)

// WARN: 2 below are **NOT tested or used**, just here to inspire
// CreateRPC is the ideal RPC signature
type SetRPC func(req *structure.BlockKV, ret *bool)

// ReadRPC is the ideal RPC signature
type GetRPC func(id string, ret *gifts.Block)

// SetFunc is the function signature for Storage.Set()
type SetFunc func(id string, data gifts.Block) (bool, error)

// GetFunc is the function signature for Storage.Get()
type GetFunc func(id string, buf gifts.Block) error
