package storage

import (
	gifts "github.com/GIFTS-fs/GIFTS"
)

const (
	// RPCPathNameNode the path that NameNode listens to
	RPCPathStorage = "/_gifts_storage_"
	// RPCMethodSet the RPC method name for Storage.Set
	RPCMethodSet = "Storage.Set"
	// RPCMethodGet the RPC method name for Storage.Get
	RPCMethodGet = "Storage.Get"
)

// SetFunc is the function signature for Storage.Set()
type SetFunc func(id string, data gifts.Block) (bool, error)

// GetFunc is the function signature for Storage.Get()
type GetFunc func(id string, buf gifts.Block) error
