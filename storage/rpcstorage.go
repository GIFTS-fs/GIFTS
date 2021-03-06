package storage

import (
	"net/rpc"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// RPCStorage is a concurrency-safe key-value store accessible via RPC.
type RPCStorage struct {
	Addr   string
	Logger *gifts.Logger
	conn   *rpc.Client
}

// NewRPCStorage creates a client that allows you to access a raw Storage node
// that is accessible via RPC at the specified address.
func NewRPCStorage(addr string) *RPCStorage {
	logger := gifts.NewLogger("RPCStorage", addr, true)
	logger.Enabled = false
	return &RPCStorage{Addr: addr, Logger: logger} // PRODUCTION: banish this
}

func (s *RPCStorage) connect() (err error) {
	s.conn, err = rpc.DialHTTPPath("tcp", s.Addr, RPCPathStorage)
	return
}

// Set the data associated with the block's ID
func (s *RPCStorage) Set(kv *structure.BlockKV) error {
	var err error

	// If the Call returns an error, try reconnecting to the server and making the call again
	for try := 0; try < 2; try++ {
		// Connect to the server
		if s.conn == nil {
			if err = s.connect(); err != nil {
				break
			}
		}

		// Perform the call
		err = s.conn.Call("Storage.Set", kv, nil)
		if err == nil {
			break
		} else if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	}

	if err == nil {
		s.Logger.Printf("%q: RPCStorage.Set(%q, %d bytes) => success", s.Addr, kv.ID, len(kv.Data))
	} else {
		s.Logger.Printf("%q: RPCStorage.Set(%q, %d bytes) => %v", s.Addr, kv.ID, len(kv.Data), err)
	}

	return err
}

// Get the data associated with the block's ID
func (s *RPCStorage) Get(id string, ret *gifts.Block) error {
	var err error

	// Clear return value
	*ret = make([]byte, 0)

	// If the Call returns an error, try reconnecting to the server and making the call again
	for try := 0; try < 2; try++ {
		// Connect to the server
		if s.conn == nil {
			if err = s.connect(); err != nil {
				break
			}
		}

		// Perform the call
		err = s.conn.Call("Storage.Get", id, ret)
		if err == nil {
			if *ret == nil {
				*ret = make([]byte, 0)
			}
			break
		} else if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	}

	if err == nil {
		s.Logger.Printf("%q: RPCStorage.Get(%q) => %d bytes", s.Addr, id, len(*ret))
	} else {
		s.Logger.Printf("%q: RPCStorage.Get(%q) => %v", s.Addr, id, err)
	}

	return err
}

// Replicate the specified block to the destination Storage node
func (s *RPCStorage) Replicate(kv *structure.ReplicateKV) error {
	var err error

	// If the Call returns an error, try reconnecting to the server and making the call again
	for try := 0; try < 2; try++ {
		// Connect to the server
		if s.conn == nil {
			if err = s.connect(); err != nil {
				break
			}
		}

		// Perform the call
		err = s.conn.Call("Storage.Replicate", kv, nil)
		if err == nil {
			break
		} else if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	}

	if err == nil {
		s.Logger.Printf("%q: RPCStorage.Replicate(%v) => success", s.Addr, kv)
	} else {
		s.Logger.Printf("%q: RPCStorage.Replicate(%v) => %v", s.Addr, kv, err)
	}

	return err
}

// Unset the data associated with the block's ID
func (s *RPCStorage) Unset(id string, ignore *bool) error {
	var err error

	// If the Call returns an error, try reconnecting to the server and making the call again
	for try := 0; try < 2; try++ {
		// Connect to the server
		if s.conn == nil {
			if err = s.connect(); err != nil {
				break
			}
		}

		// Perform the call
		err = s.conn.Call("Storage.Unset", id, nil)
		if err == nil {
			break
		} else if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	}

	if err == nil {
		s.Logger.Printf("%q: RPCStorage.Unset(%q) => success", s.Addr, id)
	} else {
		s.Logger.Printf("%q: RPCStorage.Unset(%q) => %v", s.Addr, id, err)
	}

	return err
}
