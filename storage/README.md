# Storage
```go
// Storage is a concurrency-safe key-value store.
type Storage struct {
	blocks     map[string]gifts.Block
	blocksLock sync.RWMutex
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	..
}

// ServeRPC makes the raw Storage accessible via RPC at the specified IP
// address and port.
func ServeRPC(s *Storage, addr string) error {
	...
}

// Set sets the data associated with the block's ID
func (s *Storage) Set(kv *structure.BlockKV, ignore *bool) error {
	...
}

// Get gets the data associated with the block's ID
func (s *Storage) Get(id string, ret *gifts.Block) error {
	...
}

// Unset deletes the data associated with the block's ID
func (s *Storage) Unset(id string, ignore *bool) error {
	...
}
```

# RPCStorage
```go
// RPCStorage is a concurrency-safe key-value store accessible via RPC.
type RPCStorage struct {
	addr string
	conn *rpc.Client
}

// NewRPCStorage creates a client that allows you to access a raw Storage node
// that is accessible via RPC at the specified address.
func NewRPCStorage(addr string) *RPCStorage {
	...
}

// Set sets the data associated with the block's ID
func (s *RPCStorage) Set(kv *structure.BlockKV) error {
	...
}

// Get gets the data associated with the block's ID
func (s *RPCStorage) Get(id string, ret *gifts.Block) error {
	...
}

// Unset deletes the data associated with the block's ID
func (s *RPCStorage) Unset(id string, ignore *bool) error {
	...
}
```