# Storage
```go
// Storage is a concurrency-safe key-value store.
type Storage struct {
	logger     *gifts.Logger
	blocks     map[string]gifts.Block
	blocksLock sync.RWMutex
	rpc        sync.Map
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

// Migrate copies the specified block to the destination Storage node
func (s *Storage) Migrate(kv *structure.MigrateKV, ignore *bool) error {
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
	Addr   string
	logger *gifts.Logger
	conn   *rpc.Client
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

// Migrate copies the specified block to the destination Storage node
func (s *RPCStorage) Migrate(kv *structure.MigrateKV) error {
	...
}

// Unset deletes the data associated with the block's ID
func (s *RPCStorage) Unset(id string, ignore *bool) error {
	...
}
```