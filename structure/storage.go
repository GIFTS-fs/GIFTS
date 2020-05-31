package structure

import gifts "github.com/GIFTS-fs/GIFTS"

// BlockKV is the request type of Storage.Set()
type BlockKV struct {
	ID   string
	Data gifts.Block
}

// ReplicateKV is the request type of Storage.Replicate()
type ReplicateKV struct {
	ID   string
	Dest string
}
