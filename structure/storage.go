package structure

import gifts "github.com/GIFTS-fs/GIFTS"

// BlockKV is the request type of Storage.Set()
type BlockKV struct {
	ID   string
	Data gifts.Block
}
