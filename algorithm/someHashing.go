package algorithm

import (
	"hash/crc32"
	"hash/fnv"
)

// just 2 random hashing functions from nowhere

// HashingFnvTwice uses fnv32a and do it 2 times
func HashingFnvTwice(s string) int64 {
	h := fnv.New32a()
	h.Write([]byte(s))

	// manual avalanche
	firstH := h.Sum(nil)
	h.Write(firstH)

	return int64(h.Sum32())
}

// HashingCrc32 uses IEEE crc32
func HashingCrc32(s string) int64 {
	h := crc32.NewIEEE()
	h.Write([]byte(s))
	return int64(h.Sum32())
}
