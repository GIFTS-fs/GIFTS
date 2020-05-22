// Package generate generates random bytes
package generate

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"time"
)

// Generate is a wrapper for rand.Rand.
// It saves us from the hassle of calling current system time.
type Generate struct {
	rand *rand.Rand
}

// NewGenerate is the constructor for Gernerate
func NewGenerate() *Generate {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, time.Now().UnixNano())
	h.Write(h.Sum(nil))
	return &Generate{rand: rand.New(rand.NewSource(int64(h.Sum64())))}
}

// Read populates the whole buf with random bytes
func (g *Generate) Read(buf []byte) (int, error) {
	return g.rand.Read(buf)
}
