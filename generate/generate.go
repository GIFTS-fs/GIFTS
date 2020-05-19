// Pakcage generate generates random bytes
package generate

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"time"
)

type Generate struct {
	rand *rand.Rand
}

func NewGenerate() *Generate {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, time.Now().UnixNano())
	h.Write(h.Sum(nil))
	return &Generate{rand: rand.New(rand.NewSource(int64(h.Sum64())))}
}

func (g *Generate) Read(buf []byte) (int, error) {
	return g.rand.Read(buf)
}
