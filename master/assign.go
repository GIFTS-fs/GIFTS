package master

import (
	"math/rand"
	"strconv"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// nextStorage to be assigned for a new block,
// use CLOCK algorithm to simulate LRU with minimum overhead
func (m *Master) nextStorage() (s *storage.RPCStorage) {
	m.createClockHand, s = m.createClockHand+1, m.storages[m.createClockHand]

	// comparision is much cheaper than modular
	if m.createClockHand == len(m.storages) {
		m.createClockHand = 0
	}

	return
}

// makeAssignment for the request, assume all argumetns are valid to the best knowledge of the caller
func (m *Master) makeAssignment(req *structure.FileCreateReq) (assignments []structure.BlockAssign) {
	// WARN: SHOULD NOT HAVE TYPE CASTING,
	// its safety is based on the MaxRfactor is not larger than the overflow number
	nReplica := int(req.Rfactor)
	if nReplica > len(m.storages) {
		nReplica = len(m.storages)
	}

	nBlocks := gifts.NBlocks(req.Fsize)
	assignments = make([]structure.BlockAssign, nBlocks)

	// Policy 1: Random
	// TODO

	// Policy 2: LRU
	// TODO

	// Policy 3: CLOCK
	for i := range assignments {
		assignments[i].BlockID = req.Fname + strconv.FormatInt(int64(i), 10)
		for j := 0; j < nReplica; j++ {
			// uniqueness of each replica is ensured by
			// the if check above, that ensures nReplica
			// is at most the number of storages
			assignments[i].Replicas = append(assignments[i].Replicas, m.nextStorage().Addr)
		}
	}

	return
}

// pickReadReplica for the file, return value has at least one conn,
// TODO: need to check the error of 0 size replica???
func (m *Master) pickReadReplica(fm *fMeta) (assignment []structure.BlockAssign) {
	// Policy 1: (badly) random pick one
	pick := rand.New(rand.NewSource(int64(fm.fSize))).Intn(len(fm.assignments))
	assignment[0] = fm.assignments[pick]

	// Policy 2: LRU
	// TODO

	// Policy 3: CLOCK page replacement algorithm
	// TODO

	return
}
