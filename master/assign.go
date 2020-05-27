package master

import (
	"math/rand"
	"strconv"
	"time"

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

// makeAssignment for the request, assume all arguments are valid to the best knowledge of the caller
func (m *Master) makeAssignment(request *structure.FileCreateReq, nBlocks int) (assignments []structure.BlockAssign) {
	// WARN: SHOULD NOT HAVE TYPE CASTING,
	// its safety is based on the MaxRFactor is not larger than the overflow number
	// DLAD: I'm not a fan of silently changing the user's input.  This should
	// be an error.
	nReplica := int(request.RFactor)
	if nReplica > len(m.storages) {
		nReplica = len(m.storages)
	}

	assignments = make([]structure.BlockAssign, nBlocks)

	// Policy 1: Random
	// TODO

	// Policy 2: LRU
	// TODO

	// Policy 3: CLOCK
	for i := range assignments {
		// WARN: very innocent way to make BlockID
		assignments[i].BlockID = request.FName + strconv.FormatInt(int64(i), 10)
		for j := 0; j < nReplica; j++ {
			// uniqueness of each replica is ensured by
			// the if check above, that ensures nReplica
			// is at most the number of storages
			assignments[i].Replicas = append(assignments[i].Replicas, m.nextStorage().Addr)
		}
	}

	return
}

// pickReadReplica for the file
func (m *Master) pickReadReplica(fm *fMeta) (assignment []structure.BlockAssign) {
	assignment = make([]structure.BlockAssign, fm.nBlocks)

	for i, completeAssignment := range fm.assignments {
		assignment[i].BlockID = completeAssignment.BlockID

		// DLAD: Is there a scenario in which this happens and it's not an
		// error?
		nReplica := len(completeAssignment.Replicas)
		if nReplica <= 0 {
			continue
		}

		// Policy 1: (badly) randomly pick one
		// DLAD: Do we need to reseed the RNG on every call?
		// DLAD: Is there a reason to create a new source instead of using the top-level RNG?
		pick := rand.New(rand.NewSource(int64(fm.fSize) ^ time.Now().UnixNano())).Intn(nReplica)
		assignment[i].Replicas = []string{completeAssignment.Replicas[pick]}

		// Policy 2: LRU
		// TODO

		// Policy 3: CLOCK page replacement algorithm
		// TODO
	}
	return
}
