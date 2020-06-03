package master

import (
	"math/rand"

	"github.com/GIFTS-fs/GIFTS/structure"
)

// nextStorage to be assigned for a new block,
// use CLOCK algorithm to simulate LRU with minimum overhead
func (m *Master) nextStorage() (s *storeMeta, idx, nextIdx int) {
	var addr string
	idx, addr = m.createClockHand, m.storages[m.createClockHand]

	si, _ := m.sMap.Load(addr)
	s = si.(*storeMeta)

	m.createClockHand = clockTick(m.createClockHand, m.nStorage)
	nextIdx = m.createClockHand

	return
}

// makeAssignment for the request, assume all arguments are valid to the best knowledge of the caller
func (m *Master) makeAssignment(req *structure.FileCreateReq, nBlocks int) (assignments []*fileBlock, nReplica int, blockAssignments []structure.BlockAssign) {
	nReplica = int(req.Rfactor)

	// cannot create more than nStorage number of replicas
	if nReplica > m.nStorage {
		nReplica = m.nStorage
	}

	assignments = make([]*fileBlock, nBlocks)
	blockAssignments = make([]structure.BlockAssign, nBlocks)

	// Policy 1: Random
	// Discarded

	// Policy 2: Least load
	// (sorted by sum(traffic counter) for all files stored, break ties using nBlocks stored etc.)
	// TODO

	// Policy 3: CLOCK
	for i := range assignments {
		bID := nameBlock(req.Fname, i)
		assignments[i] = newFileBlock(bID)
		blockAssignments[i].BlockID = bID
		for j := 0; j < nReplica; j++ {
			// uniqueness of each replica is ensured by
			// the if check above, that ensures nReplica
			// is at most the number of storages
			store, idx, nextIdx := m.nextStorage()
			assignments[i].clockEnd = idx
			assignments[i].clockBeg = nextIdx
			assignments[i].replicas = append(assignments[i].replicas, store)
			assignments[i].rMap[store.Addr] = store
			blockAssignments[i].Replicas = append(blockAssignments[i].Replicas, store.Addr)
		}
	}

	return
}

// pickReadReplica for the file
func (m *Master) pickReadReplica(fm *fileMeta) (assignment []structure.BlockAssign) {
	assignment = make([]structure.BlockAssign, fm.nBlocks)

	for i, completeAssignment := range fm.assignments {
		assignment[i].BlockID = completeAssignment.BlockID

		nReplica := len(completeAssignment.replicas)
		if nReplica <= 0 {
			continue
		}

		// Policy 1: (badly) randomly pick one
		pick := rand.Intn(nReplica)
		assignment[i].Replicas = []string{completeAssignment.replicas[pick].Addr}

		// Policy 2: LRU
		// TODO

		// Policy 3: CLOCK page replacement algorithm
		// TODO
	}
	return
}
