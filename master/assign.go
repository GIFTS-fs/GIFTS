package master

import (
	"math/rand"

	"github.com/GIFTS-fs/GIFTS/structure"
)

// nextStorage to be assigned for a new block,
// use CLOCK algorithm to simulate LRU with minimum overhead
func (m *Master) nextStorage() (s *storeMeta, idx, nextIdx int) {
	idx, s, m.createClockHand = m.createClockHand, m.storages[m.createClockHand], clockTick(m.createClockHand, m.nStorage)
	nextIdx = m.createClockHand
	return
}

// createAssignments for the request, assume all arguments are valid to the best knowledge of the caller
func (m *Master) createAssignments(req *structure.FileCreateReq, nBlocks int) (assignments []*fileBlock, nReplica int, blockAssignments []structure.BlockAssign) {
	// overflow safety checked by caller
	nReplica = int(req.Rfactor)

	// cannot create more than nStorage number of replicas
	if nReplica > m.nStorage {
		nReplica = m.nStorage
	}

	assignments = make([]*fileBlock, nBlocks)
	blockAssignments = make([]structure.BlockAssign, nBlocks)

	// New block placement policy 1: CLOCK
	for i := range assignments {
		bID := nameBlock(req.Fname, i)
		assignments[i] = newFileBlock(bID)
		blockAssignments[i].BlockID = bID
		for j := 0; j < nReplica; j++ {
			// uniqueness of each replica is ensured by
			// the if check above, that ensures nReplica
			// is at most the number of storages
			store, idx, nextIdx := m.nextStorage()
			assignments[i].addReplica(store)
			blockAssignments[i].Replicas = append(blockAssignments[i].Replicas, store.Addr)

			// see fileBlock{} for invariant
			if j == 0 {
				assignments[i].clockEnd = idx
				assignments[i].clockBeg = nextIdx
			} else {
				assignments[i].clockBeg = nextIdx
			}
		}
	}

	// New block placement policy 2: Least load
	// (sorted by sum(traffic counter) for all files stored, break ties using nBlocks stored etc.)
	// TODO

	return
}

// pickReplica for the requested blockID
func (m *Master) pickReplica(fb *fileBlock) (picked *storeMeta, pickedAddr string) {
	// Pick block policy 1: (badly) randomly pick one
	pick := rand.Intn(fb.nReplicas())
	picked = fb.replicas[pick]
	pickedAddr = fb.replicas[pick].Addr

	// Pick block policy 2: pick the replica with lowest load
	// TODO

	// Pick block policy 3: CLOCK page replacement algorithm
	// TODO

	// Pick block policy 4: pick the replica with closest distance to user
	// TODO (never)

	return
}

// lookupReplicas for the file
func (m *Master) lookupReplicas(fm *fileMeta) (assignment []structure.BlockAssign) {
	assignment = make([]structure.BlockAssign, fm.nBlocks)

	for i, completeAssignment := range fm.blocks {
		assignment[i].BlockID = completeAssignment.BlockID

		nReplica := len(completeAssignment.replicas)
		if nReplica <= 0 {
			continue
		}

		_, pickedAddr := m.pickReplica(completeAssignment)
		assignment[i].Replicas = []string{pickedAddr}

	}
	return
}
