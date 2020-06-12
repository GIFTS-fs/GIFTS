package master

import (
	"math/rand"

	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/policy"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// populateLookupTable for block placement policy 2,
// can potentially be called multiple times when the storage change in the future (?)
func (m *Master) populateLookupTable(names []string) {
	m.placementEntry = algorithm.PopulateLookupTable(m.config.MaglevHashingMultipler, len(names), names)
	m.placementEntryLen = len(m.placementEntry)
}

// buildReplicaPermuTable for replica placement policy 2,
// can potentially be called multiple times when the storage change in the future (?)
func (m *Master) buildReplicaPermuTable() {
	// init [1...n]
	storageList := make([]int, m.nStorage)
	for i := range storageList {
		storageList[i] = i
	}

	// make a 2D array of random permutations of [1...n]
	m.replicaPermu = make([][]int, m.config.ReplicaPlacementPermuTableSize)
	for i := range m.replicaPermu {
		rand.Shuffle(m.nStorage, func(i, j int) {
			storageList[i], storageList[j] = storageList[j], storageList[i]
		})
		m.replicaPermu[i] = make([]int, m.nStorage)
		copy(m.replicaPermu[i], storageList)
	}
}

// touchCreateHand to get current index and move it by n (blocks*replicas).
// idx is the index of the backend picked.
// It works for policy 1 rr and policy 2 permu.
func (m *Master) touchCreateHand(n int) int {
	m.createHandLock.Lock()
	defer m.createHandLock.Unlock()
	return m.touchCreateHandUnit(n)
}

func (m *Master) touchCreateHandUnitRR(n int) (ret int) {
	ret, m.createHandRR = m.createHandRR, clockTick(m.createHandRR, m.nStorage, n)
	return
}

func (m *Master) touchCreateHandUnitPermu(n int) (ret int) {
	ret, m.createHandPermu = m.createHandPermu, clockTick(m.createHandPermu, m.nStorage, n)
	return
}

func (m *Master) nextReplicaOf(fb *fileBlock) (s *storeMeta) {
	return m.nextReplicaOfUnit(fb)
}

func (m *Master) removeReplicaOf(fb *fileBlock) (s *storeMeta) {
	return m.removeReplicaOfUnit(fb)
}

/*
 * Note on nextRR and removeRR:
 * With only 2 pointers, cannot tell if full and empty
 * But since there is no need for calling Next on file with 0 rFactor
 * next is fine with the simple check;
 * remove must be called after making sure there is at least one replica
 */

// beg++ end
// caller's responibility to check if the list is used up
func (m *Master) nextReplicaOfUnitRR(fb *fileBlock) (s *storeMeta) {
	s, fb.clockBeg = m.storages[fb.clockBeg], clockTick(fb.clockBeg, m.nStorage, 1)
	return
}

// beg end++
// no correctness guaranteed if called with 0 replicas (break the whole algorithm)
func (m *Master) removeReplicaOfUnitRR(fb *fileBlock) (s *storeMeta) {
	s, fb.clockEnd = m.storages[fb.clockEnd], clockTick(fb.clockEnd, m.nStorage, 1)
	return
}

// beg++ end
func (m *Master) nextReplicaOfUnitPermu(fb *fileBlock) (s *storeMeta) {
	s, fb.clockBeg = m.storages[m.replicaPermu[fb.permuIndex][fb.clockBeg]], clockTick(fb.clockBeg, m.nStorage, 1)
	return
}

// beg end++
func (m *Master) removeReplicaOfUnitPermu(fb *fileBlock) (s *storeMeta) {
	s, fb.clockEnd = m.storages[m.replicaPermu[fb.permuIndex][fb.clockEnd]], clockTick(fb.clockEnd, m.nStorage, 1)
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

	for i := range assignments {
		bID := nameBlock(req.Fname, i)
		assignments[i] = newFileBlock(m.config, bID)
		blockAssignments[i].BlockID = bID
	}

	// no replica, no need to consult policy
	if nReplica == 0 {
		return
	}

	switch m.config.BlockPlacementPolicy {
	case policy.BlockPlacementPolicyPermutation:
		// New block placement policy 2: Permutation
		handIdx := m.touchCreateHand(nBlocks)

		for i := range assignments {
			// m.Logger.Printf("THRASHING1 Permu block %q is assigned to %v", assignments[i].BlockID, m.placementEntry[handIdx])

			assignments[i].clockEnd = m.placementEntry[handIdx]
			assignments[i].clockBeg = m.placementEntry[handIdx]
			handIdx = clockTick(handIdx, m.placementEntryLen, 1)

			for j := 0; j < nReplica; j++ {
				store := m.nextReplicaOf(assignments[i])
				// m.Logger.Printf("    THRASHING1 block %q replica %v is assigned to %q", assignments[i].BlockID, j, store.Addr)
				assignments[i].addReplica(store)
				blockAssignments[i].Replicas = append(blockAssignments[i].Replicas, store.Addr)
			}
		}

	default: // use RR as default
		// New block placement policy 1: Round-robin

		// legacy variation of policy moves the pointer
		// for each replica of each block.
		// (in the for loop, the clockTick() call)

		// new variation
		// simply move the pointer for each block.
		// (clockTick(1) for each block)

		var amount, subamount int
		if m.config.ReplicaPlacementPolicy == policy.ReplicaPlacementPolicyRR {
			// For legacy code, change in future
			amount = nBlocks * nReplica
			subamount = nReplica
		} else {
			amount = nBlocks
			subamount = 1
		}

		handIdx := m.touchCreateHand(amount)

		for i := range assignments {
			// m.Logger.Printf("THRASHING1 RR block %q is assigned to %v", assignments[i].BlockID, handIdx)
			assignments[i].clockEnd = handIdx
			assignments[i].clockBeg = handIdx
			handIdx = clockTick(handIdx, m.nStorage, subamount)

			for j := 0; j < nReplica; j++ {
				store := m.nextReplicaOf(assignments[i])
				// m.Logger.Printf("    THRASHING1 block %q replica %v is assigned to %q", assignments[i].BlockID, j, store.Addr)
				assignments[i].addReplica(store)
				blockAssignments[i].Replicas = append(blockAssignments[i].Replicas, store.Addr)
			}
		}
	}

	return
}

// pickReplica for the requested blockID
func (m *Master) pickReplica(fb *fileBlock) (picked *storeMeta, pickedAddr string) {
	// Pick block policy 1: (badly) randomly pick one
	pick := rand.Intn(fb.nReplicas())
	picked = fb.replicas[pick]
	pickedAddr = fb.replicas[pick].Addr

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
