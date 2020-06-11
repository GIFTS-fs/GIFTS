package master

import (
	"math/rand"

	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/policy"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// populateLookupTable for placement policy 2,
// can potentially be called multiple times when the storage change in the future (?)
func (m *Master) populateLookupTable(names []string) {
	m.placementEntry = algorithm.PopulateLookupTable(m.config.MaglevHashingMultipler, len(names), names)
	m.placementEntryLen = len(m.placementEntry)
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

// WIP: replace all reference to the following 2 functions with
// generic functions above, and work on line 107

// beg++ end
// caller's responibility to check if the list is used up
func (m *Master) nextReplicaBlockRR(fb *fileBlock) (s *storeMeta) {
	s, fb.clockBeg = m.storages[fb.clockBeg], clockTick(fb.clockBeg, m.nStorage, 1)
	return
}

// beg end++
// no correctness guaranteed if called with 0 replicas (break the whole algorithm)
func (m *Master) removeReplicaBlockRR(fb *fileBlock) (s *storeMeta) {
	s, fb.clockEnd = m.storages[fb.clockEnd], clockTick(fb.clockEnd, m.nStorage, 1)
	return
}

// // nextCreateStoragePermu to be assigned for a new block,
// // for placement policy 2: permutation.
// // see nextCreateStorageRR for detail about idx and nextIdx
// func (m *Master) nextCreateStoragePermu() (s *storeMeta, idx, nextIdx int) {
// 	idx, m.createHandPermu = m.placementEntry[m.createHandPermu], clockTick(m.createHandPermu, m.placementEntryLen, 1)
// 	s = m.storages[idx]
// 	nextIdx = clockTick(idx, m.nStorage, 1)
// 	return
// }

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
		assignments[i] = newFileBlock(bID)
		blockAssignments[i].BlockID = bID
	}

	// no replica, no need to consult policy
	if nReplica == 0 {
		return
	}

	switch m.config.BlockPlacementPolicy {
	case policy.BlockPlacementPolicyPermutation:
		// New block placement policy 2: Permutation

	default: // use RR as default
		// New block placement policy 1: Round-robin

		// this variation of policy moves the pointer
		// for each replica of each block.
		// (in the for loop, the clockTick() call)

		// another variation (not implemented)
		// can simply move the pointer for each block.
		// (clockTick(1) for each block)

		handIdx := m.touchCreateHand(nBlocks * nReplica)

		for i := range assignments {
			assignments[i].clockEnd = handIdx
			assignments[i].clockBeg = handIdx
			handIdx = clockTick(handIdx, m.nStorage, nReplica)

			for j := 0; j < nReplica; j++ {
				store := m.nextReplicaBlockRR(assignments[i])
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
