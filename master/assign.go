package master

import (
	"math/rand"

	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/policy"
	"github.com/GIFTS-fs/GIFTS/structure"
)

func (m *Master) touchCreateHand(n int) int {
	m.createHandLock.Lock()
	defer m.createHandLock.Unlock()
	return m.touchCreateHandClosure(n)
}

// nextCreateStorageRR to be assigned for a new block,
// for placement policy 1: round-robin.
// idx is the index of the backend picked,
// nextIdx is the concequtive index of the backend to be picked,
// used for replica block placement purpose
func (m *Master) nextCreateStorageRR() (s *storeMeta, idx, nextIdx int) {
	idx, s, m.createHandRR = m.createHandRR, m.storages[m.createHandRR], clockTick(m.createHandRR, m.nStorage, 1)
	nextIdx = m.createHandRR
	return
}

// populateLookupTable for placement policy 2,
// can potentially be called multiple times when the storage change in the future (?)
func (m *Master) populateLookupTable(names []string) {
	m.placementEntry = algorithm.PopulateLookupTable(m.config.MaglevHashingMultipler, len(names), names)
	m.placementEntryLen = len(m.placementEntry)
}

// nextCreateStoragePermu to be assigned for a new block,
// for placement policy 2: permutation.
// see nextCreateStorageRR for detail about idx and nextIdx
func (m *Master) nextCreateStoragePermu() (s *storeMeta, idx, nextIdx int) {
	idx, m.createHandPermu = m.placementEntry[m.createHandPermu], clockTick(m.createHandPermu, m.placementEntryLen, 1)
	s = m.storages[idx]
	nextIdx = clockTick(idx, m.nStorage, 1)
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

	switch m.config.BlockPlacementPolicy {
	case policy.BlockPlacementPolicyPermutation:
		panic("Not implemented")
	default: // use RR as default
		// case policy.BlockPlacementPolicyRR:
		// New block placement policy 1: Round-robin
		for i := range assignments {
			bID := nameBlock(req.Fname, i)
			assignments[i] = newFileBlock(bID)
			blockAssignments[i].BlockID = bID

			if nReplica == 0 {
				continue
			}

			// WIP: calculate total increment on the createHand
			// call touch(increment)
			// then everything later is local to this thread

			// get the beginning index of this block
			_, idx, _ := m.nextCreateStorageRR()

			assignments[i].clockEnd = idx
			assignments[i].clockBeg = idx

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
