package master

// detectUnbalance based on the policy,
// return a slice of fMeta that are considered unbalanced
func (m *Master) detectUnbalance() (unbalanced []*fileMeta) {
	m.trafficLock.Lock()
	// currentMedian must be read-only after the critical section
	currentMedian := m.trafficMedian.Median()
	m.trafficLock.Unlock()

	m.fMap.Range(func(key interface{}, value interface{}) bool {
		fm := value.(*fileMeta)

		fm.trafficLock.Lock()
		tempature := fm.trafficCounter.Get()
		fm.trafficLock.Unlock()

		// cannot replicate more
		if fm.nReplica == m.nStorage {
			return true
		}

		// Assume only balance() will change nReplica and nStorage
		// no locks around those 2 fields

		// Policy 1: reference count / number of replications > median reference count / number of storage
		if tempature/float64(fm.nReplica) > currentMedian/float64(m.nStorage) {
			m.logger.Printf("balance Policy 1 caught: %v", fm)
			unbalanced = append(unbalanced, fm)
		}

		return true
	})

	return
}

// nextBalanceStorage to try based on Clock algorithm
func (m *Master) nextBalanceStorage() (s *storeMeta) {
	var addr string
	m.balanceClockHand, addr = m.balanceClockHand+1, m.storages[m.balanceClockHand]

	si, _ := m.sMap.Load(addr)
	s = si.(*storeMeta)

	if m.balanceClockHand == m.nStorage {
		m.balanceClockHand = 0
	}

	return
}

// enlistment asks the src to store a copy of blockID to dst
type enlistment struct {
	blockID string
	src     *storeMeta
	dst     *storeMeta
}

// enlistNewReplicas for file fm, returns a list of enlistment.
// this list may contain duplicated storage, storage that already
// stores the file, storage that already stores the block etc.
func (m *Master) enlistNewReplicas(fm *fileMeta) (enlistments []*enlistment) {
	// V1: extremely slow, might loop through all the storage multiple times

	assigned, toAssign, nStorage := 0, fm.nBlocks, m.nStorage
	used := make(map[string]*enlistment)
	var skipped []*storeMeta

	// First round, find storage that does not have this file at all
	for walk := 0; walk < nStorage && assigned < toAssign; walk++ {
		// Provisonal policy: Clock until find enough replicas
		candidate := m.nextBalanceStorage()

		// check if already storing the file
		candidate.assignmentLock.Lock()
		_, found := candidate.storedFiles[fm.fName]
		candidate.assignmentLock.Unlock()
		if found {
			// // Since we always prefer assign to a storage that
			// // does not have any block related to a file,
			// // as soon as there is one storage has multiple blocks,
			// // it means all storages have multiple blocks
			// if ab.nBlocks() > 1 {
			// 	skipped = m.storages
			// }
			skipped = append(skipped, candidate)
			continue
		}

		bID := fm.assignments[assigned].BlockID
		enlistment := &enlistment{blockID: bID}
		used[candidate.Addr] = enlistment
		enlistments = append(enlistments, enlistment)

		assigned++
	}

	if assigned == toAssign {
		return
	}

	// Reaching this line means:
	// walked the whole storages
	// have blocks unassigned (toAssign - assigned)
	// skipped have all storages that are not used

	// collect all unassigned block in a map
	// for ; assigned < toAssign; assigned++ {
	// }

	nSkipped := len(skipped)

	// Input: a list of B and S
	// Output: a list of (B_i, S_j) such that S_j.storedFiles[fm.fName].blockIDs[B_i] = false

	// Second round: find storage that does not have an unassigned blockID from skipped
	for walk := 0; walk < nSkipped && assigned < toAssign; walk++ {
	}

	// Third round: find storage that may have the blockID

	return
}

// replicateFile fm to newReplica
func (m *Master) replicateFile(fm *fileMeta, newReplica *storeMeta) {

}

// periodically check the load status
func (m *Master) balance() {
	// no need to sync this variable
	// since this function is invoked by a ticker
	if m.isBalancing {
		// last balance thread was still running
		return
	}
	defer func() { m.isBalancing = false }()
	m.isBalancing = true

	unbalanced := m.detectUnbalance()

	for _, ub := range unbalanced {
		_ = ub
		ub.nReplica++
		// TODO: update assignments.Replica to include the new enlistment
	}

}
