package master

// detectUnbalance based on the policy,
// return a slice of fMeta that are considered unbalanced
// and can be balanced
func (m *Master) detectUnbalance() (toBalance []*fileMeta) {
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
			toBalance = append(toBalance, fm)
		}

		return true
	})

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

	toBalance := m.detectUnbalance()

	for _, ub := range toBalance {
		_ = ub
		ub.nReplica++
		// TODO: update assignments.Replica to include the new enlistment
	}

}
