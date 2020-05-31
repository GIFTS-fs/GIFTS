package master

// detectUnbalance based on the policy,
// return a slice of fMeta that are considered unbalanced
func (m *Master) detectUnbalance() (unbalanced []*fMeta) {
	m.trafficLock.Lock()
	// currentMedian must be read-only after the critical section
	currentMedian := m.trafficMedian.Median()
	m.trafficLock.Unlock()

	m.fMap.Range(func(key interface{}, value interface{}) bool {
		fm := value.(*fMeta)

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

type enlistment struct {
	replica *storageMeta
	blockID string
}

// enlistNewReplicas for file fm, returns a list of enlistment
func (m *Master) enlistNewReplicas(fm *fMeta) (enlistments []*enlistment) {
	panic(1)
}

// replicateFile fm to newReplica
func (m *Master) replicateFile(fm *fMeta, newReplica *storageMeta) {

}

// periodically check the load status
func (m *Master) balance() {
	unbalanced := m.detectUnbalance()

	for _, ub := range unbalanced {
		_ = ub
	}

}
