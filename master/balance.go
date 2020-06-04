package master

import "github.com/GIFTS-fs/GIFTS/structure"

// detectUnbalance based on the policy,
// return a slice of fMeta that are considered unbalanced
// and can be balanced
func (m *Master) detectUnbalance() (toUp, toDown []*fileMeta) {
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

		// Unbalance detection policy 1: reference count / number of replications > median reference count / number of storage
		if tempature/float64(fm.nReplica) > currentMedian/float64(m.nStorage) {
			m.Logger.Printf("balance Policy 1 caught toUp: %v", fm)
			toUp = append(toUp, fm)
		}

		// WARN: bad, unnecessary type casting
		if fm.nReplica > int(fm.rFactor) && tempature/float64(fm.nReplica) < currentMedian/float64(m.nStorage) {
			m.Logger.Printf("balance Policy 1 caught toDown: %v", fm)
			toDown = append(toDown, fm)
		}

		return true
	})

	return
}

// enlistment asks the src to store a copy of blockID to dst
type enlistment struct {
	blockID   string
	fileBlock *fileBlock
	src       *storeMeta
	dst       *storeMeta
}

// enlistNewReplicas for file fm, returns a list of enlistment.
// this list may contain duplicated storage, storage that already
// stores the file, storage that already stores the block etc.
func (m *Master) enlistNewReplicas(fm *fileMeta) (enlistments []*enlistment) {
	if fm.nReplica == m.nStorage {
		return nil
	}

	// Replica Block Placement Policy 1: Clock
	for _, block := range fm.blocks {
		enlistment := &enlistment{blockID: block.BlockID, fileBlock: block}
		enlistment.src, _ = m.pickReplica(block)
		enlistment.dst = m.clockNextReplicaBlock(block)
		enlistments = append(enlistments, enlistment)
	}

	// Replica Block Placement Policy 2: lowest load

	return
}

// replicateEnlistment copies blockID from src to dst
func (m *Master) replicateEnlistment(enlistment *enlistment) error {
	sm, _ := m.sMap.Load(enlistment.src)
	return sm.(*storeMeta).rpc.Replicate(&structure.ReplicateKV{ID: enlistment.blockID, Dest: enlistment.dst.Addr})
}

// periodically check the load status
func (m *Master) balance() {
	m.isBalancingLock.Lock()
	if m.isBalancing {
		// last balance thread was still running
		m.isBalancingLock.Unlock()
		return
	}
	defer func() {
		defer m.isBalancingLock.Unlock()
		m.isBalancingLock.Lock()
		m.isBalancing = false
	}()
	m.isBalancing = true
	m.isBalancingLock.Unlock()

	toUp, toDown := m.detectUnbalance()

	// TODO: both enlist and innter loop updates metadata for fileBlocks and fileMeta
	// need a way to keep the updates atomic (if failure, then no change)

	for _, f := range toUp {
		enlistments := m.enlistNewReplicas(f)
		for _, enlistment := range enlistments {
			if err := m.replicateEnlistment(enlistment); err != nil {
				// TODO: gracefully and atomically handle the error
				m.Logger.Printf("balance() failed to replicateEnlistment(%v): %v", enlistment, err)
				return
			}
			enlistment.fileBlock.addReplica(enlistment.dst)
		}
		f.nReplica++
	}

	// TODO: finish decrease
	_ = toDown
}
