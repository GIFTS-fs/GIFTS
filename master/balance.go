package master

import "github.com/GIFTS-fs/GIFTS/structure"

// enlistment asks the src to store a copy of blockID to dst
type enlistment struct {
	blockID   string
	fileBlock *fileBlock
	src       *storeMeta
	dst       *storeMeta
	// TODO: the update to be done on the data structure if success, make it atomatic
	// update func()
}

// replicateEnlistment copies blockID from src to dst
func (m *Master) replicateEnlistment(enlistment *enlistment) error {
	sm, _ := m.sMap.Load(enlistment.src.Addr)
	return sm.(*storeMeta).rpc.Replicate(&structure.ReplicateKV{ID: enlistment.blockID, Dest: enlistment.dst.Addr})
}

// dereplicateEnlistment removes blockID from dst (src can be nil)
func (m *Master) dereplicateEnlistment(enlistment *enlistment) error {
	sm, _ := m.sMap.Load(enlistment.dst.Addr)
	var ignore bool
	return sm.(*storeMeta).rpc.Unset(enlistment.blockID, &ignore)
}

// detectUnbalance based on the policy,
// return 2 slices of fMeta that
// are considered unbalanced and can be balanced
func (m *Master) detectUnbalance() (toUp, toDown []*fileMeta) {
	m.trafficLock.Lock()
	// currentMedian must be read-only after the critical section
	currentMedian := m.trafficMedian.Median()
	m.trafficLock.Unlock()

	// m.Logger.Printf("DEBUG: currentMedian: %v\n", currentMedian)

	m.fMap.Range(func(key interface{}, value interface{}) bool {
		fm := value.(*fileMeta)

		fm.trafficLock.Lock()
		prev, tempature := fm.trafficCounter.GetRaw(), fm.trafficCounter.Get()
		fm.trafficLock.Unlock()

		// TODO: figure out better ways to put the critical sections
		// and data read (currentMedian is the median before the for loop currently)
		m.trafficLock.Lock()
		m.trafficMedian.Update(prev, tempature)
		m.trafficLock.Unlock()

		// m.Logger.Printf("DEBUG: temperate for file %q: %v\n", fm.fName, tempature)

		// Assume only balance() will change nReplica and nStorage
		// no locks around those 2 fields
		// since only one balance() thread running

		// Assume rFactor is not needed for toUP (no new storage can be dynamiclly added in first phase)

		// m.Logger.Printf("DEBUG Checking %q: temperature: %v, nReplica: %v, rFactor: %v\n", fm.fName, tempature, fm.nReplica, fm.rFactor)

		// Unbalance detection policy 1: reference count / number of replications > median reference count / number of storage
		threshold := currentMedian / float64(m.nStorage)

		if fm.nReplica < m.nStorage && tempature/float64(fm.nReplica) > threshold {
			// m.Logger.Printf("DEBUG balance Policy 1 caught toUp: %v", fm)
			toUp = append(toUp, fm)
		}

		// WARN: bad, unnecessary type casting
		if fm.nReplica > int(fm.rFactor) && tempature/float64(fm.nReplica) < threshold {
			// m.Logger.Printf("DEBUG balance Policy 1 caught toDown: %v", fm)
			toDown = append(toDown, fm)
		}

		// Unbalance detection policy 2: approximate average load of the system
		// TODO

		return true
	})

	return
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

// enlistNewReplicas for file fm, returns a list of enlistment.
// this list may contain duplicated storage, storage that already
// stores the file, storage that already stores the block etc.
func (m *Master) enlistNewReplicas(fm *fileMeta) (enlistments []*enlistment) {
	if fm.nReplica == m.nStorage {
		return nil
	}

	enlistments = make([]*enlistment, fm.nBlocks)

	// Replica Block Placement Policy 1: RR
	// must use in pair
	for i, block := range fm.blocks {
		enlistment := &enlistment{blockID: block.BlockID, fileBlock: block}
		enlistment.src, _ = m.pickReplica(block)
		enlistment.dst = m.nextReplicaBlockRR(block)
		enlistments[i] = enlistment
	}

	return
}

// dischargeReplicas for file fm, return a slice of enlistments for each block
func (m *Master) dischargeReplicas(fm *fileMeta) (enlistments []*enlistment) {
	if fm.nReplica <= int(fm.rFactor) {
		return nil
	}

	enlistments = make([]*enlistment, fm.nBlocks)

	// Replica Block Placement Policy 1: Clock
	// must use in pair
	for i, block := range fm.blocks {
		enlistment := &enlistment{blockID: block.BlockID, fileBlock: block}
		enlistment.dst = m.removeReplicaBlockRR(block)
		enlistments[i] = enlistment
	}

	return
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

	// m.Logger.Printf("DEBUG: Start balancing!\n")

	toUp, toDown := m.detectUnbalance()

	// m.Logger.Printf("DEBUG: Detected toUP: %v!\n", toUp)
	// m.Logger.Printf("DEBUG: Detected toDown: %v!\n", toDown)

	// TODO: support multiple increment/decrement

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

	for _, f := range toDown {
		enlistments := m.dischargeReplicas(f)
		for _, enlistment := range enlistments {
			if err := m.dereplicateEnlistment(enlistment); err != nil {
				// TODO: gracefully and atomically handle the error
				m.Logger.Printf("balance() failed to dereplicateEnlistment(%v): %v", enlistment, err)
				return
			}
			enlistment.fileBlock.rmReplica(enlistment.dst)
		}
		f.nReplica--
	}
}
