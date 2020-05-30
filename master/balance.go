package master

// periodically check the load status
func (m *Master) balance() {
	var toBalance []*fMeta

	m.trafficLock.Lock()
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

		// Policy 1: reference count / number of replications > median reference count / number of storage
		if tempature/float64(fm.nReplica) > currentMedian/float64(m.nStorage) {
			m.logger.Printf("balance Policy 1 caught: %v", fm)
			toBalance = append(toBalance, fm)
		}

		return true
	})

	if len(toBalance) == 0 {
		return
	}
}
