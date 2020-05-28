package master

// periodically check the load status
func (m *Master) balance() {
	m.fMap.Range(func(key interface{}, value interface{}) bool {
		return false
	})
}
