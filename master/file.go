package master

import "github.com/GIFTS-fs/GIFTS/structure"

type fMeta struct {
	fSize       int                     // size of the file, to handle padding
	nBlocks     int                     // save the compution
	rFactor     uint                    // how important the user thinks this file is
	assignments []structure.BlockAssign // Nodes[i] stores the addr of DataNode with ith Block, where len(Replicas) >= 1

	nRead uint64 // expontionally decaying read counter
}

// fCreate tries to map fb to fname, return loaded=true if already exists.
// either because a concurrent create or already exists.
func (m *Master) fCreate(fname string, newFm *fMeta) (fm *fMeta, loaded bool) {
	fi, loaded := m.fMap.LoadOrStore(fname, newFm)
	fm = fi.(*fMeta)
	return
}

func (m *Master) fLookup(fname string) (*fMeta, bool) {
	fm, found := m.fMap.Load(fname)
	if !found {
		return nil, false
	}

	return fm.(*fMeta), true
}

func (m *Master) fExist(fname string) bool {
	_, exist := m.fLookup(fname)
	return exist
}
