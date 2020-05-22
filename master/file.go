package master

import "github.com/GIFTS-fs/GIFTS/structure"

type fMeta struct {
	fb *structure.FileBlocks
	// uniqueHits uint64 // a potential way of record for balance
}

// fCreate tries to map fb to fname, return loaded=true if already exists.
// either because a concurrent create or already exists.
func (m *Master) fCreate(fname string, fb *structure.FileBlocks) (fm *fMeta, loaded bool) {
	fi, loaded := m.fMap.LoadOrStore(fname, fb)
	fm = fi.(*fMeta)
	return
}

func (m *Master) fLookup(fname string) (*structure.FileBlocks, bool) {
	fm, found := m.fMap.Load(fname)
	if !found {
		return nil, false
	}

	return fm.(*fMeta).fb, true
}

func (m *Master) fExist(fname string) bool {
	_, exist := m.fLookup(fname)
	return exist
}
