package master

import (
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	decayCounterHalfLife = 5000
)

type fMeta struct {
	fSize       int                     // size of the file, to handle padding
	nBlocks     int                     // save the compution
	rFactor     uint                    // how important the user thinks this file is
	nReplica    int                     // real number of replica
	assignments []structure.BlockAssign // Nodes[i] stores the addr of DataNode with ith Block, where len(Replicas) >= 1

	nRead          uint64 // expontionally decaying read counter
	trafficLock    sync.Mutex
	trafficCounter *algorithm.DecayCounter
}

// fCreate tries to create a new fMeta for fname, return loaded=true if already exists.
// either because a concurrent create or already exists.
// WARN: loaded=true does not mean the other thread finished the initialization
func (m *Master) fCreate(fname string, req *structure.FileCreateReq) (fm *fMeta, loaded bool) {
	fi, loaded := m.fMap.LoadOrStore(fname, &fMeta{})

	fm = fi.(*fMeta)
	if loaded {
		return
	}

	// Only set the data once globally
	nBlocks := gifts.NBlocks(req.Fsize)
	fm.fSize = req.Fsize
	fm.nBlocks = nBlocks
	fm.rFactor = req.Rfactor
	fm.assignments, fm.nReplica = m.makeAssignment(req, nBlocks)
	fm.nRead = 0
	fm.trafficCounter = algorithm.NewDecayCounter(decayCounterHalfLife)
	fm.trafficCounter.Reset()

	defer m.trafficLock.Unlock()
	m.trafficLock.Lock()
	m.trafficMedian.Add(fm.trafficCounter.GetRaw())

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
