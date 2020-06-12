package master

import (
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/policy"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// fileBlock keeps track of assignment information per block
type fileBlock struct {
	BlockID string

	// slice of all replicas
	replicas []*storeMeta
	// addr -> *storeMeta
	rMap map[string]*storeMeta

	// Replica block placement policy 1: Clock
	// [clockEnd ... clockBeg->]
	clockBeg int // first replica available to use
	clockEnd int // first replica assigned

	// Replica block placement policy 2: permutation
	permuIndex int
	// reuse clockBeg and clockEnd
}

func newFileBlock(conf *config.Config, bID string) *fileBlock {
	fb := &fileBlock{BlockID: bID, rMap: make(map[string]*storeMeta)}
	// TODO: improve this, find better way to pass arguments
	if conf.ReplicaPlacementPolicy == policy.ReplicaPlacementPolicyPermutation {
		fb.permuIndex = int(algorithm.HashingFnvTwice(bID)) % conf.ReplicaPlacementPermuTableSize
	}
	return fb
}

func (fb *fileBlock) addReplica(r *storeMeta) {
	fb.replicas = append(fb.replicas, r)
	fb.rMap[r.Addr] = r
}

func (fb *fileBlock) rmReplica(r *storeMeta) {
	if !fb.hasReplica(r) {
		return
	}

	for i := range fb.replicas {
		// can we compare pointer address???
		// may be faster but very insecure
		if fb.replicas[i].Addr == r.Addr {
			fb.replicas[i] = fb.replicas[len(fb.replicas)-1]
			fb.replicas = fb.replicas[:len(fb.replicas)-1]
			break
		}
	}
	delete(fb.rMap, r.Addr)
}

func (fb *fileBlock) hasReplicaAddr(addr string) bool {
	_, ok := fb.rMap[addr]
	return ok
}

func (fb *fileBlock) hasReplica(r *storeMeta) bool {
	return fb.hasReplicaAddr(r.Addr)
}

// nBlocks number of blocks stored for this file
func (fb *fileBlock) nReplicas() int {
	return len(fb.replicas)
}

type fileMeta struct {
	// const fields
	fName       string // file name
	fSize       int    // size of the file, to handle padding
	nBlocks     int    // save the compution
	rFactor     uint   // how important the user thinks this file is
	initialized bool   // if the initialization is complete

	nReplica int          // real number of replica
	blocks   []*fileBlock // Nodes[i] stores the addr of DataNode with ith Block, where len(Replicas) >= 1

	trafficLock    sync.Mutex
	trafficCounter *algorithm.DecayCounter // expontionally decaying read counter
}

// fCreate tries to create a new fMeta for fname, return loaded=true if already exists.
// either because a concurrent create or already exists.
// Acts like an once constructor for a fname.
// WARN: loaded=true does not mean the other thread finished the initialization
// TODO: may change "loaded" to "success" or even error to indicate more failure
func (m *Master) fCreate(fname string, req *structure.FileCreateReq) (blockAssignments []structure.BlockAssign, loaded bool) {
	fi, loaded := m.fMap.LoadOrStore(fname, &fileMeta{})

	fm := fi.(*fileMeta)
	if loaded {
		return
	}

	// This is the "constructor" of fileMeta
	// Only initialize the data once globally
	nBlocks := gifts.NBlocks(m.config.GiftsBlockSize, req.Fsize)
	fm.fName = fname
	fm.fSize = req.Fsize
	fm.nBlocks = nBlocks
	fm.rFactor = req.Rfactor
	fm.blocks, fm.nReplica, blockAssignments = m.createAssignments(req, nBlocks)
	fm.trafficCounter = algorithm.NewDecayCounter(m.config.TrafficDecayCounterHalfLife)
	fm.trafficCounter.Reset()

	m.trafficLock.Lock()
	defer m.trafficLock.Unlock()
	m.trafficMedian.Add(fm.trafficCounter.GetRaw()) // Add(0)

	fm.initialized = true

	return
}

// return fm and true if found and initialized
func (m *Master) fLookup(fname string) (*fileMeta, bool) {
	fm, found := m.fMap.Load(fname)
	if !found || !fm.(*fileMeta).initialized {
		return nil, false
	}

	return fm.(*fileMeta), true
}

// return true if found and initialized
func (m *Master) fExist(fname string) bool {
	_, exist := m.fLookup(fname)
	return exist
}
