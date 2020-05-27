package master

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// MaxRfactor limits the value of rfactor
	MaxRfactor = 256
	// rebalanceIntervalSec
	rebalanceIntervalSec = 10
)

// Master is the master of GIFTS
type Master struct {
	logger *gifts.Logger

	fMap sync.Map

	storages []*storage.RPCStorage
	// storageLoad     sync.Map
	createClockHand int
}

// NewMaster is the constructor for master
func NewMaster(storageAddr []string) *Master {
	m := Master{
		logger:          gifts.NewLogger("Master", "master", true), // PRODUCTION: banish this
		createClockHand: 0,
	}

	for _, addr := range storageAddr {
		m.storages = append(m.storages, storage.NewRPCStorage(addr))
	}

	return &m
}

func (m *Master) background() {
	// TODO: make the interval dynamic?
	tickerRebalance := time.NewTicker(time.Second * rebalanceIntervalSec)
	defer tickerRebalance.Stop()

	for {
		select {
		case <-tickerRebalance.C:
			go m.balance()
		}
	}
}

// ServRPC spawns a thread listen to RPC traffic
func ServRPC(m *Master, addr string) (err error) {
	serv := rpc.NewServer()

	err = serv.RegisterName("Master", m)
	if err != nil {
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	mux := http.NewServeMux()
	mux.Handle(RPCPathMaster, serv)
	go m.background()
	go http.Serve(l, mux)
	return
}

// Create a file: assign replicas for the clients to write
func (m *Master) Create(req *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	if m.fExist(req.Fname) {
		return fmt.Errorf("File %q already exists", req.Fname)
	}

	if req.Rfactor > MaxRfactor {
		return fmt.Errorf("Rfactor %v too large (> %v)", req.Rfactor, MaxRfactor)
	}

	nBlocks := gifts.NBlocks(req.Fsize)
	fm := &fMeta{
		fSize:       req.Fsize,
		nBlocks:     nBlocks,
		rFactor:     req.Rfactor,
		assignments: m.makeAssignment(req, nBlocks),
		nRead:       0,
	}

	if _, loaded := m.fCreate(req.Fname, fm); loaded {
		return fmt.Errorf("File %q already created", req.Fname)
	}

	// m.logger.Printf("Created(%q): %v\n", req.Fname, fm.assignments)
	*assignments = fm.assignments
	return nil
}

// Lookup a file: find mapping for a file
func (m *Master) Lookup(fname string, ret **structure.FileBlocks) error {
	fm, found := m.fLookup(fname)

	if !found {
		return fmt.Errorf("File %q not found", fname)
	}

	fb := &structure.FileBlocks{
		Fsize:       fm.fSize,
		Assignments: m.pickReadReplica(fm),
	}

	// m.logger.Printf("Lookup(%q): %v\n", fname, fb)
	*ret = fb

	fm.trafficLock.Lock()
	defer fm.trafficLock.Unlock()
	fm.nRead++

	return nil
}
