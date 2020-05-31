package master

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/algorithm"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// MaxRfactor limits the value of rfactor,
	// mere a magic number to prevent uint overlows int
	MaxRfactor = 256

	// rebalanceIntervalSec
	rebalanceIntervalSec = 10
)

// Master is the master of GIFTS
type Master struct {
	logger          *gifts.Logger
	config          *config.Config
	fMap            sync.Map
	nStorage        int // number of storage alive, for 1st phase, it's const
	storages        []*storage.RPCStorage
	createClockHand int

	trafficMedian *algorithm.RunningMedian
	trafficLock   sync.Mutex
}

// NewMaster creates a new GIFTS Master.
// It requires a list of addresses of Storage nodes.
func NewMaster(storageAddr []string, config *config.Config) *Master {
	m := Master{
		logger:          gifts.NewLogger("Master", "master", true), // PRODUCTION: banish this
		nStorage:        len(storageAddr),
		createClockHand: 0,
		trafficMedian:   algorithm.NewRunningMedian(),
		config:          config,
	}

	// Store a connection to every Storage node
	for _, addr := range storageAddr {
		m.storages = append(m.storages, storage.NewRPCStorage(addr))
	}

	rand.Seed(time.Now().UnixNano())

	return &m
}

// background tasks of master:
//
// 1. periodically attempt to rebalance load across storage
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

// ServeRPC makes the Master accessible via RPC
// at the specified IP address and port.
func ServeRPC(m *Master, addr string) (err error) {
	server := rpc.NewServer()

	err = server.RegisterName("Master", m)
	if err != nil {
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	mux := http.NewServeMux()
	mux.Handle(RPCPathMaster, server)

	// Start Master's background tasks
	go m.background()

	// Serve the Master at the specified IP address and port
	go http.Serve(l, mux)

	return
}

// Create a file: assign replicas for the clients to write
func (m *Master) Create(req *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	// File with the same name already exists
	if m.fExist(req.Fname) {
		err := fmt.Errorf("File %q already exists", req.Fname)
		m.logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	// Set some (arbitrary) limit on the maximum number of replicas, regardless
	// of the number of Storage nodes.
	if req.Rfactor > MaxRfactor {
		err := fmt.Errorf("RFactor %v is too large (> %v)", req.Rfactor, MaxRfactor)
		m.logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	var fm *fMeta
	var loaded bool

	// Create one and only one fMeta for each file
	if fm, loaded = m.fCreate(req.Fname, req); loaded {
		err := fmt.Errorf("File %q already created", req.Fname)
		m.logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	// Set the return value
	*assignments = fm.assignments

	m.logger.Printf("Master.Create(%v) => success", *req)
	return nil
}

// Lookup a file: find mapping for a file
func (m *Master) Lookup(fName string, ret **structure.FileBlocks) error {
	// Attempt to look up where the file is stored
	fm, found := m.fLookup(fName)

	// Check if the file exists
	if !found {
		err := fmt.Errorf("File %q not found", fName)
		m.logger.Printf("Master.Lookup(%q) => %q", fName, err)
		return err
	}

	// Figure out which replicas the client should read from
	*ret = &structure.FileBlocks{
		Fsize:       fm.fSize,
		Assignments: m.pickReadReplica(fm),
	}

	// Keep track of the number of times this file has been read
	// don't let this block the critical path
	go func() {
		// TODO: shall remove the lock since we don't care the exact data?
		fm.trafficLock.Lock()
		prev, curr := fm.trafficCounter.GetRaw(), fm.trafficCounter.Hit()
		fm.trafficLock.Unlock()

		m.trafficLock.Lock()
		m.trafficMedian.Update(prev, curr)
		m.trafficLock.Unlock()
	}()

	m.logger.Printf("Master.Lookup(%q) => success", fName)
	return nil
}
