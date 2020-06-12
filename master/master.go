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
	"github.com/GIFTS-fs/GIFTS/policy"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Master is the master of GIFTS
type Master struct {
	Logger *gifts.Logger
	config *config.Config

	// file name -> *fileMeta
	fMap sync.Map

	// number of storage alive, for 1st phase, it's const
	nStorage int
	// list of storages, used mainly by Clock
	storages []*storeMeta
	// storage addr -> *storeMeta
	sMap sync.Map

	// Only one balancing thread at one time
	isBalancing     bool
	isBalancingLock sync.Mutex

	// traffic statistics
	trafficMedian *algorithm.RunningMedian
	trafficLock   sync.Mutex

	/* Policy fields */
	createHandLock      sync.Mutex
	touchCreateHandUnit func(int) int

	nextReplicaOfUnit   func(*fileBlock) *storeMeta
	removeReplicaOfUnit func(*fileBlock) *storeMeta

	// Note that policy 1,2 can share the same createHand

	// Block placement policy 1: Round-robin
	createHandRR int

	// Block placement policy 2: consist hashing + random permutation
	placementEntry    []int
	placementEntryLen int
	createHandPermu   int // placementEntry[createHandPermu]: next backend to place a block

	// Replica placement policy 2: consist hashing + random permutation
	replicaPermu [][]int
}

// NewMaster creates a new GIFTS Master.
// It requires a list of addresses of Storage nodes.
func NewMaster(storageAddr []string, config *config.Config) *Master {
	m := Master{
		Logger:        gifts.NewLogger("Master", "local", false), // PRODUCTION: banish this
		nStorage:      len(storageAddr),
		storages:      make([]*storeMeta, len(storageAddr)),
		trafficMedian: algorithm.NewRunningMedian(),
		config:        config,
	}

	for i, addr := range storageAddr {
		s := newStoreMeta(addr)
		m.sMap.Store(addr, s)
		m.storages[i] = s
	}

	// For selection policy: rand
	rand.Seed(time.Now().UnixNano())

	switch config.BlockPlacementPolicy {
	case policy.BlockPlacementPolicyPermutation:
		m.populateLookupTable(storageAddr)
		m.touchCreateHandUnit = m.touchCreateHandUnitPermu
	default:
		m.touchCreateHandUnit = m.touchCreateHandUnitRR
	}

	switch config.ReplicaPlacementPolicy {
	case policy.ReplicaPlacementPolicyPermutation:
		m.buildReplicaPermuTable()
		m.nextReplicaOfUnit = m.nextReplicaOfUnitPermu
		m.removeReplicaOfUnit = m.removeReplicaOfUnitPermu
	default:
		m.nextReplicaOfUnit = m.nextReplicaOfUnitRR
		m.removeReplicaOfUnit = m.removeReplicaOfUnitRR
	}

	return &m
}

// background tasks of master:
//
// 1. periodically attempt to rebalance load across storage
func (m *Master) background() {
	// TODO: make the interval dynamic based on the traffic and number of files?
	if m.config.DynamicReplicationEnabled {
		tickerRebalance := time.NewTicker(time.Second * m.config.MasterRebalanceIntervalSec)
		defer tickerRebalance.Stop()

		for {
			select {
			case <-tickerRebalance.C:
				go m.balance()
			}
		}
	}
}

// ServeRPCBlock makes the Master accessible via RPC at the specified IP
// address and port.  Blocks and does not return.
func ServeRPCBlock(m *Master, addr string, readyChan chan bool) (err error) {
	m.Logger = gifts.NewLogger("Master", addr, m.Logger.Enabled) // PRODUCTION: banish this

	server := rpc.NewServer()
	defer func() {
		if readyChan != nil {
			select {
			case readyChan <- false:
			default:
			}
		}
	}()

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
	if readyChan != nil {
		readyChan <- true
		readyChan = nil
	}

	// Serve the Master at the specified IP address and port
	return http.Serve(l, mux)
}

// ServeRPC makes the Master accessible via RPC
// at the specified IP address and port.
func ServeRPC(m *Master, addr string) (err error) {
	readyChan := make(chan bool)
	go func() {
		err = ServeRPCBlock(m, addr, readyChan)
	}()
	if !<-readyChan && err == nil {
		err = fmt.Errorf("Master %v at %q not ready", m, addr)
	}
	return
}

// Create a file: assign replicas for the clients to write
func (m *Master) Create(req *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	// File with the same name already exists
	if m.fExist(req.Fname) {
		err := fmt.Errorf("File %q already exists", req.Fname)
		m.Logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	if int(req.Rfactor) < 0 {
		err := fmt.Errorf("req.Rfactor too large and overflowed int type: %v", req.Rfactor)
		m.Logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	var loaded bool
	var blockAssignments []structure.BlockAssign

	// Create one and only one fMeta for each file
	if blockAssignments, loaded = m.fCreate(req.Fname, req); loaded {
		err := fmt.Errorf("File %q already created", req.Fname)
		m.Logger.Printf("Master.Create(%v) => %q", *req, err)
		return err
	}

	*assignments = blockAssignments

	m.Logger.Printf("Master.Create(%v) => success", *req)
	return nil
}

// Lookup a file: find mapping for a file
func (m *Master) Lookup(fName string, ret **structure.FileBlocks) error {
	// Attempt to look up where the file is stored
	fm, found := m.fLookup(fName)

	// Check if the file exists
	if !found {
		err := fmt.Errorf("File %q not found", fName)
		m.Logger.Printf("Master.Lookup(%q) => %q", fName, err)
		return err
	}

	// Figure out which replicas the client should read from
	*ret = &structure.FileBlocks{
		Fsize:       fm.fSize,
		Assignments: m.lookupReplicas(fm),
	}

	// Keep track of the number of times this file has been read
	// don't let this block the critical path
	go func() {
		// TODO: shall remove the lock since we don't care the exact data?

		m.trafficLock.Lock()
		prev, curr := fm.trafficCounter.GetRaw(), fm.trafficCounter.Hit()
		m.trafficMedian.Update(prev, curr)
		m.trafficLock.Unlock()

		// m.Logger.Printf("DEBUG: traffic for %q: prev: %v curr: %v\n", fm.fName, prev, curr)
	}()

	m.Logger.Printf("Master.Lookup(%q) => %v", fName, *ret)
	return nil
}
