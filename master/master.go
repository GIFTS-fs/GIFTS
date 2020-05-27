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
	// MaxRFactor limits the value of rfactor
	MaxRFactor = 256

	// rebalanceIntervalSec
	rebalanceIntervalSec = 10
)

// Master is the master of GIFTS
type Master struct {
	logger          *gifts.Logger
	fMap            sync.Map
	storages        []*storage.RPCStorage
	createClockHand int
}

// NewMaster creates a new GIFTS Master.  It requires a list of addresses of
// all Storage nodes.
func NewMaster(storageAddr []string) *Master {
	m := Master{
		logger:          gifts.NewLogger("Master", "master", true), // PRODUCTION: banish this
		createClockHand: 0,
	}

	// Store a connection to every Storage node
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

// ServeRPC makes the Master accessible via RPC at the specified IP address and
// port.
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

	// Start Master's background task that periodically tries to rebalance the
	// replicas amongst the Storage nodes.
	go m.background()

	// Serve the Master at the specified IP address and port
	go http.Serve(l, mux)

	return
}

// Create a file: assign replicas for the clients to write
func (m *Master) Create(request *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	// RFactor must be at least 1
	if request.RFactor < 1 {
		msg := "RFactor must be at least 1"
		m.logger.Printf("Master.Create(%v) => %q", *request, msg)
		return fmt.Errorf(msg)
	}

	// File with the same name already exists
	if m.fileExists(request.FName) {
		msg := fmt.Sprintf("File %q already exists", request.FName)
		m.logger.Printf("Master.Create(%v) => %q", *request, msg)
		return fmt.Errorf(msg)
	}

	// DLAD: Why is MaxRFactor a constant?  Shouldn't it be based on the number
	// of storage nodes (i.e. you have a check for this in makeAssignment, why
	// not make it an error)?
	if request.RFactor > MaxRFactor {
		msg := fmt.Sprintf("RFactor %v is too large (> %v)", request.RFactor, MaxRFactor)
		m.logger.Printf("Master.Create(%v) => %q", *request, msg)
		return fmt.Errorf(msg)
	}

	// Split the file into blocks
	nBlocks := gifts.NBlocks(request.FSize)
	fm := &fMeta{
		fSize:       request.FSize,
		nBlocks:     nBlocks,
		rFactor:     request.RFactor,
		assignments: m.makeAssignment(request, nBlocks),
		nRead:       0,
	}

	// Store the block-to-Storage-node mapping
	// DLAD: The master might need to store indexes into m.storages instead of
	// the IP addresses.  When we increase replication, we'll need to find an
	// storage not already used.
	if _, loaded := m.fCreate(request.FName, fm); loaded {
		msg := fmt.Sprintf("File %q already created", request.FName)
		m.logger.Printf("Master.Create(%v) => %q", *request, msg)
		return fmt.Errorf(msg)
	}

	// Set the return value
	*assignments = fm.assignments

	m.logger.Printf("Master.Create(%v) => success", *request)
	return nil
}

// Lookup a file: find mapping for a file
// DLAD: why is the return value a pointer to a pointer?
func (m *Master) Lookup(fName string, ret **structure.FileBlocks) error {
	// Attempt to look up where the file is stored
	fm, found := m.fLookup(fName)

	// Check if the file exists
	if !found {
		msg := fmt.Sprintf("File %q not found", fName)
		m.logger.Printf("Master.Lookup(%q) => %q", fName, msg)
		return fmt.Errorf(msg)
	}

	// Figure out which replicas the client should read from
	*ret = &structure.FileBlocks{
		Fsize:       fm.fSize,
		Assignments: m.pickReadReplica(fm),
	}

	// Keep track of the number of times this file has been read
	fm.trafficLock.Lock()
	defer fm.trafficLock.Unlock()
	fm.nRead++

	m.logger.Printf("Master.Lookup(%q) => success", fName)
	return nil
}
