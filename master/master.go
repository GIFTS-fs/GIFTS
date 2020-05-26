package master

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/storage"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// MaxRfactor limits the value of rfactor
const MaxRfactor = 256

// Master is the master of GIFTS
type Master struct {
	logger *gifts.Logger

	fMap sync.Map

	storages        []*storage.RPCStorage
	storageLoad     sync.Map
	createClockHand int
}

// NewMaster is the constructor for master
func NewMaster() *Master {
	m := Master{}
	m.logger = gifts.NewLogger("Master", "master", true) // PRODUCTION: banish this
	return &m
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

	fm := &fMeta{
		fSize:       req.Fsize,
		rFactor:     req.Rfactor,
		assignments: m.makeAssignment(req),
		nRead:       0}

	if _, loaded := m.fCreate(req.Fname, fm); loaded {
		return fmt.Errorf("File %q already created", req.Fname)
	}

	assignments = &fm.assignments
	return nil
}

// Lookup a file: find mapping for a file
func (m *Master) Lookup(fname string, ret *structure.FileBlocks) error {
	fm, found := m.fLookup(fname)

	if !found {
		return fmt.Errorf("File %q not found", fname)
	}

	// TODO: BALANCE
	//fm.nRead++

	fb := &structure.FileBlocks{
		Fsize:       fm.fSize,
		Assignments: m.pickReadReplica(fm),
	}

	ret = fb

	return nil
}
