package master

import (
	"net"
	"net/http"
	"net/rpc"
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Master is the master of GIFTS
type Master struct {
	fMap   sync.Map
	logger *gifts.Logger
}

// NewMaster is the constructor for master
func NewMaster() *Master {
	m := Master{}
	m.logger = gifts.NewLogger("Master", "master", true) // PRODUCTION: banish this
	return &m
}

// ServRPC spawns a thread listen to RPC traffic
func ServRPC(addr string) (err error) {
	serv := rpc.NewServer()

	err = serv.RegisterName("Master", NewMaster())
	if err != nil {
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	mux := http.NewServeMux()
	mux.Handle(RPCPathMaster, serv)
	err = http.Serve(l, mux)
	return
}

// Create a file: assign replicas for the clients to write
func (m *Master) Create(req *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	// structure.FileCreateReq{Fname: fname, Fsize: fsize, Rfactor: rfactor},

	return nil
}

// Lookup a file: find mapping for a file
func (m *Master) Lookup(fname string, ret *structure.FileBlocks) error {
	fb, found := m.fLookup(fname)

	if !found {
		ret = nil
		return nil // NOT MY ERROR! IT'S CLIENT'S
	}

	// TODO: find a way to return the replicas
	// Ideally like CLOCK page replacement algorithm
	ret = fb

	return nil
}
