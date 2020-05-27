package master

import (
	"net/rpc"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Conn is the connection to one Master.
// It is cache safe (i.e. can reuse as long as the server is alive, no matter failed in between)
type Conn struct {
	addr   string
	Create CreateFunc
	Lookup LookupFunc
}

// NewConn constructor for Client.Conn
func NewConn(addr string) *Conn {
	c := Conn{addr: addr}
	rpcClient := gifts.NewRPCClient(addr, RPCPathMaster)
	c.makeCreate(rpcClient)
	c.makeLookup(rpcClient)
	return &c
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeCreate(rcli *gifts.RPCClient) {
	c.Create = func(fname string, fsize int, rfactor uint) ([]structure.BlockAssign, error) {
		var ret []structure.BlockAssign
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodCreate,
				&structure.FileCreateReq{Fname: fname, Fsize: fsize, Rfactor: rfactor},
				&ret,
			)
		})
		return ret, err
	}
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeLookup(rcli *gifts.RPCClient) {
	c.Lookup = func(fname string) (*structure.FileBlocks, error) {
		var ret *structure.FileBlocks
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodLookup,
				fname,
				&ret,
			)
		})
		return ret, err
	}
}
