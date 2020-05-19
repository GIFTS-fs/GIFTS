package master

import (
	"net/rpc"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// RPCPathNameNode the path that NameNode listens to
	RPCPathNameNode = "/_gifts_namenode_"
	// RPCMethodCreate ...
	RPCMethodCreate = "Master.Create"
	// RPCMethodRead ...
	RPCMethodRead = "Master.Read"
)

// Conn is the connection to one Master.
// It is cache safe (i.e. can reuse as long as the server is alive, no matter failed in between)
type Conn struct {
	addr   string
	Create CreateFunc
	Read   ReadFunc
}

// NewConn constructor for Client.Conn
func NewConn(addr string) *Conn {
	c := Conn{addr: addr}
	rpcClient := gifts.NewRPCClient(addr, gifts.RPCPathNameNode)
	c.makeCreate(rpcClient)
	c.makeRead(rpcClient)
	return &c
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeCreate(rcli *gifts.RPCClient) {
	c.Create = func(fname string, fsize uint64, rfactor int) ([]structure.BlockAssign, error) {
		var ret []structure.BlockAssign
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodCreate,
				structure.FileCreateReq{Fname: fname, Fsize: fsize, Rfactor: rfactor},
				&ret,
			)
		})
		return ret, err
	}
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeRead(rcli *gifts.RPCClient) {
	c.Read = func(fname string) (structure.FileBlocks, error) {
		var ret structure.FileBlocks
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodRead,
				fname,
				&ret,
			)
		})
		return ret, err
	}
}
