package client

import (
	"net/rpc"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

type ClientCreate func(fname string, fsize uint64, rfactor int) ([]structure.ReplicaMap, error)

type Conn struct {
	addr      string
	rpcClient *gifts.RPCClient
	Create    ClientCreate
}

// NewConn constructor for Client.Conn
func NewConn(addr string) *Conn {
	c := Conn{addr: addr, rpcClient: gifts.NewRPCClient(addr)}
	c.makeCreate()
	return &c
}

// TODO: fix hard-code for RPC
func (c *Conn) makeCreate() {
	c.Create = func(fname string, fsize uint64, rfactor int) ([]structure.ReplicaMap, error) {
		// Working in progress: too many arguments for RPC call, need a struct for args
		var ret []structure.ReplicaMap
		err := c.rpcClient.Call(func(conn *rpc.Client) error {
			conn.Call("NameNode.Create", fname, fsize, rfactor, &ret)
		})
	}

}
