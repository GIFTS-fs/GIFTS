package storage

import (
	"net/rpc"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

// Conn is the connection to one Storage.
// It is cache safe (i.e. can reuse as long as the server is alive, no matter failed in between)
type Conn struct {
	addr string
	Set  SetFunc
	Get  GetFunc
}

// NewConn constructor for Client.Conn
func NewConn(addr string) *Conn {
	c := Conn{addr: addr}
	rpcClient := gifts.NewRPCClient(addr, RPCPathStorage)
	c.makeSet(rpcClient)
	c.makeGet(rpcClient)
	return &c
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeSet(rcli *gifts.RPCClient) {
	c.Set = func(id string, data gifts.Block) (bool, error) {
		var ret bool
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodSet,
				&structure.BlockKV{ID: id, Data: data},
				&ret,
			)
		})
		return ret, err
	}
}

// TODO: fix hard-coding for RPC
func (c *Conn) makeGet(rcli *gifts.RPCClient) {
	c.Get = func(id string, buf gifts.Block) error {
		err := rcli.Call(func(conn *rpc.Client) error {
			return conn.Call(
				RPCMethodGet,
				id,
				&buf,
			)
		})
		return err
	}
}
