package gifts

import (
	"net/rpc"
)

// RPCCall is the ingredient of a Request
type RPCCall func(*rpc.Client) error

// RPCClient is the client for RPC Calls that
// delays the connecting until first request,
// caches the connection,
// and automatically reconnects after failure.
// user of RPCClient can have guaranteed connection
// as long as the server at addr is alive
// without worrying about the liveness of the underneath connection
type RPCClient struct {
	// logger *gifts.Logger
	addr string
	path string
	conn *rpc.Client // will connect on first call
}

// NewRPCClient constructor for RPCFactory
func NewRPCClient(addr string, path string) *RPCClient {
	// return &RPCClient{logger: gifts.NewLogger("RPCClient", addr, false), addr: addr, path: path}
	return &RPCClient{addr: addr, path: path}
}

func (f *RPCClient) callRPCUnit(call RPCCall) (dialErr, callErr error) {
	if dialErr = f.Dial(); dialErr != nil {
		// f.logger.Printf("Dial error: %v", dialErr.Error())
		return
	}

	if callErr = call(f.conn); callErr != nil {
		// f.logger.Printf("call error: %v", callErr.Error())
		f.Close()
	}
	return
}

// try twice, in case failed but recovered
func (f *RPCClient) callRPC(call RPCCall) (err error) {
	var dialErr error
	if dialErr, err = f.callRPCUnit(call); dialErr != nil {
		_, err = f.callRPCUnit(call)
	}
	return
}

// Dial the server: Dial if not connected
func (f *RPCClient) Dial() (err error) {
	if f.conn == nil {
		// WARN: no concurrent control for performance concern
		f.conn, err = rpc.DialHTTPPath("tcp", f.addr, f.path)
	}
	return
}

// Close the RPC connection
func (f *RPCClient) Close() (err error) {
	if f.conn != nil {
		var c *rpc.Client
		// WARN: no concurrenct control for performance concern
		c, f.conn = f.conn, nil
		// since no concurrency control, c might be nil
		if c != nil {
			err = c.Close()
		}
	}
	return
}

// Call the RPCCall.
// Example usage: Call(func(r *rpc.Client) error {return r.Call("RPCMethod", arg, ret)})
func (f *RPCClient) Call(call RPCCall) error {
	return f.callRPC(call)
}
