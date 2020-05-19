package gifts

import "net/rpc"

// RPCCall is the ingredient of a Request
type RPCCall func(*rpc.Client) error

// RPCClient is a factory of RPCCalls
type RPCClient struct {
	addr string
	conn *rpc.Client // will connect on first RPC call
}

// NewRPCClient constructor for RPCFactory
func NewRPCClient(addr string) *RPCClient {
	// return &RPCFactory{logger: newLogger(logPrefixNameClientRPC, addr), addr: addr}
	return &RPCClient{addr: addr}
}

func (f *RPCClient) callRPCUnit(call RPCCall) (dialErr, callErr error) {
	if dialErr = f.Dial(); dialErr != nil {
		// client.logger.Printf("Salute error: %v", err.Error())
		return
	}

	if callErr = call(f.conn); callErr != nil {
		// client.logger.Printf("call error: %v", err.Error())
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
		f.conn, err = rpc.DialHTTPPath("tcp", f.addr, GIFTS_RPC_PATH)
	}
	return
}

// Close the RPC connection
func (f *RPCClient) Close() (err error) {
	if f.conn != nil {
		err = f.conn.Close()
	}
	return
}

// Call the RPCCall
func (f *RPCClient) Call(call RPCCall) error {
	return f.callRPC(call)
}
