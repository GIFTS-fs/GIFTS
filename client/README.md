# Client

```go
type Client struct {
	master   *master.Conn
	storages sync.Map
}

// NewClient creates a new GIFTS client
func NewClient(masters []string) *Client {
	...
}

// Store stores a file with the specified file name, replication factor, and
// data.  Note that the replication factor is only a hint: we may allocate
// fewer replicas depending on the number of Storage nodes available.
// It returns an error if:
//		- A file with the specified file name already exists
//		- The Master does not give us enough blocks in which to store the data
//		- There is a network error (this is fatal and cannot be recovered from)
func (c *Client) Store(fname string, rfactor uint, data *[]byte) error {
	...
}

// Read reads a file with the specified file name from the remote Storage.
// It returns an error if:
//		- The file does not exist
// 		- The Master fails or returns inconsistent metadata
//		- There is a network error
func (c *Client) Read(fname string, ret *[]byte) error {
	...
}
```