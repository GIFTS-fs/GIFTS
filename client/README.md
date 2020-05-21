# Client
## APIs
```go
// Store stores a file with the specified file name, replication factor, and
// data.  Note that the replication factor is only a hint: we may allocate
// fewer replicas depending on the number of Storage nodes available.
// It returns an error if:
//		- A file with the specified file name already exists
//		- The Master does not give us enough blocks in which to store the data
//		- There is a network error (this is fatal and cannot be recovered from)
func Store(fname string, rfactor uint, data []byte) error{
    ...
}

func (c *Client) Read(fname string) ([]byte, error) {
    ...
}
```