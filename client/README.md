# Client


## APIs

`Create(fname string, fsize uint64, rfactor int, data []byte) error`

Create a file with name, size, replication factor and content

`Read(fname string) []byte`