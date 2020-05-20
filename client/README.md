# Client


## APIs

`Store(fname string, fsize uint64, rfactor int, data []byte) error`

Store a file with name, size, replication factor and content

`Read(fname string) []byte`