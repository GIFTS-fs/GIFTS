package structure

// FileMap is the return type of NameNode.Read()
type FileMap struct {
	Fsize uint64   // size of the file, to handle padding
	Nodes []string // Nodes[i] stores the addr of DataNode with ith Block
}
