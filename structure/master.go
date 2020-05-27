package structure

// FileCreateReq is the request type of Master.Create(),
// needed since Go RPC only support one argument
type FileCreateReq struct {
	FName   string
	FSize   int
	RFactor uint
}

// BlockAssign is the slice element of return value of Master.Create(),
// it maps the BlockID to the pre-assigned set of replicas,
// which is a slice of DataNode addresses
type BlockAssign struct {
	BlockID  string
	Replicas []string
}

// FileBlocks is the return type of Master.Lookup()
type FileBlocks struct {
	Fsize       int           // size of the file, to handle padding
	Assignments []BlockAssign // Nodes[i] stores the addr of DataNode with ith Block, where len(Replicas) >= 1
}
