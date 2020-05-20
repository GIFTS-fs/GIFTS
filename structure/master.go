package structure

// FileCreateRep is the request type of Master.Create(),
// needed since Go RPC only support one argument
type FileCreateReq struct {
	Fname   string
	Fsize   uint64
	Rfactor int
}

// BlockAssign is the slice element of return value of Master.Create(),
// it maps the BlockID to the pre-assigned set of replicas,
// which is a slice of DataNode addresses
type BlockAssign struct {
	BlockID  string
	Replicas []string
}

// FileBlocks is the return type of Master.Read()
type FileBlocks struct {
	Fsize       uint64        // size of the file, to handle padding
	Assignments []BlockAssign // Nodes[i] stores the addr of DataNode with ith Block, where len(Replicas) == 1
}
