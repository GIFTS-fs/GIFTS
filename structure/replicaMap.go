package structure

type FileCreateReq struct {
}

// ReplicaMap is the slice element of return value of NameNode.Create(),
// it maps the BlockID to the pre-assigned set of replicas,
// which is a slice of DataNode addresses
type ReplicaMap struct {
	BlockID  string
	Replicas []string
}
