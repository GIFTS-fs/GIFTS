package policy

// BlockPlacementPolicy specifies which policy to use to place a block
type BlockPlacementPolicy int

// ReplicaPlacementPolicy specifies which policy to use to choose a replica
type ReplicaPlacementPolicy int

// BlockPlacementPolicy
const (
	BlockPlacementPolicyNull BlockPlacementPolicy = iota
	BlockPlacementPolicyRR
	BlockPlacementPolicyPermutation
)

// ReplicaPlacementPolicy
const (
	ReplicaPlacementPolicyNull ReplicaPlacementPolicy = iota
	ReplicaPlacementPolicyRR
	ReplicaPlacementPolicyPermutation
)
