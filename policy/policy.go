package policy

// BlockPlacementPolicy specifies which policy to use to place a block
type BlockPlacementPolicy int

// BlockPlacementPolicy
//
// 0: Null
//
// 1: round robin
//
// 2: random permutation + consist hashing
//
const (
	BlockPlacementPolicyNull BlockPlacementPolicy = iota
	BlockPlacementPolicyRR
	BlockPlacementPolicyPermutation
)
