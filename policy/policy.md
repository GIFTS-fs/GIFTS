# Policy

## New Block Placement

### Round-Robin

### Permutation (Maglev Hashing)

### Least Load (Optimal)

(sorted by sum(traffic counter) for all files stored, break ties using nBlocks stored etc.)

### Round-Robin

## Lookup Selection

### Least Load (Optimal)

###	Round-Robin

### Closest Distance to User (Most Pratical)

## Imbalance Detection

## Replica Block Placement

### Round-Robin

Each block metadata stores indicies

### Consist Hashing + Permutation

Have a list of slices with size n_storage with fixed size (say 10)

Each block is hashed into one of the slice

Each slice stores shuffled indices of all storages

```golang
h1 := hashFunction1(BlockID)
h2 := hashFunction2(BlockID)
table_idx := h1 % table_size
idx_beg := h2 % n_storage
idx_end := idx_beg + 1
for i := 0; i < nReplica; i++ {
  idx_end_next := clockTick(idx_end)
  replicas[i] = replicaLookupTable[table_idx][idx_end] 
  idx_end = idx_end_next
}
```

### Discarded: Priortize Storage with No Belonging Block

Need a way to bookkeep, both fast and reliable to read and update.

Those assumed "better" choices may store popular blocks of other
files.

Its benefit may only outweigh its cost when there is exactly one
popular file with moderate popularity, since extreme popularity
leads to full utilization of all storage nodes.

Discarded.
