package algorithm

// PopulateLookupTable for the Maglev Hashing algorithm
func PopulateLookupTable(MaglevHashingMultipler int, N int, names []string) (entry []int) {
	// heavily inspired by Maglev hashing lookup table Populate code
	// https://storage.googleapis.com/pub-tools-public-publication-data/pdf/44824.pdf
	// Maglev: A Fast and Reliable Software Network Load Balancer
	// Daniel E. Eisenbud and Cheng Yi and Carlo Contavalli and Cody Smith and
	// Roman Kononov and Eric Mann-Hielscher and Ardas Cilingiroglu and
	// Bin Cheyney and Wentao Shang and Jinnah Dylan Hosein

	if N == 0 {
		return
	}

	M := NextPrimeOf(N * MaglevHashingMultipler)

	// make the 2D slice for permutation
	permutation := make([][]int, N)
	for i := range permutation {
		permutation[i] = make([]int, M)
	}

	// fill the permutation
	for i, name := range names {
		offset := int(HashingFnvTwice(name) % int64(M))
		skip := int(HashingCrc32(name)%int64(M-1)) + 1
		for j := 0; j < M; j++ {
			permutation[i][j] = (offset + j*skip) % M
		}
	}

	// next[i]: the next index in permutation for storage i
	next := make([]int, N)

	// entry[i]: the chosen backend for ith block
	entry = make([]int, M)
	for j := 0; j < M; j++ {
		entry[j] = -1
	}

	n := 0

	for {
		for i := 0; i < N; i++ {
			c := permutation[i][next[i]]
			for entry[c] >= 0 {
				next[i]++
				c = permutation[i][next[i]]
			}
			entry[c] = i
			next[i]++
			n++
			if n == M {
				return
			}
		}
	}
}
