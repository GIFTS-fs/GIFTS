package algorithm

// TODO: very bad algorithm design

// PrimesUntil non-negative number n (including n)
func PrimesUntil(n int) (primes []int) {
	if n <= 1 {
		return
	}

	nPrimes := 0
	checked := make(map[int]bool)

	for i := 2; i <= n; i++ {
		if !checked[i] {
			primes = append(primes, i)
			nPrimes++
		}
		for j := 0; j < nPrimes && i*primes[j] <= n; j++ {
			checked[i*primes[j]] = true
			if (i % primes[j]) == 0 {
				break
			}
		}
	}

	return
}

// NextPrimeOf number n
func NextPrimeOf(n int) (nextPrime int) {
	primes := PrimesUntil(n)
	isPrime := func(n int) bool {
		if n <= 1 {
			return false
		}
		if n <= 3 {
			return true
		}
		for _, p := range primes {
			if n%p == 0 {
				return false
			}
		}
		return true
	}

	nextPrime = n + 1

	for !isPrime(nextPrime) {
		nextPrime++
	}

	return
}
