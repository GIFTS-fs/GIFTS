package algorithm

import (
	"fmt"
	"testing"

	"github.com/GIFTS-fs/GIFTS/test"
)

var firstFewPrimes = [...]int{
	2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
	31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
	73, 79, 83, 89, 97, 101, 103, 107, 109, 113,
	127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
	179, 181, 191, 193, 197, 199, 211, 223, 227, 229,
	233, 239, 241, 251, 257, 263, 269, 271, 277, 281,
	283, 293, 307, 311, 313, 317, 331, 337, 347, 349,
	353, 359, 367, 373, 379, 383, 389, 397, 401, 409,
	419, 421, 431, 433, 439, 443, 449, 457, 461, 463,
	467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
	547, 557, 563, 569, 571, 577, 587, 593, 599, 601,
	607, 613, 617, 619, 631, 641, 643, 647, 653, 659,
	661, 673, 677, 683, 691, 701, 709, 719, 727, 733,
	739, 743, 751, 757, 761, 769, 773, 787, 797, 809,
	811, 821, 823, 827, 829, 839, 853, 857, 859, 863,
	877, 881, 883, 887, 907, 911, 919, 929, 937, 941,
	947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013,
}

func TestPrimesUntil(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	verifyPrimes := func(primes []int) {
		for i := range primes {
			if i >= len(firstFewPrimes) {
				t.Logf("Primes too many!")
				break
			}
			af(primes[i] == firstFewPrimes[i], fmt.Sprintf("VerifyPrimes: [%v] Want %v Got %v", i, firstFewPrimes[i], primes[i]))
		}
	}

	var primes []int

	primes = PrimesUntil(1)
	af(len(primes) == 0, "PrimesBelow 1 is []")

	primes = PrimesUntil(2)
	af(len(primes) == 1, "PrimesBelow 2 is [2]")
	verifyPrimes(primes)

	for i := 3; i < firstFewPrimes[len(firstFewPrimes)-1]-1; i++ {
		primes = PrimesUntil(i)
		verifyPrimes(primes)
	}

	primes = PrimesUntil(firstFewPrimes[len(firstFewPrimes)-1])
	af(len(primes) == len(firstFewPrimes), "len(PrimesBelow(1013)) is 170")
	verifyPrimes(primes)
}

func TestNextPrimeOf(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	verifyNextPrime := func(n, nextPrime int) {
		if n >= firstFewPrimes[len(firstFewPrimes)-1] {
			t.Logf("Primes too large to check!")
		}
		for _, p := range firstFewPrimes {
			if p > n {
				af(nextPrime == p, fmt.Sprintf("VerifyNextPrime: n: %v Want %v Got %v", n, p, nextPrime))
				break
			}
		}

	}

	for i := 0; i < firstFewPrimes[len(firstFewPrimes)-1]; i++ {
		verifyNextPrime(i, NextPrimeOf(i))
	}
}
