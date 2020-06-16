package bench

import (
	"testing"
)

func clockTick1Mod(hand int, len int, amount int) (newHand int) {
	return (hand + amount) % len
}

func clockTickIfThenMod(hand int, len int, amount int) (newHand int) {
	newHand = hand + amount
	if newHand >= len {
		newHand %= len
	}

	return
}

func clockTick2If1Mod(hand int, len int, amount int) (newHand int) {
	if amount > len {
		amount %= len
	}

	newHand = hand + amount

	if newHand >= len {
		newHand -= len
	}

	return
}

func BenchmarkClockTick1Mod(b *testing.B) {
	for i := 1; i < b.N; i++ {
		clockTick1Mod(1*i, 10*i, i*1)
		clockTick1Mod(1*i, 10*i, i*5)
		clockTick1Mod(1*i, 10*i, i*9)
		clockTick1Mod(1*i, 10*i, i*11)
		clockTick1Mod(1*i, 10*i, i*20)
	}
}

func BenchmarkClockTickIfThenMod(b *testing.B) {
	for i := 1; i < b.N; i++ {
		clockTickIfThenMod(i*1, i*10, i*1)
		clockTickIfThenMod(i*1, i*10, i*5)
		clockTickIfThenMod(i*1, i*10, i*9)
		clockTickIfThenMod(i*1, i*10, i*11)
		clockTickIfThenMod(i*1, i*10, i*20)
	}
}

func BenchmarkClockTick2If1Mod(b *testing.B) {
	for i := 1; i < b.N; i++ {
		clockTick2If1Mod(i*1, i*10, i*1)
		clockTick2If1Mod(i*1, i*10, i*5)
		clockTick2If1Mod(i*1, i*10, i*9)
		clockTick2If1Mod(i*1, i*10, i*11)
		clockTick2If1Mod(i*1, i*10, i*20)
	}
}
