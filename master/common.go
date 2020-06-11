package master

import "strconv"

func nameBlock(fname string, i int) string {
	// WARN: very flippant and frivolous way to make BlockID
	// TODO: use better ways both for security and make it look cool, such as a hash
	return fname + strconv.FormatInt(int64(i), 16)
}

// panic if len == 0
func clockTick(hand int, len int, amount int) int {
	return (hand + amount) % len
}
