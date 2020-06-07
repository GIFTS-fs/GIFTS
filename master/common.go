package master

import "strconv"

func nameBlock(fname string, i int) string {
	// WARN: very flippant and frivolous way to make BlockID
	// TODO: use better ways both for security and make it look cool, such as a hash
	return fname + strconv.FormatInt(int64(i), 16)
}

func clockTick(hand int, len int) (newHand int) {
	newHand = hand + 1
	if newHand >= len {
		newHand = 0
	}
	return
}

/* UNTESTED
func clockTickBack(hand int, len int) (newHand int) {
	newHand = hand - 1
	if newHand == 0 {
		newHand = len - 1
	}
	return
}
*/
