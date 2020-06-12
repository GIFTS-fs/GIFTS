package bench

import "log"

// ExitUnless the cond is true
func ExitUnless(cond bool, msg string) {
	if !cond {
		log.Fatalf(msg)
	}
}
