package bench

import "log"

const (
	// DefaultConfigPathClient since client does not need to know the policy
	// those can be ignored and using only one config for all
	DefaultConfigPathClient = "config-client.json"
)

// ExitUnless the cond is true
func ExitUnless(cond bool, msg string) {
	if !cond {
		log.Fatalf(msg)
	}
}
