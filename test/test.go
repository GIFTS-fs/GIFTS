package test

import (
	"runtime/debug"
	"testing"
)

// AF asserts that the condition is true.  If it is false, it will print the
// debug stack and terminate the test.
func AF(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Errorf(msg)
		debug.PrintStack()
		t.FailNow()
	}
}
