package algorithm

import (
	"fmt"
	"testing"
	"time"

	"github.com/GIFTS-fs/GIFTS/test"
)

func TestDecayCounterBasic(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	counter1 := NewDecayCounter(100.0 * time.Minute.Seconds())
	counter1.Reset()

	h := counter1.Hit()
	af(h == 1.0, "1 hit")
	af(counter1.GetRaw() == 1.0, "GetLast 1")

	h = counter1.Hit()
	af(h <= 2.0, fmt.Sprintf("2 hit: Want <= %f Got %f", 2.0, h))

	// ????
}
