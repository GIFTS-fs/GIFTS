package generate

import (
	"testing"

	"github.com/GIFTS-fs/GIFTS/test"
)

func TestGenerate(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	t.Logf("WARN: since output is random, no way to check")

	g := NewGenerate()
	buf := make([]byte, 10)
	readCnt, err := g.Read(buf)
	af(readCnt == 10, "not enough bytes generated")
	af(err == nil, "not expecting failure")
}
