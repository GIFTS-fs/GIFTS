package bench

// Inspired by
// https://stackoverflow.com/questions/51885117/loadorstore-in-a-sync-map-without-creating-a-new-structure-each-time
// but replace slow strconv

import (
	"sync"
	"testing"

	"github.com/GIFTS-fs/GIFTS/storage"
)

type stringStruct struct {
	s string
}

func BenchmarkLoadLoadOrStore(b *testing.B) {
	var m sync.Map
	m.Store("addr", storage.NewRPCStorage("a"))

	var a interface{}
	for N := 0; N < b.N; N++ {
		a, _ = m.Load("addr")
	}
	_ = a
}

func BenchmarkLoadOrStore(b *testing.B) {
	var m sync.Map
	m.Store("addr", storage.NewRPCStorage("a"))

	var a interface{}
	for N := 0; N < b.N; N++ {
		a, _ = m.LoadOrStore("addr", storage.NewRPCStorage("a"))
	}
	_ = a
}
