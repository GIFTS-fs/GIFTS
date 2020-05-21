package bench

// Inspired by
// https://stackoverflow.com/questions/51885117/loadorstore-in-a-sync-map-without-creating-a-new-structure-each-time
// but replace slow strconv

import (
	"sync"
	"testing"
)

type stringStruct struct {
	s string
}

func BenchmarkLoadThenStore(b *testing.B) {
	for N := 0; N < b.N; N++ {
		var m sync.Map
		for i := 0; i < 64*1024; i++ {
			for k := 0; k < 256; k++ {

				// Assume cache hit
				v, ok := m.Load(k)
				if !ok {
					// allocate and initialize value
					v = stringStruct{"a"}
					a, loaded := m.LoadOrStore(k, v)
					if loaded {
						v = a
					}
				}
				_ = v

			}
		}
	}
}

func BenchmarkLoadOrStore(b *testing.B) {
	for N := 0; N < b.N; N++ {
		var m sync.Map
		for i := 0; i < 64*1024; i++ {
			for k := 0; k < 256; k++ {

				// Assume cache miss
				// allocate and initialize value
				var v interface{} = stringStruct{"a"}
				a, loaded := m.LoadOrStore(k, v)
				if loaded {
					v = a
				}
				_ = v

			}
		}
	}
}
