package algorithm

import "testing"

func TestPopulateTable(t *testing.T) {
	// TODO: how to test??
	t.Logf("WARN: this test hasn't been written")
	entry := PopulateLookupTable(100, 3, []string{"a", "b", "c"})
	if len(entry) < 300 {
		t.Fatalf("wrong number of entries")
	}
	// t.Logf("entry: %v", entry)
	// count := make([]int, 3)
	// for _, e := range entry {
	// 	count[e]++
	// }
	// t.Logf("count: 0:%v 1:%v 2:%v", count[0], count[1], count[2])
}
