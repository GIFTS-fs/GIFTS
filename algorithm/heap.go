package algorithm

import "container/heap"

// Heap interface with Top (since Golang's heap has no Top())
type Heap interface {
	heap.Interface
	Top() interface{}
}

// BasicHeap based on a slice and has no Less() defined
type BasicHeap struct {
	data []interface{}
}

// Len of the heap
func (h BasicHeap) Len() int { return len(h.data) }

// No Less Defined

// Swap 2 heap elements
func (h BasicHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

// Push to the end
func (h *BasicHeap) Push(x interface{}) { h.data = append(h.data, x) }

// Pop from top
func (h *BasicHeap) Pop() (x interface{}) { x, h.data = h.data[h.Len()-1], h.data[0:h.Len()-1]; return }

// Top of data
func (h BasicHeap) Top() interface{} { return h.data[0] }

/*
// TheHeap defines Less and needs a less compare function
type TheHeap struct {
	BasicHeap
	less func(a, b interface{}) bool
}

func NewTheHeap(less func(a, b interface{}) bool) *TheHeap {
	return &TheHeap{less: less}
}

func (rh *TheHeap) Less(i, j int) bool {
	return rh.less(rh.data[i], rh.data[j])
}
*/

/* int */

// BasicIntHeap based on BasicHeap
type BasicIntHeap struct {
	BasicHeap
}

// Top of the int data
func (h BasicIntHeap) Top() int { return h.BasicHeap.Top().(int) }

// MinIntHeap based on BasicIntHeap
type MinIntHeap struct {
	BasicIntHeap
}

// Less for min heap
func (h MinIntHeap) Less(i, j int) bool { return h.data[i].(int) < h.data[j].(int) }

// MaxIntHeap based on BasicIntHeap
type MaxIntHeap struct {
	BasicIntHeap
}

// Less for max heap
func (h MaxIntHeap) Less(i, j int) bool { return h.data[i].(int) > h.data[j].(int) }

/* float64 */

// BasicFloat64Heap based on BasicHeap
type BasicFloat64Heap struct {
	BasicHeap
}

// Top of the int data
func (h BasicFloat64Heap) Top() float64 { return h.BasicHeap.Top().(float64) }

// MinIntHeap based on BasicIntHeap
type MinFloat64Heap struct {
	BasicFloat64Heap
}

// Less for min heap
func (h MinFloat64Heap) Less(i, j int) bool { return h.data[i].(float64) < h.data[j].(float64) }

// MaxIntHeap based on BasicIntHeap
type MaxFloat64Heap struct {
	BasicFloat64Heap
}

// Less for max heap
func (h MaxFloat64Heap) Less(i, j int) bool { return h.data[i].(float64) > h.data[j].(float64) }
