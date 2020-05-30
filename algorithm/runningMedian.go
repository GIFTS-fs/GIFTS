package algorithm

import "container/heap"

// RunningMedian provides median for a data stream
type RunningMedian struct {
	size   uint64
	median float64

	lower  *MaxFloat64Heap
	higher *MinFloat64Heap

	del map[float64]int
}

// NewRunningMedian constructs a RunningMedian for the given less function
func NewRunningMedian() *RunningMedian {
	r := &RunningMedian{
		lower:  &MaxFloat64Heap{},
		higher: &MinFloat64Heap{},
		del:    make(map[float64]int),
	}

	heap.Init(r.lower)
	heap.Init(r.higher)

	return r
}

func (r *RunningMedian) calcuate() {
	if r.size&1 == 1 {
		r.median = r.lower.Top()
	} else {
		r.median = 0.5 * (r.lower.Top() + r.higher.Top())
	}
}

// Median of seen data, not concurrency safe
func (r *RunningMedian) Median() float64 {
	return r.median
}

func (r *RunningMedian) balance() {
	if r.lower.Len() > r.higher.Len()+1 {
		heap.Push(r.higher, heap.Pop(r.lower))
	} else if r.lower.Len() < r.higher.Len() {
		heap.Push(r.lower, heap.Pop(r.higher))
	}
}

// Add a new data, not concurrency safe
func (r *RunningMedian) Add(add float64) {
	if r.lower.Len() == 0 || add <= r.lower.Top() {
		heap.Push(r.lower, add)
	} else {
		heap.Push(r.higher, add)
	}

	r.balance()

	r.size++
	r.calcuate()
}

func (r *RunningMedian) delete() {
	for r.lower.Len() > 0 && r.del[r.lower.Top()] > 0 {
		r.del[r.lower.Top()]--
		heap.Pop(r.lower)
	}

	for r.higher.Len() > 0 && r.del[r.higher.Top()] > 0 {
		r.del[r.higher.Top()]--
		heap.Pop(r.higher)
	}
}

// Delete an element, not concurrency safe
func (r *RunningMedian) Delete(del float64) {
	if r.size <= 0 {
		return
	}

	if del <= r.lower.Top() {
		if del == r.lower.Top() {
			heap.Pop(r.lower)
		} else {
			r.del[del]++
		}
		heap.Push(r.lower, heap.Pop(r.higher))
	} else {
		if del == r.higher.Top() {
			heap.Pop(r.higher)
		} else {
			r.del[del]++
		}
		heap.Push(r.higher, heap.Pop(r.lower))
	}

	r.balance()
	r.delete()

	r.size--
	r.calcuate()
}

// Update the median by deleting del and adding add,
// assuming del has been seen, will break the internal
// datastructure otherwise
func (r *RunningMedian) Update(del, add float64) {
	balance := 0

	if del <= r.lower.Top() {
		balance--
		if del == r.lower.Top() {
			heap.Pop(r.lower)
		} else {
			r.del[del]++
		}
	} else {
		balance++
		if del == r.higher.Top() {
			heap.Pop(r.higher)
		} else {
			r.del[del]++
		}
	}

	if r.lower.Len() > 0 && add <= r.lower.Top() {
		balance++
		heap.Push(r.lower, add)
	} else {
		balance--
		heap.Push(r.higher, add)
	}

	if balance < 0 {
		heap.Push(r.lower, heap.Pop(r.higher))
	} else if balance > 0 {
		heap.Push(r.higher, heap.Pop(r.lower))
	}

	r.delete()

	r.calcuate()
}
