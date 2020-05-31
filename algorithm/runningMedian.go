package algorithm

import "container/heap"

// TODO: make the whole class concurrency safe without locks
// namely, remove strict requirements on size, if 0 then quit gracefully.
// That requires new algorithm

// TODO: the Add() may still need to consider
// edge cases to behave idential to Update()
// may contain bugs when mixing Add() and Update()

// RunningMedian provides median for a data stream:
//
// Invariant:
//
// * {x \in lower | x <= median}
//
// * {x \in higher and x != higher.Top() | x > median }
//
// when N is odd, median is .5*(higher.Top() + lower.Top())
type RunningMedian struct {
	size   uint64
	median float64

	lower  *MaxFloat64Heap
	higher *MinFloat64Heap

	delLower  int
	delHigher int
	del       map[float64]int // lazy delete: buffer of the numbers to delete
}

// NewRunningMedian constructs a RunningMedian for the given less function
func NewRunningMedian() *RunningMedian {
	r := &RunningMedian{
		lower:  &MaxFloat64Heap{},
		higher: &MinFloat64Heap{},
		del:    make(map[float64]int),
	}

	// useless calls, cosmetic only
	heap.Init(r.lower)
	heap.Init(r.higher)

	return r
}

// calculate the median and update
// assuming there is at least one data
// and it must be in lower heap
func (r *RunningMedian) calculate() {
	if r.size&1 == 1 {
		r.median = r.lower.Top()
	} else {
		r.median = 0.5 * (r.lower.Top() + r.higher.Top())
	}
}

// Median of seen data
func (r *RunningMedian) Median() float64 {
	return r.median
}

// balance the lower and higher,
// may contain bugs.
// ideally mimic the behavior of Update
func (r *RunningMedian) balance(balance int) {
	if balance > 0 && r.lower.Len()-r.delLower > r.higher.Len()-r.delHigher+1 {
		heap.Push(r.higher, heap.Pop(r.lower))
	} else if balance < 0 && r.lower.Len()-r.delLower < r.higher.Len()-r.delHigher {
		heap.Push(r.lower, heap.Pop(r.higher))
	}
}

// Add a new data
func (r *RunningMedian) Add(add float64) {
	balance := 0

	// log N
	if r.lower.Len() == 0 || add <= r.lower.Top() {
		balance++
		heap.Push(r.lower, add)
	} else {
		balance--
		heap.Push(r.higher, add)
	}

	r.size++

	r.balance(balance)

	r.calculate()
}

// delete until either top is not a number
// that was marked as to delete
// Assume delLower is correct count of number to delete in current lower heap
// and vice versa
func (r *RunningMedian) delete() {
	for r.lower.Len() > 0 && r.del[r.lower.Top()] > 0 {
		r.del[r.lower.Top()]--
		heap.Pop(r.lower)
		r.delLower--
	}

	for r.higher.Len() > 0 && r.del[r.higher.Top()] > 0 {
		r.del[r.higher.Top()]--
		heap.Pop(r.higher)
		r.delHigher--
	}

	// edge case: deleting until r.size == 1 and last one on higher
	// this implies potential bugs
	if r.lower.Len() == 0 && r.higher.Len() == 1 {
		heap.Push(r.lower, heap.Pop(r.higher))
	}
}

// Delete an element, not concurrency safe.
// If the element to delete was not Added,
// the behavior is undefined (may panic eventually)
// Not tested!
func (r *RunningMedian) Delete(del float64) {
	if r.size <= 0 {
		return
	}

	balance := 0
	// logN or buffer
	if del <= r.lower.Top() {
		balance--
		if del == r.lower.Top() {
			heap.Pop(r.lower)
		} else {
			r.delLower++
			r.del[del]++
		}
		if r.size > 1 {
			heap.Push(r.lower, heap.Pop(r.higher))
		}
	} else {
		balance++
		if del == r.higher.Top() {
			heap.Pop(r.higher)
		} else {
			r.delHigher++
			r.del[del]++
		}
		heap.Push(r.higher, heap.Pop(r.lower))
	}

	r.size--

	r.delete() // ensures not moving to-be-deleted data
	r.balance(balance)
	r.delete()

	// 1
	if r.size > 0 {
		r.calculate()
	} else {
		r.median = 0
	}
}

// Update the median by deleting del and adding add.
// If the element to delete was not Added,
// the behavior is undefined (may panic eventually)
func (r *RunningMedian) Update(del, add float64) {
	if del == add {
		return
	}

	balance := 0

	if del <= r.lower.Top() {
		balance--
		if del == r.lower.Top() {
			heap.Pop(r.lower)
		} else {
			r.delLower++
			r.del[del]++
		}
	} else {
		balance++
		if del == r.higher.Top() {
			heap.Pop(r.higher)
		} else {
			r.delHigher++
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

	r.calculate()
}
