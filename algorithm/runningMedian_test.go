package algorithm

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/GIFTS-fs/GIFTS/test"
)

func TestRunningMedianAdd(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	as := func(got, want float64, msg string) {
		af(want == got, fmt.Sprintf("%q: Want %f Got %f", msg, want, got))
	}

	var running *RunningMedian
	var data []float64

	// A good old base case: [1]
	running = NewRunningMedian()
	running.Add(1)
	as(running.Median(), 1, "1")

	// [1] * 1000
	running = NewRunningMedian()
	for i := 0; i < 1000; i++ {
		running.Add(1)
	}
	as(running.Median(), 1, "1000 * 1")

	// [1, 2, 3, 4]
	running = NewRunningMedian()
	for i := 1; i <= 4; i++ {
		running.Add(float64(i))
	}
	as(running.Median(), 2.5, "1 2 3 4")

	// [1, 2, 3, 4, 5]
	running = NewRunningMedian()
	for i := 1; i <= 5; i++ {
		running.Add(float64(i))
	}
	as(running.Median(), 3, "1 2 3 4 5")

	// [0...999]
	running = NewRunningMedian()
	for i := 0; i < 999; i++ {
		running.Add(float64(i))
	}
	as(running.Median(), 999/2, "999/2")

	// [999...0]
	running = NewRunningMedian()
	for i := 999 - 1; i >= 0; i-- {
		running.Add(float64(i))
	}
	as(running.Median(), 999/2, "999/2")

	// [0...999] but shuffled
	running = NewRunningMedian()
	for i := 0; i < 999; i++ {
		data = append(data, float64(i))
	}
	rand.Shuffle(999, func(i, j int) { data[i], data[j] = data[j], data[i] })
	for _, f := range data {
		running.Add(f)
	}
	as(running.Median(), 999/2, "999/2")
}

func TestRunningMedianUpdate(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	as := func(got, want float64, msg string) {
		af(want == got, fmt.Sprintf("%q: Want %f Got %f", msg, want, got))
	}

	var running *RunningMedian
	var data, window []float64

	// [1] -> [1]
	running = NewRunningMedian()
	running.Add(1)
	as(running.Median(), 1, "1")
	running.Update(1, 1)
	as(running.Median(), 1, "1")

	// [0] -> [1] -> ... -> [1000]
	running = NewRunningMedian()
	running.Add(0)
	for i := 0; i < 1000; i++ {
		running.Update(float64(i), float64(i+1))
	}
	as(running.Median(), 1000, "1000")

	// [0, 123] -> [1000, 123]
	running = NewRunningMedian()
	running.Add(0)
	running.Add(123)
	for i := 0; i < 1000; i++ {
		running.Update(float64(i), float64(i+1))
	}
	as(running.Median(), 0.5*1123, "1000, 123")

	// [1] * 1000 -> [1] * 1000 -> [2] * 1000
	running = NewRunningMedian()
	for i := 0; i < 1000; i++ {
		running.Add(1)
	}
	as(running.Median(), 1, "1000 * 1")
	for i := 0; i < 1000; i++ {
		running.Update(1, 1)
	}
	as(running.Median(), 1, "1000 * 1")
	for i := 0; i < 1000; i++ {
		running.Update(1, 2)
	}
	as(running.Median(), 2, "1000 * 2")

	// [1] * 1000 -> [1,3] * 500 -> [1,3] * 500 + [2]
	running = NewRunningMedian()
	for i := 0; i < 1000; i++ {
		running.Add(1)
	}
	for i := 0; i < 500; i++ {
		running.Update(1, 1)
		running.Update(1, 3)
	}
	running.Add(2)
	as(running.Median(), 2, "500 * 1, 500 * 3, 1 * 2")

	// [0...999] but shuffled
	// sliding window size 5
	running = NewRunningMedian()
	data = []float64{}
	window = []float64{}
	for i := 0; i < 999; i++ {
		data = append(data, float64(i))
	}
	rand.Shuffle(999, func(i, j int) { data[i], data[j] = data[j], data[i] })
	for i := 0; i < 5; i++ {
		running.Add(data[i])
		window = append(window, data[i])
	}
	sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
	as(running.Median(), window[2], "window size 5")
	for i := 5; i < 999; i++ {
		running.Update(data[i-5], data[i])
		for j := range window {
			if window[j] == data[i-5] {
				window[j] = data[i]
				sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
				break
			}
		}
		as(running.Median(), window[2], "window size 5")
	}

	// [0...999] but shuffled
	// sliding window size 6
	running = NewRunningMedian()
	data = []float64{}
	window = []float64{}
	for i := 0; i < 999; i++ {
		data = append(data, float64(i))
	}
	rand.Shuffle(999, func(i, j int) { data[i], data[j] = data[j], data[i] })
	for i := 0; i < 6; i++ {
		running.Add(data[i])
		window = append(window, data[i])
	}
	sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
	as(running.Median(), 0.5*(window[2]+window[3]), "window size 6")
	for i := 6; i < 999; i++ {
		running.Update(data[i-6], data[i])
		for j := range window {
			if window[j] == data[i-6] {
				window[j] = data[i]
				sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
				break
			}
		}
		as(running.Median(), 0.5*(window[2]+window[3]), "window size 6")
	}
}

func TestRunningMedianDelete(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	as := func(got, want float64, msg string) {
		af(want == got, fmt.Sprintf("%q: Want %f Got %f", msg, want, got))
	}

	var running *RunningMedian

	// [1] -> [1,2] -> [2]
	running = NewRunningMedian()
	running.Add(1)
	as(running.Median(), 1, "1")
	running.Add(2)
	as(running.Median(), 1.5, "1.5")
	running.Delete(1)
	as(running.Median(), 2, "2")

	// [1] -> [1,2] -> [1]
	running = NewRunningMedian()
	running.Add(1)
	as(running.Median(), 1, "1")
	running.Add(2)
	as(running.Median(), 1.5, "1.5")
	running.Delete(2)
	as(running.Median(), 1, "1")

	// [1] -> [1,2] -> [1] -> [] -> [3]
	running = NewRunningMedian()
	running.Add(1)
	as(running.Median(), 1, "1")
	running.Add(2)
	as(running.Median(), 1.5, "1.5")
	running.Delete(2)
	as(running.Median(), 1, "1")
	running.Delete(1)
	running.Add(3)
	as(running.Median(), 3, "3")

	// TODO!!! add more tests
}

func TestRunningMedian_Add(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	var result float64
	rm := NewRunningMedian()

	// RunningMedian with one data point: [10]
	rm.Add(10)
	result = rm.Median()
	af(result == 10, fmt.Sprintf("Expected %f, received %f", 10.0, result))

	// Running Median with two data points: [3 10]
	rm.Add(3)
	result = rm.Median()
	af(result == 6.5, fmt.Sprintf("Expected %f, received %f", 6.5, result))

	// Running Median with three data points: [3 7 10]
	rm.Add(7)
	result = rm.Median()
	af(result == 7, fmt.Sprintf("Expected %f, received %f", 7.0, result))

	// Running Median with four data points: [1 3 7 10]
	rm.Add(1)
	result = rm.Median()
	af(result == 5, fmt.Sprintf("Expected %f, received %f", 5.0, result))

	// Running Median with five data points: [1 3 7 10 20]
	rm.Add(20)
	result = rm.Median()
	af(result == 7, fmt.Sprintf("Expected %f, received %f", 7.0, result))
}

func TestRunningMedian_Update(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	var result float64
	rm := NewRunningMedian()

	// RunningMedian with one data point: [] -> [10] -> [11]
	rm.Add(10)
	rm.Update(10, 11)
	result = rm.Median()
	af(result == 11, fmt.Sprintf("Expected %f, received %f", 11.0, result))
	af(rm.lower.data[0] == 11.0, fmt.Sprintf("Expected %f, received %f", 11.0, rm.lower.data[0]))

	// Running Median with two data points: [11] -> [3 11] -> [4 11]
	rm.Add(3)
	rm.Update(3, 4)
	result = rm.Median()
	af(result == 7.5, fmt.Sprintf("Expected %f, received %f", 7.5, result))
	af(rm.lower.data[0] == 4.0, fmt.Sprintf("Expected %f, received %f", 4.0, rm.lower.data[0]))
	af(rm.higher.data[0] == 11.0, fmt.Sprintf("Expected %f, received %f", 11.0, rm.higher.data[0]))

	// Running Median with three data points: [4 11] -> [4 7 11] -> [4 8 11]
	rm.Add(7)
	rm.Update(7, 8)
	result = rm.Median()
	af(result == 8, fmt.Sprintf("Expected %f, received %f", 8.0, result))
	af(rm.lower.data[0] == 8.0, fmt.Sprintf("Expected %f, received %f", 8.0, rm.lower.data[0]))
	af(rm.lower.data[1] == 4.0, fmt.Sprintf("Expected %f, received %f", 4.0, rm.lower.data[1]))
	af(rm.higher.data[0] == 11.0, fmt.Sprintf("Expected %f, received %f", 11.0, rm.higher.data[0]))

	// Running Median with four data points: [4 8 11] -> [2 4 8 11] -> [1 4 8 11]
	rm.Add(2)
	result = rm.Median()
	af(result == 6, fmt.Sprintf("Expected %f, received %f", 6.0, result))

	rm.Update(2, 1)
	t.Log(rm.lower.data)
	t.Log(rm.higher.data)
	result = rm.Median()
	af(result == 6, fmt.Sprintf("Expected %f, received %f", 6.0, result))

	// Running Median with five data points: [1 4 8 11] -> [1 4 8 11 19] -> [1 4 8 11 20]
	rm.Add(19)
	result = rm.Median()
	af(result == 8, fmt.Sprintf("Expected %f, received %f", 8.0, result))

	rm.Update(19, 20)
	result = rm.Median()
	af(result == 8, fmt.Sprintf("Expected %f, received %f", 8.0, result))
}
