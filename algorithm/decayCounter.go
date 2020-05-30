package algorithm

// Heavily insipred by
// https://github.com/ceph/ceph/blob/25ac1528419371686740412616145703810a561f/src/common/DecayCounter.h
// TODO: add GNU license

import (
	"math"
	"time"
)

// DecayCounter expontionally decays, must Reset() before use
type DecayCounter struct {
	k         float64 // k = ln(.5)/half_life
	val       float64 // the counter value
	lastDecay time.Time
}

// NewDecayCounter counstucts a NewDecayCounter
func NewDecayCounter(halflife float64) *DecayCounter {
	dc := &DecayCounter{}
	dc.setHalflife(halflife)
	return dc
}

func (dc *DecayCounter) setHalflife(hl float64) {
	dc.k = math.Log(.5) / hl
}

func (dc *DecayCounter) getHalflife() float64 {
	return math.Log(.5) / dc.k
}

func (dc *DecayCounter) decay(delta float64) {
	now := time.Now()
	el := float64(now.Sub(dc.lastDecay))

	// calculate new value
	newval := dc.val*math.Exp(el*dc.k) + delta
	if newval < .01 {
		newval = 0.0
	}

	dc.val, dc.lastDecay = newval, time.Now()
}

// Get the counter
func (dc *DecayCounter) Get() float64 {
	dc.decay(0.0)
	return dc.val
}

// GetRaw value without decaying
func (dc *DecayCounter) GetRaw() float64 {
	return dc.val
}

// GetLastDecay the time when last decay occurs
func (dc *DecayCounter) GetLastDecay() time.Time {
	return dc.lastDecay
}

// Increment the counter
func (dc *DecayCounter) Increment(v float64) float64 {
	dc.decay(v)
	return dc.val
}

// Hit the counter, i.e. increment by 1
func (dc *DecayCounter) Hit() float64 {
	return dc.Increment(1.0)
}

// Scale the counter, does not decay
func (dc *DecayCounter) Scale(v float64) float64 {
	dc.val *= v
	return dc.val
}

// Reset the counter
func (dc *DecayCounter) Reset() {
	dc.lastDecay = time.Now()
	dc.val = 0
}
