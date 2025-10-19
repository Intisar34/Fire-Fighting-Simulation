package main

import "sync"

// Each FireTruck instance will have its own clock.
type LamportClock struct {
	mu   sync.Mutex
	time int
}

// Increment increases the local clock for an event.
func (lc *LamportClock) Increment() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.time++
	return lc.time
}

// Update adjusts the local clock when receiving a timestamp from another truck.
func (lc *LamportClock) Update(received int) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if received > lc.time {
		lc.time = received
	}
	lc.time++
	return lc.time
}

// Time returns the current clock value.
func (lc *LamportClock) Time() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.time
}
