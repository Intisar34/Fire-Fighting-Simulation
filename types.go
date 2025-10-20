package main

import "github.com/nats-io/nats.go"

type FireTruck struct {
	ID    string
	X, Y  int
	Conn  *nats.Conn
	Busy  bool
	Clock *LamportClock // Added Lamport clock
}

var (
	globalWater            = 300.0
	maxGlobalWater         = 300.0
	numTrucks              = 5
	maxWaterPerTimestep    = 50.0
	waterDeliveredThisStep = 0.0
)
