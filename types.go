package main

import "github.com/nats-io/nats.go"

type FireTruck struct {
	ID    string
	X, Y  int
	Conn  *nats.Conn
	Busy  bool
	Clock *LamportClock // Added Lamport clock
}

type FireClaim struct {
	TruckID   string  `json:"truck_id"`
	FireX     int     `json:"fire_x"`
	FireY     int     `json:"fire_y"`
	Distance  float64 `json:"distance"`
	Timestamp int     `json:"timestamp"`
}

var (
	globalWater            = 300.0
	maxGlobalWater         = 300.0
	numTrucks              = 5
	maxWaterPerTimestep    = 50.0
	waterDeliveredThisStep = 0.0
	activeTrucks = make(map[string]bool) // to handle truck failures
	missedResponses = make(map[string]int) // tracks consecutive missed replies for each truck

)
