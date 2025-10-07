package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	//Establishes NATS connection
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	// Create the central manager (with arbitrary example values)
	manager := createManager(20, 500, 20)

	//Starts the NATS based responder
	err = Responder(nc, manager)
	if err != nil {
		panic(err)
	}

	// Create the 'done' channel to signal when the simulation is over
	done := make(chan struct{})

	// Run the manager for 50 timesteps with 2 seconds per timestep
	go runManager(manager, 50, 2*time.Second, done,nc)

	// Create two initial fires at random grid positions (example)
	addFire(manager, rand.Intn(20), rand.Intn(20),nc)
	addFire(manager, rand.Intn(20), rand.Intn(20),nc)

	// Create, register, and run 2 firetrucks at random grid positions (example)
	truck1 := createTruck("T1", float64(rand.Intn(20)), float64(rand.Intn(20)), nc)
	truck2 := createTruck("T2", float64(rand.Intn(20)), float64(rand.Intn(20)), nc)

	ListenForFires(truck1)
    ListenForFires(truck2)

	if _, err := registerTruck(truck1); err != nil {
		fmt.Println("Error registering truck1:", err)
	}
	if _, err := registerTruck(truck2); err != nil {
		fmt.Println("Error registering truck2:", err)
	}

	ListenForFires(truck1)
    ListenForFires(truck2)
	
	go truckLoop(truck1, done)
	go truckLoop(truck2, done)

	// Finish simulation once the manager signals completion
	<-done
}
