package main

import (
    "fmt"
    "math/rand"
    "time"
    "github.com/nats-io/nats.go"
)

func main() {
    rand.Seed(time.Now().UnixNano())

    // NATS connection
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        panic(err)
    }
    defer nc.Drain()

    // Create grid
    gridmap := createGrid(20, maxGlobalWater, 20)

    // Spawn fires and trucks
    spawnFires(gridmap, 5)
    numTrucks := 5
    trucks := spawnTrucks(gridmap, numTrucks, nc)

    // Each truck listens for water requests once
    for i := range trucks {
        trucks[i].ListenForWaterRequests()
    }

    // Subscribe to fire-extinguished events
    nc.Subscribe("fire.extinguished", func(m *nats.Msg) {
        fmt.Printf("📢 Event: %s\n", string(m.Data))
    })

    // Simulation loop
    timesteps := 50
    for t := 0; t < timesteps; t++ {
        waterDeliveredThisStep = 0
        fmt.Printf("\n⏱ Time step %d\n", t+1)

        for i := range trucks {
            if near, fx, fy := isNearFire(gridmap, trucks[i]); near {
                extinguishFire(gridmap, &trucks[i], fx, fy, nc)
            } else {
                moveTruckRandomly(gridmap, &trucks[i])
            }
        }

        display(gridmap)
        fmt.Printf("Water: %.0f / %.0f\n", globalWater, maxGlobalWater)
        time.Sleep(1 * time.Second)
    }

    fmt.Println("Simulation finished.")
}