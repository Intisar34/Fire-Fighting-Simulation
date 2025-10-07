package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

// New type "Message" for truck-manager communication
type Message map[string]interface{}

func createManager(size int, waterCapacity int, refillRate int) map[string]interface{} {
	grid := make([][]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		grid[i] = make([]map[string]interface{}, size)
		for j := 0; j < size; j++ {
			grid[i][j] = map[string]interface{}{
				"fire":      false,
				"intensity": 0.0,
				"truck":     "",
			}
		}
	}
	return map[string]interface{}{
		"grid":   grid,
		"water":  waterCapacity,
		"max":    waterCapacity,
		"refill": refillRate,
	}
}

// Run the central manager
func runManager(manager map[string]interface{}, stopAfter int, tick time.Duration, done chan struct{},nc * nats.Conn) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	timestep := 0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for range ticker.C {
		timestep++

		if timestep%2 == 0 {
			spreadFires(manager)
		}

		if rand.Float32() < 0.5 {
			addFire(manager, rand.Intn(size), rand.Intn(size),nc) // keep int here
		}

		refillWater(manager)

		fmt.Printf("\n--- TIMESTEP %d ---\n", timestep)
		display(manager)

		if stopAfter > 0 && timestep >= stopAfter {
			fmt.Println("Simulation ended after", stopAfter, "timesteps.")
			close(done)
			return
		}
	}
}

// --- Helper Functions ---
func inBounds(size, x, y int) bool {
	return x >= 0 && x < size && y >= 0 && y < size
}

// FIX: accept int, not float64
func addFire(manager map[string]interface{}, x, y int, nc *nats.Conn) {
	grid := manager["grid"].([][]map[string]interface{})
	if !grid[x][y]["fire"].(bool) {
		intensity := float64(rand.Intn(3) + 1)
		grid[x][y]["fire"] = true
		grid[x][y]["intensity"] = intensity

		// Publish fire update
		msg := Message{
			"type":      "new fire",
			"x":         x,
			"y":         y,
			"intensity": intensity,
		}
		data, _ := json.Marshal(msg)
		err := nc.Publish("fires.updates", data)

		if err != nil {
			fmt.Println("Error publishing fire update:", err)
		} else {
			fmt.Println("🔥 Published new fire at:", x, y)
		}
	}
}



func spreadFires(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			if cell["fire"].(bool) {
				cell["intensity"] = cell["intensity"].(float64) + 1
				if cell["intensity"].(float64) > 10 {
					cell["intensity"] = 10.0
				}
			}
		}
	}
}

func refillWater(manager map[string]interface{}) {
	w := manager["water"].(int) + manager["refill"].(int)
	if w > manager["max"].(int) {
		w = manager["max"].(int)
	}
	manager["water"] = w
}

// Function for absolute value
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// Displays the grid
func display(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	fmt.Print("   ")
	for j := 0; j < size; j++ {
		fmt.Printf("%3d", j)
	}
	fmt.Println()
	for i := 0; i < size; i++ {
		fmt.Printf("%2d ", i)
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			switch {
			case cell["truck"].(string) != "" && cell["fire"].(bool):
				fmt.Print("💧")
			case cell["truck"].(string) != "":
				fmt.Print("🚛")
			case cell["fire"].(bool):
				intensity := int(cell["intensity"].(float64))
				switch {
				case intensity >= 8:
					fmt.Print("🔥")
				case intensity >= 5:
					fmt.Print("🟥")
				case intensity >= 3:
					fmt.Print("🟧")
				default:
					fmt.Print("🟨")
				}
			default:
				fmt.Print("🌲")
			}
			fmt.Print(" ")
		}
		fmt.Println()
	}
	fmt.Println("Water:", manager["water"], "/", manager["max"])
}

// Subscribes to the "truck.requests" subject
func Responder(nc *nats.Conn, manager map[string]interface{}) error {
	_, err := nc.Subscribe("truck.requests", func(msg *nats.Msg) {
		var request Message
		json.Unmarshal(msg.Data, &request)

		reply := handleRequest(manager, request)
		data, _ := json.Marshal(reply)
		msg.Respond(data)
	})
	if err != nil {
		return err
	}

	fmt.Println("Manager is running and listening on 'manager' subject...")
	return nil
}

func handleRequest(manager map[string]interface{}, req Message) Message {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	reply := Message{}

	switch req["type"] {
	case "register":
		x := int(req["x"].(float64))
		y := int(req["y"].(float64))
		id := req["from"].(string)

		if !inBounds(size, x, y) {
			reply["ok"] = false
			reply["info"] = "out of bounds"
			return reply
		}
		if grid[x][y]["truck"].(string) != "" {
			reply["ok"] = false
			reply["info"] = "cell occupied"
			return reply
		}

		grid[x][y]["truck"] = id
		reply["ok"] = true
		reply["info"] = "registered"
		reply["x"] = x
		reply["y"] = y
		return reply

	case "nearest fire":
		x := int(req["x"].(float64))
		y := int(req["y"].(float64))
		minDistance := size * 2
		fireX, fireY := -1, -1

		for i := 0; i < size; i++ {
			for j := 0; j < size; j++ {
				if grid[i][j]["fire"].(bool) {
					distance := abs(i-x) + abs(j-y)
					if distance < minDistance {
						minDistance = distance
						fireX, fireY = i, j
					}
				}
			}
		}

		if fireX != -1 && fireY != -1 {
			reply["ok"] = true
			reply["info"] = "Fire found!"
			reply["x"] = float64(fireX)
			reply["y"] = float64(fireY)
			reply["distance"] = minDistance
			reply["intensity"] = grid[fireX][fireY]["intensity"].(float64)
		} else {
			reply["ok"] = false
			reply["info"] = "No fire on grid"
		}
		return reply

	case "request water":
		id := req["from"].(string)
		amount := int(req["amount"].(float64))
		existingWater := manager["water"].(int)

		if amount <= existingWater {
			manager["water"] = existingWater - amount
			reply["ok"] = true
			reply["info"] = fmt.Sprintf("Truck %s has received %d units of water", id, amount)
			reply["left"] = manager["water"].(int)
		} else if existingWater != 0 {
			manager["water"] = 0
			reply["ok"] = true
			reply["info"] = fmt.Sprintf("Not enough water, truck %s received %d units of water", id, existingWater)
			reply["left"] = 0
		} else {
			reply["ok"] = false
			reply["info"] = "No water available"
			reply["left"] = 0
		}
		return reply

	case "extinguish fire":
		x := int(req["x"].(float64))
		y := int(req["y"].(float64))
		amount := req["amount"].(float64)

		if grid[x][y]["fire"].(bool) {
			intensity := grid[x][y]["intensity"].(float64)
			if amount >= intensity {
				grid[x][y]["fire"] = false
				grid[x][y]["intensity"] = 0.0
				reply["ok"] = true
				reply["info"] = fmt.Sprintf("Fire at %d,%d was extinguished!", x, y)
				reply["remaining"] = 0.0
			} else {
				grid[x][y]["intensity"] = intensity - amount
				reply["ok"] = true
				reply["info"] = fmt.Sprintf("Fire at %d,%d reduced to intensity %.1f!", x, y, grid[x][y]["intensity"].(float64))
				reply["remaining"] = grid[x][y]["intensity"].(float64)
			}
		} else {
			return Message{"ok": false, "info": "No fire at this location"}
		}

		return reply

	case "request move":
		id := req["from"].(string)
		direction := req["direction"].(string)
		truckX, truckY := -1, -1

		for i := 0; i < size; i++ {
			for j := 0; j < size; j++ {
				if grid[i][j]["truck"] == id {
					truckX, truckY = i, j
				}
			}
		}

		newX, newY := truckX, truckY
		switch direction {
		case "n":
			newX--
		case "s":
			newX++
		case "e":
			newY++
		case "w":
			newY--
		}

		if !inBounds(size, newX, newY) {
			return Message{"ok": false, "info": "out of bounds"}
		}
		if grid[newX][newY]["truck"].(string) != "" {
			return Message{"ok": false, "info": "cell occupied"}
		}

		grid[truckX][truckY]["truck"] = ""
		grid[newX][newY]["truck"] = id

		reply["ok"] = true
		reply["info"] = "moved successfully"
		return reply

	default:
		reply["ok"] = false
		reply["info"] = "Unknown request type"
		return reply
	}
}
