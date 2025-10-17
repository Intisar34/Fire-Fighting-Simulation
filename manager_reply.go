package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/nats-io/nats.go"
	// import your Task0 package
)

// Mutex for concurrent access
var mu sync.Mutex

// StartManagerResponder subscribes to the "manager" subject and responds dynamically
func StartManagerResponder(nc *nats.Conn, manager map[string]interface{}) error {
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

// handleRequest reuses all your existing request handling logic dynamically
func handleRequest(manager map[string]interface{}, req Message) Message {
	mu.Lock()
	defer mu.Unlock()

	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	reply := Message{}

	switch req["type"] {

	case "register":
		x := req["x"].(int)
		y := req["y"].(int)
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
		x := req["x"].(int)
		y := req["y"].(int)
		minDistance := math.MaxFloat64
		fireX, fireY := -1, -1

		for i := 0; i < size; i++ {
			for j := 0; j < size; j++ {
				if grid[i][j]["fire"].(bool) {
					dist := math.Hypot(float64(i-x), float64(j-y))
					if dist < minDistance {
						minDistance = dist
						fireX = i
						fireY = j
					}
				}
			}
		}

		if fireX != -1 && fireY != -1 {
			reply["ok"] = true
			reply["info"] = "Fire found!"
			reply["x"] = fireX
			reply["y"] = fireY
			reply["distance"] = minDistance
		} else {
			reply["ok"] = false
			reply["info"] = "No fire on grid"
		}
		return reply

	case "request water":
		id := req["from"].(string)
		amount := req["amount"].(int)
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
		x := req["x"].(int)
		y := req["y"].(int)
		amount := req["amount"].(int)

		if grid[x][y]["fire"].(bool) {
			intensity := grid[x][y]["intensity"].(int)
			if amount >= intensity {
				grid[x][y]["fire"] = false
				grid[x][y]["intensity"] = 0
				reply["ok"] = true
				reply["info"] = fmt.Sprintf("Fire at %d,%d was extinguished!", x, y)
				reply["remaining"] = 0

			} else {
				grid[x][y]["intensity"] = intensity - amount
				reply["ok"] = true
				reply["info"] = fmt.Sprintf("Fire at %d,%d reduced to intensity %d!", x, y, grid[x][y]["intensity"].(int))
				reply["remaining"] = grid[x][y]["intensity"]
			}
		} else {
			reply["ok"] = false
			reply["info"] = "No fire at this location"
		}
		return reply

	default:
		reply["ok"] = false
		reply["info"] = "unknown request type"
		return reply
	}
}
