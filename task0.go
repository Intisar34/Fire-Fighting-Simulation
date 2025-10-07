package main

import (
	"fmt"
	"math/rand"
	"time"
)

/*
================================= Task 0 Template =================================
This is a very basic, minimal template that aims to provide a starting point for
Task 0 of the firetruck simulation system.

In later tasks, you will need to extend the simulation system to include:
- Distributed messaging (Task 1)
- Coordination and logical clocks (Task 2)
- Decentralized strategies (Task 3)

Currently implemented features:
- 20x20 grid with fires and trucks
- Fires can randomly ignite and spread/intensify
- Firetrucks can be created and registered on the grid
  (extend to include the rest of the firetruck functinoalities)
- Central manager for handling messages
  (this should be changed later when implementing distributed communication)
====================================================================================
*/

// New type "Message" for truck-manager communication
type Message map[string]interface{}

// ----------------------- Central manager -----------------------
// Create manager
func createManager(size int, waterCapacity int, refillRate int) map[string]interface{} {
	grid := make([][]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		grid[i] = make([]map[string]interface{}, size)
		for j := 0; j < size; j++ {
			grid[i][j] = map[string]interface{}{
				"fire":      false,
				"intensity": 0,
				"truck":     "",
			}
		}
	}
	return map[string]interface{}{
		"grid":   grid,
		"water":  waterCapacity,
		"max":    waterCapacity,
		"refill": refillRate,
		"inbox":  make(chan Message, 100), // This is where the messages from the trucks are sent to. It can hold up to 100 messages
	}
}

// Run the central manager, which is responsible for what's happening on the grid
func runManager(manager map[string]interface{}, stopAfter int, tick time.Duration, done chan struct{}) {
	inbox := manager["inbox"].(chan Message)
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	timestep := 0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		timestep++
		// Handle messages from trucks
		drainMessages(manager)

		// Spread fires every 2 timesteps
		if timestep%2 == 0 {
			spreadFires(manager)
		}

		// Random new fire
		if rand.Float32() < 0.5 {
			addFire(manager, rand.Intn(size), rand.Intn(size))
		}

		// Refill global water supply so that it doesn't just run out
		refillWater(manager)

		// Display grid
		fmt.Printf("\n--- TIMESTEP %d ---\n", timestep)
		display(manager)

		// Wait until next tick
		waitUntil := time.Now().Add(tick)
		for time.Now().Before(waitUntil) {
			select {
			case msg := <-inbox:
				handleMessage(manager, msg)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}

		if stopAfter > 0 && timestep >= stopAfter {
			fmt.Println("Simulation ended after", stopAfter, "timesteps.")
			close(done) // Signal to main that manager is finished (all timesteps completed)
			return
		}
	}
}

// Drain messages from the manager's inbox; it is called at the start of each timestep (tick),
// meaning that the manager processes all pending truck requests at the start of each timestep (tick)
func drainMessages(manager map[string]interface{}) {
	inbox := manager["inbox"].(chan Message)
	for {
		select {
		case msg := <-inbox:
			handleMessage(manager, msg)
		default:
			return
		}
	}
}

// Handle messages (placeholder)
func handleMessage(manager map[string]interface{}, msg Message) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	switch msg["type"] {
	case "register":
		/*
			This example implementation has a central-manager approach.
			For Task 1, you will replace the synchronous channel-based
			truck registration with a network handshake (TCP/NATS).
		*/
		// Truck requests to register at position (x, y) on the grid
		x, y := msg["x"].(int), msg["y"].(int)
		id := msg["from"].(string)

		// Validate grid bounds and cell occupancy
		if !inBounds(size, x, y) {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "out of bounds"}
			}
			return
		}
		if grid[x][y]["truck"].(string) != "" {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "cell occupied"}
			}
			return
		}

		// Place truck on grid
		grid[x][y]["truck"] = id

		if msg["reply"] != nil {
			msg["reply"].(chan Message) <- Message{"ok": true, "info": "registered"}
		}
		return

	case "nearest fire":

		// Manager extracts x and y coordinates of the truck
		// These come from the message sent by the truck

		x, y := msg["x"].(int), msg["y"].(int)

		minDistance := size * 2
		
		// Store the coordinates of the nearest fire (initialized to invalid -1)

		fireX, fireY := -1, -1

		for i := 0; i < size; i++ {
			for j := 0; j < size; j++ {
				if grid[i][j]["fire"].(bool) {
					distance := manhattanDistance(x, y, i, j) // use Absolute value for positive coordinates
					if distance < minDistance {
						minDistance = distance
						fireX, fireY = i, j
					}
				}
			}

		}
        // If fire is found in that cell
		if fireX != -1 && fireY != -1 {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{
					"ok":   true,
					"info": "Fire found!",
					"x":    fireX,
					"y":    fireY,
				}
			}
		} else {
			msg["reply"].(chan Message) <- Message{
				"ok":   false,
				"info": "No fire on grid"}
		}

		return
    
	case "request water":

		id := msg["from"].(string)
		amount := msg["amount"].(int) // Extract the amount of water the truck is requesting

		existingWater := manager["water"].(int)

		if amount <= existingWater {
			manager["water"] = existingWater - amount
			// Send a confirmation reply to the truck 
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{
					"ok":   true,
					"info": fmt.Sprintf("Truck %s has received %d units of water", id, amount),
					"left": manager["water"].(int),
				}
			}
          //Not enough water to fulfill the full request, but some water is available
		} else if amount > existingWater && existingWater != 0 {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{
					"ok":   true,
					"info": fmt.Sprintf("Not enough water, truck %s received %d units of water", id, existingWater),
					"left": 0,
				}

			}

		} else {
			//No water is available at all
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{
					"ok":   false,
					"info": "No water available",
					"left": 0,
				}
			}
		}
		return

	case "extinguish fire":

		x, y := msg["x"].(int), msg["y"].(int)
		amount := msg["amount"].(int)

		if grid[x][y]["fire"].(bool) {
			intensity := grid[x][y]["intensity"].(int)

			if amount >= intensity {

				grid[x][y]["fire"] = false
				grid[x][y]["intensity"] = 0

				if msg["reply"] != nil {
					msg["reply"].(chan Message) <- Message{
						"ok":   true,
						"info": fmt.Sprintf("Fire at %d,%d was extinguished!", x, y),
					}
				}

			} else {
				grid[x][y]["intensity"] = intensity - amount

				if msg["reply"] != nil {
					msg["reply"].(chan Message) <- Message{
						"ok":             true,
						"info":           fmt.Sprintf("Fire at %d,%d was reduced to intensity %d!", x, y, grid[x][y]["intensity"].(int)),
						"intensity_left": grid[x][y]["intensity"],
					}
				}

			}
		} else {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{
					"ok":   false,
					"info": "No fire at this location",
				}
			}
		}
		return

	

	case "move":
		id := msg["from"].(string)
	    direction := msg["direction"].(string)

		truckX,truckY := -1,-1

		for i := 0; i < size; i++{
			for j := 0; j <size; j++{
				if grid[i][j]["truck"] == id{
					truckX,truckY = i,j

				}
			}
		}

		newX,newY := truckX,truckY

		switch direction{
		case "n" :
			newX = newX - 1
		case "s" :
			newX = newX + 1
		case "e":
			newY = newY + 1
		case "w":
			newY = newY - 1
		}

		if !inBounds(size, newX, newY) {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "out of bounds"}
			}
			return
		}
			
		if grid[newX][newY]["truck"].(string) != "" {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "cell occupied"}
			}
			return
		}

		grid[truckX][truckY]["truck"] = ""
		grid[newX][newY]["truck"] = id

		if msg["reply"] != nil {
			msg["reply"].(chan Message) <- Message{"ok": true, "info": "moved successfully"}
		}
		return

		default:
		 msg["reply"].(chan Message) <- Message{"ok": false}
	}
}

// ------------------- Manager helper functions -------------------
// TODO for Task 0: add/modify/expand as needed

func inBounds(size, x, y int) bool {
	return x >= 0 && x < size && y >= 0 && y < size
}

// Add new fire to the grid
func addFire(manager map[string]interface{}, x, y int) {
	grid := manager["grid"].([][]map[string]interface{})
	if !grid[x][y]["fire"].(bool) {
		grid[x][y]["fire"] = true
		grid[x][y]["intensity"] = rand.Intn(3) + 1
	}
}

// Spread fires, increases existing fires' intensity (basic implementation)
func spreadFires(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			if cell["fire"].(bool) {
				cell["intensity"] = cell["intensity"].(int) + 1
				if cell["intensity"].(int) > 10 { // Arbitrarily cap fire intensity at 10
					cell["intensity"] = 10
				}
			}
		}
	}
}
// function for absolute valiue
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// calculate manhattan distance using the absolute value.
func manhattanDistance(x1, y1, x2, y2 int) int {
	return abs(x1-x2) + abs(y1-y2)
}

// Refill the global shared water supply
func refillWater(manager map[string]interface{}) {
	w := manager["water"].(int) + manager["refill"].(int)
	if w > manager["max"].(int) {
		w = manager["max"].(int)
	}
	manager["water"] = w
}

// ----------------------- Display -----------------------
func display(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	fmt.Print("   ")
	for j := 0; j < size; j++ {
		fmt.Printf("%3d", j) // Column numbers
	}
	fmt.Println()
	for i := 0; i < size; i++ {
		fmt.Printf("%2d ", i) // Row numbers
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			switch {
			case cell["truck"].(string) != "" && cell["fire"].(bool):
				fmt.Print("💧") // Truck fighting fire
			case cell["truck"].(string) != "":
				fmt.Print("🚛") // Truck
			case cell["fire"].(bool):
				intensity := cell["intensity"].(int)
				// Fire intensity symbols and levels with arbitrary values
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
				fmt.Print("🌲") // Forest cell with no fire or truck on it
			}
			fmt.Print(" ")
		}
		fmt.Println()
	}
	// Print the current water supply level
	fmt.Println("Water:", manager["water"], "/", manager["max"])
}

// ----------------------- Firetruck -----------------------
// Add/modify/expand as needed

// Create a firetruck - note: this creates a local truck but does not register it to the grid
func createTruck(id string, x, y int, inbox chan Message) map[string]interface{} {
	return map[string]interface{}{
		"id":    id,
		"x":     x,
		"y":     y,
		"mgr":   inbox,
		"reply": make(chan Message, 1),
	}
}

// Register a firetruck to the grid via the manager
func registerTruck(truck map[string]interface{}) {
	msg := Message{
		"type":  "register",
		"from":  truck["id"],
		"x":     truck["x"],
		"y":     truck["y"],
		"reply": truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	<-truck["reply"].(chan Message)
}
//Request the nearest fire to the manager
func requestNearestFire(truck map[string]interface{}) Message {

	msg := Message{
		"type":  "nearest fire",
		"from":  truck["id"],
		"x":     truck["x"],
		"y":     truck["y"],
		"reply": truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	response := <-truck["reply"].(chan Message)

	return response
}
// request to move in the direction required to the manager
func requestMove(truck map[string]interface{}, direction string) Message {
	msg := Message{
		"type":  "move",
		"from":  truck["id"],
		"direction": direction,
		"reply":     truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	reply := <-truck["reply"].(chan Message)
	return reply
}
// request water to the manager
func requestWater(truck map[string]interface{}, amount int) Message {

	msg := Message{
		"type":   "request water",
		"from":   truck["id"],
		"amount": amount,
		"reply":  truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	response := <-truck["reply"].(chan Message)

	return response

}
// decide the direction the truck will move based on the position of the fire
func decideDirection(truckX, truckY, fireX, fireY int) string{
	if truckX < fireX{
		return "s"
	} else if truckX > fireX {
		return "n"
	} else if truckY < fireY {
		return "e"
	} else if truckY > fireY {
		return "w"
	}
	return ""
}

func extinguishFire(truck map[string]interface{}, amount int) Message {

	msg := Message{
		"type":   "extinguish fire",
		"x":      truck["x"],
		"y":      truck["y"],
		"amount": amount,
		"reply":  truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	response := <-truck["reply"].(chan Message)

	return response
}

// Skeleton of firetruck actions
func truckLoop(truck map[string]interface{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {

		fireResponse := requestNearestFire(truck)

        if !fireResponse["ok"].(bool) {
          fmt.Printf("No fire found for truck %v\n", truck["id"])
          continue
}

		fireX := fireResponse["x"].(int)
		fireY := fireResponse["y"].(int)

		truckX := truck["x"].(int)
		truckY := truck["y"].(int)
		
		decision := decideDirection(truckX,truckY, fireX, fireY)

	if decision == "" {
			// Fire reached → extinguish it
			fmt.Printf("🔥 Truck %v is at the fire location! Extinguishing...\n", truck["id"])
			waterResp := requestWater(truck, 3)
			fmt.Printf("💧 Water request: %v\n", waterResp)

			extResp := extinguishFire(truck, 3)
			fmt.Printf("🧯 Extinguish result: %v\n", extResp)
			continue
		}

		moveResponse := requestMove(truck,decision)

	if moveResponse["ok"].(bool) {
	   switch decision {
	    case "n":
		  truck["x"] = truck["x"].(int) - 1
	    case "s":
		  truck["x"] = truck["x"].(int) + 1
	    case "e":
		  truck["y"] = truck["y"].(int) + 1
	    case "w":
		  truck["y"] = truck["y"].(int) - 1
	}
}
	fmt.Printf("➡️ Truck %v moved %v: %v\n", truck["id"], decision, moveResponse)
	}
	}


// ----------------------- Main -----------------------
func main() {
	rand.Seed(time.Now().UnixNano())

	// Create the central manager (with arbitrary example values)
	manager := createManager(20, 500, 20)

	// Create the 'done' channel to signal when the simulation is over
	done := make(chan struct{})

	// Run the manager for 50 timesteps with 2 seconds per timestep
	go runManager(manager, 50, 2*time.Second, done)

	// Create two initial fires at random grid positions (example)
	addFire(manager, rand.Intn(20), rand.Intn(20))
	addFire(manager, rand.Intn(20), rand.Intn(20))

	// Create, register, and run 2 firetrucks at random grid positions (example)
	truck1 := createTruck("T1", rand.Intn(20), rand.Intn(20), manager["inbox"].(chan Message))
	truck2 := createTruck("T2", rand.Intn(20), rand.Intn(20), manager["inbox"].(chan Message))

	registerTruck(truck1)
	registerTruck(truck2)

	go truckLoop(truck1)
	go truckLoop(truck2)

	// Finish simulation once the manager signals completion
	<-done
}
