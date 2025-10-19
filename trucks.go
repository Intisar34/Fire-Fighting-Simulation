package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

type FireTruck struct {
	ID    string
	X, Y  int
	Conn  *nats.Conn
	Busy  bool
	Clock *LamportClock // Added Lamport clock
}

var globalWater = 300.0
var maxGlobalWater = 300.0
var totalTrucks = 5
var maxWaterPerTimestep = 50.0
var waterDeliveredThisStep = 0.0

func createGrid(size int, waterCapacity float64, refillRate int) map[string]interface{} {
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

// Displays the grid
func display(gridmap map[string]interface{}) {
	grid := gridmap["grid"].([][]map[string]interface{})
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
}

// Creates random fires on the grid
func spawnFires(gridmap map[string]interface{}, numFires int) {
	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)

	for i := 0; i < numFires; i++ {
		x := rand.Intn(size)
		y := rand.Intn(size)
		cell := grid[x][y]
		cell["fire"] = true
		cell["intensity"] = float64(rand.Intn(10) + 1)
	}
}

// Creates trucks on the grid
func spawnTrucks(gridmap map[string]interface{}, numTrucks int, nc *nats.Conn) []FireTruck {
	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)
	trucks := make([]FireTruck, numTrucks)

	for i := 0; i < numTrucks; i++ {
		x := rand.Intn(size)
		y := rand.Intn(size)
		grid[x][y]["truck"] = fmt.Sprintf("T%d", i+1)
		trucks[i] = FireTruck{
			ID:    fmt.Sprintf("T%d", i+1),
			X:     x,
			Y:     y,
			Conn:  nc,
			Clock: &LamportClock{}, // Added clock initialization
		}
	}

	return trucks
}

func moveTruckRandomly(gridmap map[string]interface{}, truck *FireTruck) {

	if truck.Busy {
		return
	}

	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)

	// Clear old position
	grid[truck.X][truck.Y]["truck"] = ""

	// Random direction: 0=up, 1=down, 2=left, 3=right
	dir := rand.Intn(4)
	switch dir {
	case 0:
		if truck.X > 0 {
			truck.X = truck.X - 1
		}
	case 1:
		if truck.X < size-1 {
			truck.X = truck.X + 1
		}
	case 2:
		if truck.Y > 0 {
			truck.Y = truck.Y - 1
		}
	case 3:
		if truck.Y < size-1 {
			truck.Y = truck.Y + 1
		}
	}

	// Update new position
	grid[truck.X][truck.Y]["truck"] = truck.ID

	// Log move with Lamport timestamp
	timestamp := truck.Clock.Increment()
	fmt.Printf("[%d] %s moved to (%d,%d)\n", timestamp, truck.ID, truck.X, truck.Y)
}

func checkForFire(gridmap map[string]interface{}, truck FireTruck) bool {
	grid := gridmap["grid"].([][]map[string]interface{})
	cell := grid[truck.X][truck.Y]
	return cell["fire"].(bool)
}

// Listens for water requests from other trucks
func (t *FireTruck) ListenForWaterRequests() {
	t.Conn.Subscribe("water.request", func(msg *nats.Msg) {
		var req map[string]interface{}
		json.Unmarshal(msg.Data, &req)
		requester := req["truck_id"].(string)
		needed := req["needed"].(float64)
		requestID := req["request_id"].(string)
		receivedTS := int(req["timestamp"].(float64)) // added timestamp field

		// Update Lamport clock on receive
		t.Clock.Update(receivedTS)

		// Ignore if this truck made the request
		if requester == t.ID {
			return
		}

		status := "approved"

		// Mutual exclusion using Lamport timestamps + truck ID tie-break
		if receivedTS > t.Clock.Time() || (receivedTS == t.Clock.Time() && requester > t.ID) {
			status = "denied"
		}

		reply := map[string]interface{}{
			"approver":   t.ID,
			"request_id": requestID,
			"status":     status,
			"timestamp":  t.Clock.Increment(), // include timestamp in reply
		}

		data, _ := json.Marshal(reply)
		msg.Respond(data)
		fmt.Printf("[%d] 💬 %s replied '%s' to %s’s request.\n", t.Clock.Time(), t.ID, status, requester)
	})
}

func (t *FireTruck) RequestWater(amount float64) bool {
	timestamp := t.Clock.Increment()
	fmt.Printf("[%d] %s requesting %.0f units of water\n", timestamp, t.ID, amount)

	request := map[string]interface{}{
		"truck_id":   t.ID,
		"needed":     amount,
		"request_id": fmt.Sprintf("%s-%d", t.ID, time.Now().UnixNano()),
		"timestamp":  timestamp, // include timestamp
	}
	data, _ := json.Marshal(request)

	// Subscribe to the reply channel for this request
	sub, _ := t.Conn.SubscribeSync(fmt.Sprintf("water.reply.%s", request["request_id"]))
	defer sub.Unsubscribe()

	// Publish the request to the "water.request" subject
	t.Conn.PublishRequest("water.request", fmt.Sprintf("water.reply.%s", request["request_id"]), data)

	approvals := 0
	denials := 0
	timeout := time.After(1 * time.Second)

	// Track which trucks have replied
	repliedTrucks := make(map[string]bool)

	// Collect replies from other trucks for this water request.
collectLoop:
	for {
		select {
		case <-timeout:
			break collectLoop
		default:

			// Waits for the next message with a short timeout
			msg, err := sub.NextMsg(200 * time.Millisecond)
			if err != nil {
				continue
			}

			var reply map[string]interface{}
			json.Unmarshal(msg.Data, &reply)

			// Update Lamport clock with reply timestamp
			if ts, ok := reply["timestamp"].(float64); ok {
				t.Clock.Update(int(ts))
			}

			truckID := reply["approver"].(string)
			if repliedTrucks[truckID] {
				continue
			}
			repliedTrucks[truckID] = true

			if reply["status"] == "approved" {
				approvals++
			} else {
				denials++
			}

			fmt.Printf("[%d] %s received '%s' from %s\n", t.Clock.Time(), t.ID, reply["status"], truckID)
		}
	}

	requiredApprovals := 3
	fmt.Printf("[%d] 📊 %s got %d approvals / %d denials\n", t.Clock.Time(), t.ID, approvals, denials)

	if approvals >= requiredApprovals {

		// Ensure we don't exceed max water per timestep
		remaining := maxWaterPerTimestep - waterDeliveredThisStep
		if remaining <= 0 {
			fmt.Printf("[%d] 🚫 %s cannot receive water this timestep (limit reached)\n", t.Clock.Time(), t.ID)
			return false
		}

		if amount > remaining {
			amount = remaining
		}

		if globalWater >= amount {
			globalWater -= amount
			waterDeliveredThisStep += amount
			fmt.Printf("[%d] 💧 %s received %.0f units of water! Remaining global water: %.0f\n",
				t.Clock.Time(), t.ID, amount, globalWater)
			return true
		} else {
			fmt.Printf("[%d] 🚫 %s was approved, but global water insufficient.\n", t.Clock.Time(), t.ID)
			return false
		}
	}

	fmt.Printf("[%d] 🚫 %s did not receive enough approvals (needed %d).\n", t.Clock.Time(), t.ID, requiredApprovals)
	return false
}

// isNearFire checks the 4 neighboring cells around the truck
func isNearFire(gridmap map[string]interface{}, truck FireTruck) (bool, int, int) {
	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)

	directions := [][2]int{{-1, 0}, {1, 0}, {0, -1}, {0, 1}}
	for _, d := range directions {
		nx, ny := truck.X+d[0], truck.Y+d[1]
		if nx >= 0 && nx < size && ny >= 0 && ny < size {
			if grid[nx][ny]["fire"].(bool) {
				return true, nx, ny
			}
		}
	}
	return false, -1, -1
}

func extinguishFire(gridmap map[string]interface{}, truck *FireTruck, fx, fy int, nc *nats.Conn) {
	grid := gridmap["grid"].([][]map[string]interface{})
	cell := grid[fx][fy]
	intensity := cell["intensity"].(float64)

	if intensity <= 0 {
		fmt.Printf("No fire at (%d,%d) to extinguish.\n", fx, fy)
		return
	}

	// Request water equal to fire intensity
	timestamp := truck.Clock.Increment()
	fmt.Printf("[%d] %s attempting to extinguish fire at (%d,%d)\n", timestamp, truck.ID, fx, fy)
	success := truck.RequestWater(intensity)
	if !success {
		return
	}

	cell["intensity"] = 0
	cell["fire"] = false
	fmt.Printf("[%d] 🔥 Fire at (%d,%d) extinguished by %s!\n", truck.Clock.Increment(), fx, fy, truck.ID)

	// Publish event to notify other trucks
	msg := fmt.Sprintf("%s extinguished fire at (%d,%d)", truck.ID, fx, fy)
	nc.Publish("fire.extinguished", []byte(msg))
}
