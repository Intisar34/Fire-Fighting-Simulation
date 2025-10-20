package main

import (
	"fmt"
	"math/rand"

	"github.com/nats-io/nats.go"
)

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

func (t *FireTruck) MoveTowardFire(gridmap map[string]interface{}, fx, fy int) {
	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)

	// Clear old position
	grid[t.X][t.Y]["truck"] = ""

	if t.X < fx {
		t.X++
	} else if t.X > fx {
		t.X--
	}
	if t.Y < fy {
		t.Y++
	} else if t.Y > fy {
		t.Y--
	}

	// Update new position
	if t.X >= 0 && t.X < size && t.Y >= 0 && t.Y < size {
		grid[t.X][t.Y]["truck"] = t.ID
	}

	fmt.Printf("[%d] 🚛 %s moving toward fire (%d,%d)\n", t.Clock.Increment(), t.ID, fx, fy)
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
