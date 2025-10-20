package main

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/nats-io/nats.go"
)

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
func spawnFires(gridmap map[string]interface{}, numFires int, nc *nats.Conn) {
	grid := gridmap["grid"].([][]map[string]interface{})
	size := len(grid)

	for i := 0; i < numFires; i++ {
		x := rand.Intn(size)
		y := rand.Intn(size)
		cell := grid[x][y]
		cell["fire"] = true
		cell["intensity"] = float64(rand.Intn(10) + 1)

		msg := map[string]int{
			"x": x,
			"y": y,
		}

		data, _ := json.Marshal(msg)
		nc.Publish("new.fire", data)
		fmt.Printf("🔥 New fire spawned at (%d,%d) with intensity %.0f\n", x, y, cell["intensity"])

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
