package main

import (
	"encoding/json"
	"fmt"
	"time"
	"github.com/nats-io/nats.go"
)

func createTruck(id string, x, y float64, nc *nats.Conn) map[string]interface{} {
	return map[string]interface{}{
		"id": id,
		"x":  x,
		"y":  y,
		"nc": nc,
	}
}

// NATS request helper
func sendRequest(nc *nats.Conn, subject string, msg Message) (Message, error) {
	data, _ := json.Marshal(msg)
	resp, err := nc.Request(subject, data, 2*time.Second)
	if err != nil {
		return nil, err
	}

	var reply Message
	json.Unmarshal(resp.Data, &reply)
	return reply, nil
}

// Register a truck
func registerTruck(truck map[string]interface{}) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type": "register",
		"from": truck["id"],
		"x":    truck["x"],
		"y":    truck["y"],
	}

	return sendRequest(nc, "truck.requests", msg)
}

// Request nearest fire
func requestNearestFire(truck map[string]interface{}) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type": "nearest fire",
		"from": truck["id"],
		"x":    truck["x"],
		"y":    truck["y"],
	}
	return sendRequest(nc, "truck.requests", msg)
}

func ListenForFires(truck map[string]interface{}) {
    nc := truck["nc"].(*nats.Conn)
    id := truck["id"].(string)

    _, err := nc.Subscribe("fires.updates", func(m *nats.Msg) {
        var msg Message
        json.Unmarshal(m.Data, &msg)
        fmt.Printf("🚒 Truck %s received fire update: %v\n", id, msg)
    })
    if err != nil {
        fmt.Println("Error subscribing to fires:", err)
    }
}

// Request water
func requestWater(truck map[string]interface{}, amount float64) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":   "request water",
		"from":   truck["id"],
		"amount": amount,
	}

	return sendRequest(nc, "truck.requests", msg)
}

// Extinguish fire
func extinguishFire(truck map[string]interface{}, amount float64) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":   "extinguish fire",
		"x":      truck["x"],
		"y":      truck["y"],
		"amount": amount,
	}

	return sendRequest(nc, "truck.requests", msg)
}

func requestMove(truck map[string]interface{}, direction string) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":      "request move",
		"from":      truck["id"],
		"direction": direction,
	}

	return sendRequest(nc, "truck.requests", msg)
}

// decide the direction the truck will move based on the position of the fire
func decideDirection(truckX, truckY, fireX, fireY float64) string {

	if truckX < fireX {
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

func truckLoop(truck map[string]interface{}, done <-chan struct{}) {

	ticker := time.NewTicker(2 * time.Second)

	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fireResponse, err := requestNearestFire(truck)
			if err != nil || !fireResponse["ok"].(bool) {
				fmt.Printf("No fire found for truck %v\n", truck["id"])
				continue
			}

			fireX := fireResponse["x"].(float64)
			fireY := fireResponse["y"].(float64)
			fireIntensity := fireResponse["intensity"].(float64)
			truckX := truck["x"].(float64)
			truckY := truck["y"].(float64)

			decision := decideDirection(truckX, truckY, fireX, fireY)

			if decision == "" {

				fmt.Printf("🔥 Truck %v is at the fire location! Extinguishing...\n", truck["id"])

				neededWater := float64(fireIntensity) * 2.0

				waterResp, err := requestWater(truck, neededWater)
				if err != nil {
					fmt.Println(" 💧 Error requesting water:", err)
					continue
				}
				fmt.Printf("💧 Water request: %v\n", waterResp)

				extResp, err := extinguishFire(truck, neededWater)
				if err != nil {
					fmt.Println("🧯 Error extinguish fire:", err)
					continue
				}
				fmt.Printf("🧯 Extinguish result: %v\n", extResp)

			}

			moveResponse, err := requestMove(truck, decision)

			if err != nil {
				fmt.Println("🚫 Error moving truck:", err)
				continue
			}

			if moveResponse["ok"].(bool) {
				switch decision {
				case "n":
					truck["x"] = float64(truckX - 1)
				case "s":
					truck["x"] = float64(truckX + 1)
				case "e":
					truck["y"] = float64(truckY + 1)
				case "w":
					truck["y"] = float64(truckY - 1)
				}
			}
			fmt.Printf("➡️ Truck %v moved %v: %v\n", truck["id"], decision, moveResponse)

		case <-done:
			fmt.Printf("🛑 Truck %v stopping.\n", truck["id"])
			return
		}

	}
}

