package main

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
)

// NATS request helper
func sendRequest(nc *nats.Conn, subject string, msg Message) (Message, error) {
	data, _ := json.Marshal(msg)
	resp, err := nc.Request(subject, data, 2*time.Second) // waits up to 2s for reply
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
	return sendRequest(nc, "manager", msg)
}

// Request water
func requestWater(truck map[string]interface{}, amount int) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":   "request water",
		"from":   truck["id"],
		"amount": amount,
	}

	return sendRequest(nc, "manager", msg)
}

// Extinguish fire
func extinguishFire(truck map[string]interface{}, amount int) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":   "extinguish fire",
		"x":      truck["x"],
		"y":      truck["y"],
		"amount": amount,
	}

	return sendRequest(nc, "manager", msg)
}

func requestMove(truck map[string]interface{}, direction string) (Message, error) {
	nc := truck["nc"].(*nats.Conn)

	msg := Message{
		"type":      "move",
		"from":      truck["id"],
		"direction": direction,
	}

	return sendRequest(nc, "manager", msg)
}

// decide the direction the truck will move based on the position of the fire
func decideDirection(truckX, truckY, fireX, fireY int) string {

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
