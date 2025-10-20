package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Listens for water requests from other trucks
func (t *FireTruck) ListenForWaterRequests() {
	t.Conn.Subscribe("water.request", func(msg *nats.Msg) {
		var req map[string]interface{}
		json.Unmarshal(msg.Data, &req)
		requester := req["truck_id"].(string)
		//needed := req["needed"].(float64) maybe not needed
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
	fmt.Printf("[%d] %s got %d approvals / %d denials\n", t.Clock.Time(), t.ID, approvals, denials)

	if approvals >= requiredApprovals {

		// Ensure we don't exceed max water per timestep
		remaining := maxWaterPerTimestep - waterDeliveredThisStep
		if remaining <= 0 {
			fmt.Printf("[%d] %s cannot receive water this timestep (limit reached)\n", t.Clock.Time(), t.ID)
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
			fmt.Printf("[%d] %s was approved, but global water insufficient.\n", t.Clock.Time(), t.ID)
			return false
		}
	}

	fmt.Printf("[%d] %s did not receive enough approvals (needed %d).\n", t.Clock.Time(), t.ID, requiredApprovals)
	return false
}
