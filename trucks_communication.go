package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
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
		fmt.Printf("[%d] %s replied '%s' to %s’s request.\n", t.Clock.Time(), t.ID, status, requester)
	})
}

func (t *FireTruck) RequestWater(amount float64) bool {
	timestamp := t.Clock.Increment()
	fmt.Printf("[%d] %s requesting %.0f units of water\n", timestamp, t.ID, amount)

	request := map[string]interface{}{
		"truck_id":   t.ID,
		"needed":     amount,
		"request_id": fmt.Sprintf("%s-%d", t.ID, time.Now().UnixNano()),
		"timestamp":  timestamp,
	}
	data, _ := json.Marshal(request)

	sub, _ := t.Conn.SubscribeSync(fmt.Sprintf("water.reply.%s", request["request_id"]))
	defer sub.Unsubscribe()

	t.Conn.PublishRequest("water.request", fmt.Sprintf("water.reply.%s", request["request_id"]), data)

	approvals := 0
	denials := 0
	repliedTrucks := make(map[string]bool)
	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(200 * time.Millisecond)
		if err != nil {
			continue
		}

		var reply map[string]interface{}
		json.Unmarshal(msg.Data, &reply)

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

	requiredApprovals := 3
	fmt.Printf("[%d] %s got %d approvals / %d denials\n", t.Clock.Time(), t.ID, approvals, denials)

	if approvals >= requiredApprovals {
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

func (t *FireTruck) ListenForFires(gridmap map[string]interface{}) {
	t.Conn.Subscribe("new.fire", func(msg *nats.Msg) {
		var fire map[string]int
		json.Unmarshal(msg.Data, &fire)
		fx, fy := fire["x"], fire["y"]

		// Calculate distance
		dist := math.Abs(float64(t.X-fx)) + math.Abs(float64(t.Y-fy))

		// Simulate random small delay to avoid collisions
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)

		// Publish claim
		claim := FireClaim{
			TruckID:   t.ID,
			FireX:     fx,
			FireY:     fy,
			Distance:  dist,
			Timestamp: t.Clock.Increment(),
		}
		data, _ := json.Marshal(claim)
		t.Conn.Publish("fire.claim", data)

		fmt.Printf("[%d] %s detected fire at (%d,%d), distance %.1f\n", t.Clock.Time(), t.ID, fx, fy, dist)
	})
}

func (t *FireTruck) ListenForClaims(gridmap map[string]interface{}) {
	t.Conn.Subscribe("fire.claim", func(msg *nats.Msg) {
		var claim FireClaim
		json.Unmarshal(msg.Data, &claim)

		// Ignore own claims
		if claim.TruckID == t.ID {
			return
		}

		key := fmt.Sprintf("%d,%d", claim.FireX, claim.FireY)
		current := gridmap[key]

		// Update Lamport clock
		t.Clock.Update(claim.Timestamp)

		// Compare distances or use ID as tie-breaker
		if currClaim, ok := current.(FireClaim); ok {
			if claim.Distance < currClaim.Distance ||
				(claim.Distance == currClaim.Distance && claim.TruckID < currClaim.TruckID) {
				gridmap[key] = claim
			}
		} else {
			gridmap[key] = claim
		}
	})
}
