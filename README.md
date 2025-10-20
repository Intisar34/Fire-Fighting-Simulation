# Distributed Fire Fighting Simulation


## Description
This project simulates a decentralized forest fire management system where multiple fire trucks independently detect and extinguish fires on a grid. Each truck communicates with others using NATS messaging to request water and coordinate which fires to fight, eliminating the need for a central controller. The system demonstrates distributed decision-making using Lamport clocks to handle priorities and avoid conflicts.

**Key Features**
- Decentralized fire management without central control.
- Trucks coordinate using publish-subscribe and request-reply messaging patterns.
- Fires spawn dynamically with varying intensity.
- Trucks make decisions based on proximity and priority, ensuring efficient firefighting.
- Demonstrates distributed algorithms and decentralized resource management.

## Architecture

**Modules**
- `main.go`: Simulation entry point and loop.
- `types.go`: Definitions for FireTruck, FireClaim, and global variables.
- `grid.go`: Grid creation, display, and fire & truck spawning.
- `truck_actions.go`: Movement, fire detection, and extinguishing logic.
- `trucks_communication.go`: Inter truck communication and water request handling.
- `lamport.go`: Implements the Lamport clock used for decentralized coordination and prioritizing events.

**System Architecture**
- The system consists of multiple independent fire trucks operating on a shared grid.
- Trucks communicate with each other using NATS messaging, leveraging publish-subscribe for fire events and request-reply for water requests.
- Each truck independently detects fires, decides which to fight, and requests water when needed.


## Setup

**1. Prerequisites**
- Go
- Nats Server
- Docker

**2. Clone the Repository**
- `git clone <repo-url>`
- `cd <repo-folder>`

**3. Start the Nats Server**
- `docker start nats-server`

**4. Run Simulation**

`go run main.go types.go grid.go truck_actions.go trucks_communication.go lamport.go`


## Contribution
- Anisa Hashi
- Intisar Warfa
- Leonard Blomdahl