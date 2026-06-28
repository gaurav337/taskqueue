# TaskScheduler

TaskScheduler is a robust, Redis-backed asynchronous task queue library written in Go. It supports task persistence, job priority queues, worker consumer groups, and automated retries.

## Architecture

The system consists of three primary components:
1. **Persistence Store (`internal/job`)**: Marshals Go structs to JSON and stores job metadata in Redis Strings with a configurable TTL (defaults to 24 hours).
2. **Queue Broker (`internal/queue`)**: Distributes tasks dynamically across priority levels (`critical`, `default`, `low`) utilizing Redis Streams and Consumer Groups.
3. **REST API Server (`cmd/api`)**: Receives HTTP POST requests to create new tasks asynchronously, generating UUIDs and saving/publishing them.
4. **Worker Daemon (`cmd/worker`)**: Spawns concurrent consumer loops that check priority streams (`critical` -> `default` -> `low`) and process tasks.

### Non-Blocking Priority Checks & Connection Stall Prevention
The Queue Broker's `Read` function checks streams sequentially to enforce strict priority. To prevent thread stalls or blocking the connection pool on higher priority empty streams, the broker uses a negative block duration (`-1`) to perform an immediate socket-level check on the `critical` and `default` lanes, only blocking (with a safety threshold) on the `low` priority lane.

### Cold Boot Backlog Resilience
To prevent task starvation during worker downtime (cold boot scenarios), consumer groups are initialized using the starting ID of `"0"` instead of `"$"` inside the stream broker (`internal/queue/queue.go`). This ensures that pre-existing backlog messages written to the stream while workers were offline are correctly consumed on startup.

---

## Getting Started

### Prerequisites
- Go 1.25+
- Docker and Docker Compose (to run Redis)

### Running Infrastructure (Redis)
Start the Redis instance in detached mode:
```bash
docker compose up -d
```

### Running the API Server
Start the producer API server:
```bash
go run ./cmd/api/main.go
```
The server listens on port `8080`.
- **Submit a job**: `POST /jobs` with a JSON body:
  ```json
  {
    "type": "email",
    "priority": "critical",
    "payload": {
      "to": "user@example.com",
      "body": "Hello World!"
    }
  }
  ```
- **Check job status**: `GET /jobs/{job_id}/status`

### Running the Worker Daemon
Start the consumer worker pool:
```bash
go run ./cmd/worker/main.go
```

### Running Tests
Execute the Go testing suite:
```bash
go test -v ./...
```

---

## Directory Structure

```
├── cmd/
│   ├── api/
│   │   ├── main.go        # HTTP REST API server (Producer)
│   │   └── main_test.go   # API integration tests
│   └── worker/
│       └── main.go        # Concurrent Worker Daemon (Consumer)
├── internal/
│   ├── job/
│   │   ├── job.go        # Job and Status type definitions
│   │   ├── store.go      # Redis String JSON serialization store
│   │   └── store_test.go # Persistence store unit tests
│   └── queue/
│       ├── queue.go      # Redis Stream broker interface
│       └── queue_test.go # Stream publishing, priority selection & backlog tests
├── docker-compose.yml     # Local Redis environment definition
├── engineering_journal.md # Recorded engineering incidents & fixes
├── go.mod                 # Go module file
└── README.md              # Project documentation
```
