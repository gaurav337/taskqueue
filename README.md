# TaskScheduler

TaskScheduler is a robust, Redis-backed asynchronous task queue library written in Go. It supports task persistence, job priority queues, worker consumer groups, and automated retries.

## Architecture

The system consists of two primary modules:
1. **Persistence Store (`internal/job`)**: Marshals Go structs to JSON and stores job metadata in Redis Strings with a configurable TTL (defaults to 24 hours).
2. **Queue Broker (`internal/queue`)**: Distributes tasks dynamically across priority levels (`critical`, `default`, `low`) utilizing Redis Streams and Consumer Groups.

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

### Running Tests
Execute the Go testing suite to verify store operations, queue publishing, and the backlog processing behavior:
```bash
go test -v ./...
```

---

## Directory Structure

```
├── internal/
│   ├── job/
│   │   ├── job.go        # Job and Status type definitions
│   │   ├── store.go      # Redis String JSON serialization store
│   │   └── store_test.go # Persistence store unit tests
│   └── queue/
│       ├── queue.go      # Redis Stream broker interface
│       └── queue_test.go # Stream publishing & cold boot backlog tests
├── docker-compose.yml     # Local Redis environment definition
├── engineering_journal.md # Recorded engineering incidents & fixes
├── go.mod                 # Go module file
└── README.md              # Project documentation
```
