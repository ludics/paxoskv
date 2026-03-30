# PaxosKV

A distributed key-value store based on the Paxos consensus protocol, built with gRPC and LevelDB.

## Architecture

Each acceptor runs an independent gRPC server backed by its own LevelDB database. Paxos state (LastBallot, VBallot, Value) is persisted per key/version instance.

```
┌──────────┐     gRPC     ┌──────────────────┐
│ Proposer │─────────────▶│ Acceptor (LevelDB)│
│          │   Prepare    └──────────────────┘
│          │   Accept
│          │─────────────▶┌──────────────────┐
│          │              │ Acceptor (LevelDB)│
└──────────┘              └──────────────────┘
       │                         ...
       └─────────────▶┌──────────────────┐
                      │ Acceptor (LevelDB)│
                      └──────────────────┘
```

## Project Structure

```
paxoskv/
├── go.mod
├── go.sum
├── README.md
└── src/
    ├── paxoskv.proto              # Protobuf service & message definitions
    ├── paxoskv.pb.go              # Generated protobuf code (DO NOT EDIT)
    ├── paxoskv_grpc.pb.go         # Generated gRPC code (DO NOT EDIT)
    ├── impl.go                    # Paxos implementation (Acceptor, Proposer, KVServer)
    ├── bench_test.go              # Benchmark & stress tests
    ├── paxos_slides_case_test.go  # Functional test cases
    └── set_get_test.go            # Example set/get test
```

## Prerequisites

- Go 1.26+
- protoc (Protocol Buffers compiler)
- protoc-gen-go & protoc-gen-go-grpc

## Generate Protobuf Code

```bash
cd src
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       paxoskv.proto
```

## Run Tests

```bash
# Run all tests (including stress test)
go test ./src -v -count=1

# Run only functional tests
go test ./src -v -run "Case"

# Run stress test (50 workers x 100 ops)
go test ./src -v -run "Stress"
```

## Run Benchmarks

```bash
# All benchmarks
go test ./src -bench=. -benchmem

# Write throughput only
go test ./src -bench=BenchmarkPaxosWrite -benchmem

# Concurrent writes scaling
go test ./src -bench=BenchmarkPaxosConcurrentWrites -benchmem

# Competing proposers (safety under contention)
go test ./src -bench=BenchmarkPaxosCompetingProposers -benchmem
```

## Test Cases

| Test | Description |
|------|-------------|
| TestCase1 | Single proposer write & read |
| TestCase2 | Multiple versions (v1/v2) independent read/write |
| TestCase3 | Two concurrent proposers competing same key — convergence check |
| TestCase4 | Different keys isolation |
| TestCase5 | Read non-existent key returns nil |
| TestCase6 | Higher ballot inherits already-chosen value (Paxos safety) |
| TestStress | 50 concurrent workers × 100 ops correctness stress test |

## Key Design Decisions

- **LevelDB persistence**: Each acceptor has its own LevelDB instance at `/tmp/paxoskv-leveldb-{id}`
- **State serialization**: Acceptor state stored as JSON with key format `paxos:<key>:<version>`
- **Quorum**: Majority quorum (`n/2 + 1`)
- **Ballot comparison**: `(N, ProposerId)` tuple with lexicographic ordering
