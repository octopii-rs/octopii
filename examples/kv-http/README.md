# Distributed KV Store Example

A simple HTTP-based distributed key-value store built with Octopii.

This example demonstrates:
- Running a 3-node Raft cluster with Docker Compose
- HTTP API for key-value operations
- Automatic leader election and cluster formation
- Data replication across all nodes

## Quick Start

```bash
# Start the 3-node cluster
docker compose up --build

# In another terminal, interact with the cluster:

# Check cluster health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Store a value (goes through leader)
curl -X PUT http://localhost:8001/kv/hello \
  -H "Content-Type: application/json" \
  -d '{"value":"world"}'

# Read from any node (data is replicated)
curl http://localhost:8001/kv/hello
curl http://localhost:8002/kv/hello
curl http://localhost:8003/kv/hello

# Delete a key
curl -X DELETE http://localhost:8001/kv/hello
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `PUT` | `/kv/{key}` | Set a value. Body: `{"value": "..."}` |
| `GET` | `/kv/{key}` | Get a value |
| `DELETE` | `/kv/{key}` | Delete a key |
| `GET` | `/health` | Node status (node_id, is_leader, has_leader) |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   node1     │     │   node2     │     │   node3     │
│  (leader)   │◄───►│  (follower) │◄───►│  (follower) │
│             │     │             │     │             │
│ HTTP :8001  │     │ HTTP :8002  │     │ HTTP :8003  │
│ QUIC :5001  │     │ QUIC :5002  │     │ QUIC :5003  │
│ 10.5.0.11   │     │ 10.5.0.12   │     │ 10.5.0.13   │
└─────────────┘     └─────────────┘     └─────────────┘
```

- **HTTP ports (8001-8003)**: Client-facing API
- **QUIC ports (5001-5003)**: Internal Raft communication
- **Static IPs**: Used for peer discovery in Docker

## Testing Leader Failover

```bash
# Check who's leader
curl http://localhost:8001/health
curl http://localhost:8002/health

# Stop the leader
docker compose stop node1

# Wait a few seconds, then check again
curl http://localhost:8002/health
curl http://localhost:8003/health
# One should now be leader

# Restart node1 - it rejoins as follower
docker compose start node1
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ID` | Unique node identifier | `1` |
| `BIND_ADDR` | QUIC transport address | `127.0.0.1:5001` |
| `HTTP_ADDR` | HTTP API address | `127.0.0.1:8001` |
| `PEERS` | Comma-separated peer addresses (IP:port) | (empty) |
| `INITIAL_LEADER` | Bootstrap as initial leader | `false` |
| `DATA_DIR` | WAL storage directory | `./data` |
| `RUST_LOG` | Log level | `info` |

## Running Without Docker

```bash
# Terminal 1 - Start node1 (leader)
NODE_ID=1 BIND_ADDR=127.0.0.1:5001 HTTP_ADDR=127.0.0.1:8001 \
  PEERS=127.0.0.1:5002,127.0.0.1:5003 INITIAL_LEADER=true \
  DATA_DIR=./data/node1 cargo run --release

# Terminal 2 - Start node2
NODE_ID=2 BIND_ADDR=127.0.0.1:5002 HTTP_ADDR=127.0.0.1:8002 \
  PEERS=127.0.0.1:5001,127.0.0.1:5003 \
  DATA_DIR=./data/node2 cargo run --release

# Terminal 3 - Start node3
NODE_ID=3 BIND_ADDR=127.0.0.1:5003 HTTP_ADDR=127.0.0.1:8003 \
  PEERS=127.0.0.1:5001,127.0.0.1:5002 \
  DATA_DIR=./data/node3 cargo run --release
```

## Notes

- **Writes must go to the leader.** Check `/health` to find the current leader.
- **Reads can go to any node** (fast, but may be slightly stale on followers).
- Data is persisted in Docker volumes (`node1-data`, `node2-data`, `node3-data`).
- To reset the cluster: `docker compose down -v`
