#!/bin/bash

# Adaptive REPL client for the KV store cluster
# Automatically fails over to other nodes when one is down

NODE=1
PORT=8001
HOST="localhost"
NODES=(1 2 3)

print_help() {
    cat <<'EOF'
KV Store REPL Commands:

  Replicated KV (Raft consensus - all nodes have all data):
    set <key> <value>   - Store a value (goes through leader)
    get <key>           - Retrieve a value
    del <key>           - Delete a key

  Sharded KV (no consensus - keys distributed across nodes):
    sset <key> <value>  - Store in sharded store (routes to owner)
    sget <key>          - Retrieve from sharded store
    sdel <key>          - Delete from sharded store

  Cluster:
    health              - Show current node status
    nodes               - Show all nodes status
    leader              - Find and switch to the leader
    use <1|2|3>         - Switch to node 1, 2, or 3
    help                - Show this help
    quit/exit           - Exit the REPL

Sharded KV shows routing info: which node owns the key, placement group, etc.
EOF
}

# Try a curl request, return 0 on success, 1 on failure
# Sets RESULT variable with output
try_request() {
    local method="$1"
    local url="$2"
    local data="$3"

    if [[ -n "$data" ]]; then
        RESULT=$(curl -sf -X "$method" "$url" \
            -H "Content-Type: application/json" \
            -d "$data" 2>&1)
    else
        RESULT=$(curl -sf -X "$method" "$url" 2>&1)
    fi
    return $?
}

# Try request on current node, then failover to others
# Usage: adaptive_request <method> <path> [data]
adaptive_request() {
    local method="$1"
    local path="$2"
    local data="$3"

    # Try current node first
    if try_request "$method" "http://$HOST:$PORT$path" "$data"; then
        return 0
    fi

    # Current node failed, try others
    echo "(node $NODE unavailable, trying others...)" >&2
    for n in "${NODES[@]}"; do
        [[ $n -eq $NODE ]] && continue
        local p=$((8000 + n))
        if try_request "$method" "http://$HOST:$p$path" "$data"; then
            NODE=$n
            PORT=$p
            echo "(switched to node $NODE)" >&2
            return 0
        fi
    done

    return 1
}

# Find the current leader, returns node number or empty
find_leader() {
    for n in "${NODES[@]}"; do
        local p=$((8000 + n))
        local result=$(curl -sf "http://$HOST:$p/health" 2>/dev/null)
        if [[ $? -eq 0 ]]; then
            local is_leader=$(echo "$result" | jq -r '.is_leader')
            if [[ "$is_leader" == "true" ]]; then
                echo "$n"
                return 0
            fi
        fi
    done
    return 1
}

# Switch to leader for write operations
ensure_leader() {
    local leader=$(find_leader)
    if [[ -n "$leader" && "$leader" != "$NODE" ]]; then
        NODE=$leader
        PORT=$((8000 + leader))
        echo "(redirecting to leader node $NODE)" >&2
    fi
}

do_set() {
    local key="$1"
    local value="$2"
    if [[ -z "$key" || -z "$value" ]]; then
        echo "Usage: set <key> <value>"
        return 1
    fi

    # Writes need to go to the leader
    ensure_leader

    if adaptive_request "PUT" "/kv/$key" "{\"value\":\"$value\"}"; then
        echo "OK"
    else
        echo "Error: all nodes unavailable"
    fi
}

do_get() {
    local key="$1"
    if [[ -z "$key" ]]; then
        echo "Usage: get <key>"
        return 1
    fi

    if adaptive_request "GET" "/kv/$key"; then
        echo "$RESULT" | jq -r '.value // "(nil)"'
    else
        # Check if it's a 404 vs connection failure
        for n in "${NODES[@]}"; do
            local p=$((8000 + n))
            local http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://$HOST:$p/kv/$key" 2>/dev/null)
            if [[ "$http_code" == "404" ]]; then
                NODE=$n
                PORT=$p
                echo "(nil)"
                return 0
            elif [[ "$http_code" == "200" ]]; then
                # Shouldn't happen but handle it
                NODE=$n
                PORT=$p
                curl -sf "http://$HOST:$p/kv/$key" | jq -r '.value'
                return 0
            fi
        done
        echo "Error: all nodes unavailable"
    fi
}

do_del() {
    local key="$1"
    if [[ -z "$key" ]]; then
        echo "Usage: del <key>"
        return 1
    fi

    # Writes need to go to the leader
    ensure_leader

    if adaptive_request "DELETE" "/kv/$key"; then
        echo "OK"
    else
        echo "Error: all nodes unavailable"
    fi
}

# Sharded KV commands - show routing info

do_sset() {
    local key="$1"
    local value="$2"
    if [[ -z "$key" || -z "$value" ]]; then
        echo "Usage: sset <key> <value>"
        return 1
    fi

    if adaptive_request "PUT" "/sharded/$key" "{\"value\":\"$value\"}"; then
        local ok=$(echo "$RESULT" | jq -r '.ok')
        local owner=$(echo "$RESULT" | jq -r '.routing.owner_node')
        local is_local=$(echo "$RESULT" | jq -r '.routing.is_local')
        local forwarded=$(echo "$RESULT" | jq -r '.routing.forwarded_to // empty')
        local hash=$(echo "$RESULT" | jq -r '.routing.key_hash')
        local pg=$(echo "$RESULT" | jq -c '.routing.placement_group')

        echo "Key:    $key"
        echo "Hash:   $hash"
        echo "Owner:  node $owner"
        echo "PG:     $pg"
        if [[ "$is_local" == "true" ]]; then
            echo "Route:  LOCAL (handled by node $NODE)"
        else
            echo "Route:  FORWARDED to $forwarded"
        fi
        if [[ "$ok" == "true" ]]; then
            echo "Status: OK"
        else
            echo "Status: FAILED"
        fi
    else
        echo "Error: all nodes unavailable"
    fi
}

do_sget() {
    local key="$1"
    if [[ -z "$key" ]]; then
        echo "Usage: sget <key>"
        return 1
    fi

    if adaptive_request "GET" "/sharded/$key"; then
        local value=$(echo "$RESULT" | jq -r '.value // "(nil)"')
        local owner=$(echo "$RESULT" | jq -r '.routing.owner_node')
        local is_local=$(echo "$RESULT" | jq -r '.routing.is_local')
        local forwarded=$(echo "$RESULT" | jq -r '.routing.forwarded_to // empty')
        local hash=$(echo "$RESULT" | jq -r '.routing.key_hash')
        local pg=$(echo "$RESULT" | jq -c '.routing.placement_group')

        echo "Key:    $key"
        echo "Hash:   $hash"
        echo "Owner:  node $owner"
        echo "PG:     $pg"
        if [[ "$is_local" == "true" ]]; then
            echo "Route:  LOCAL (handled by node $NODE)"
        else
            echo "Route:  FORWARDED to $forwarded"
        fi
        echo "Value:  $value"
    else
        echo "Error: all nodes unavailable"
    fi
}

do_sdel() {
    local key="$1"
    if [[ -z "$key" ]]; then
        echo "Usage: sdel <key>"
        return 1
    fi

    if adaptive_request "DELETE" "/sharded/$key"; then
        local deleted=$(echo "$RESULT" | jq -r '.deleted')
        local owner=$(echo "$RESULT" | jq -r '.routing.owner_node')
        local is_local=$(echo "$RESULT" | jq -r '.routing.is_local')
        local forwarded=$(echo "$RESULT" | jq -r '.routing.forwarded_to // empty')
        local hash=$(echo "$RESULT" | jq -r '.routing.key_hash')
        local pg=$(echo "$RESULT" | jq -c '.routing.placement_group')

        echo "Key:    $key"
        echo "Hash:   $hash"
        echo "Owner:  node $owner"
        echo "PG:     $pg"
        if [[ "$is_local" == "true" ]]; then
            echo "Route:  LOCAL (handled by node $NODE)"
        else
            echo "Route:  FORWARDED to $forwarded"
        fi
        if [[ "$deleted" == "true" ]]; then
            echo "Status: DELETED"
        else
            echo "Status: NOT FOUND"
        fi
    else
        echo "Error: all nodes unavailable"
    fi
}

do_health() {
    if adaptive_request "GET" "/health"; then
        local node_id=$(echo "$RESULT" | jq -r '.node_id')
        local is_leader=$(echo "$RESULT" | jq -r '.is_leader')
        local has_leader=$(echo "$RESULT" | jq -r '.has_leader')
        if [[ "$is_leader" == "true" ]]; then
            echo "Node $node_id: LEADER"
        elif [[ "$has_leader" == "true" ]]; then
            echo "Node $node_id: follower (has leader)"
        else
            echo "Node $node_id: follower (no leader)"
        fi
    else
        echo "Error: all nodes unavailable"
    fi
}

do_nodes() {
    for n in 1 2 3; do
        p=$((8000 + n))
        result=$(curl -sf "http://$HOST:$p/health" 2>&1)
        if [[ $? -eq 0 ]]; then
            node_id=$(echo "$result" | jq -r '.node_id')
            is_leader=$(echo "$result" | jq -r '.is_leader')
            has_leader=$(echo "$result" | jq -r '.has_leader')
            marker=""
            [[ $n -eq $NODE ]] && marker=" <-- current"
            if [[ "$is_leader" == "true" ]]; then
                echo "Node $n (port $p): LEADER$marker"
            elif [[ "$has_leader" == "true" ]]; then
                echo "Node $n (port $p): follower$marker"
            else
                echo "Node $n (port $p): follower (no leader)$marker"
            fi
        else
            marker=""
            [[ $n -eq $NODE ]] && marker=" <-- current"
            echo "Node $n (port $p): OFFLINE$marker"
        fi
    done
}

do_leader() {
    local leader=$(find_leader)
    if [[ -n "$leader" ]]; then
        NODE=$leader
        PORT=$((8000 + leader))
        echo "Switched to leader node $NODE (localhost:$PORT)"
    else
        echo "No leader found (cluster may be forming or down)"
    fi
}

do_use() {
    local n="$1"
    if [[ "$n" =~ ^[1-3]$ ]]; then
        NODE=$n
        PORT=$((8000 + n))
        echo "Switched to node $NODE (localhost:$PORT)"
    else
        echo "Usage: use <1|2|3>"
    fi
}

# Main REPL
echo "KV Store REPL (adaptive mode)"
echo "Connected to node $NODE (localhost:$PORT)"
echo "Type 'help' for commands, 'quit' to exit"
echo ""

while true; do
    read -r -p "kv:$NODE> " cmd args

    # Handle EOF (Ctrl+D)
    if [[ $? -ne 0 ]]; then
        echo ""
        break
    fi

    case "$cmd" in
        set)
            key=$(echo "$args" | awk '{print $1}')
            value=$(echo "$args" | cut -d' ' -f2-)
            do_set "$key" "$value"
            ;;
        get)
            do_get "$args"
            ;;
        del|delete)
            do_del "$args"
            ;;
        sset)
            key=$(echo "$args" | awk '{print $1}')
            value=$(echo "$args" | cut -d' ' -f2-)
            do_sset "$key" "$value"
            ;;
        sget)
            do_sget "$args"
            ;;
        sdel|sdelete)
            do_sdel "$args"
            ;;
        health)
            do_health
            ;;
        nodes)
            do_nodes
            ;;
        leader)
            do_leader
            ;;
        use)
            do_use "$args"
            ;;
        help|h|\?)
            print_help
            ;;
        quit|exit|q)
            echo "Bye!"
            break
            ;;
        "")
            # Empty line, do nothing
            ;;
        *)
            echo "Unknown command: $cmd (type 'help' for commands)"
            ;;
    esac
done
