#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Building cluster example..."
cargo build --release --example cluster

# Cleanup function to kill backgrounded brokers
# Shuts down in reverse order to minimize Raft replication errors during shutdown
cleanup() {
    echo ""
    echo "Shutting down brokers gracefully (reverse order)..."

    # Helper function to wait for a process to exit with timeout
    wait_for_process() {
        local pid=$1
        local name=$2
        local timeout=5
        local elapsed=0

        while kill -0 "$pid" 2>/dev/null; do
            if [ $elapsed -ge $timeout ]; then
                echo "    $name didn't exit after ${timeout}s, force killing..."
                kill -9 "$pid" 2>/dev/null || true
                break
            fi
            sleep 0.5
            elapsed=$((elapsed + 1))
        done
    }

    # Shutdown broker 2 first (non-initializer)
    if [ -n "$BROKER2_PID" ] && kill -0 "$BROKER2_PID" 2>/dev/null; then
        echo "  Stopping broker 2 (PID: $BROKER2_PID)..."
        kill -s TERM "$BROKER2_PID" 2>/dev/null || true
        wait_for_process "$BROKER2_PID" "Broker 2"
    fi

    # Then broker 1
    if [ -n "$BROKER1_PID" ] && kill -0 "$BROKER1_PID" 2>/dev/null; then
        echo "  Stopping broker 1 (PID: $BROKER1_PID)..."
        kill -s TERM "$BROKER1_PID" 2>/dev/null || true
        wait_for_process "$BROKER1_PID" "Broker 1"
    fi

    # Finally broker 0 (cluster initializer)
    if [ -n "$BROKER0_PID" ] && kill -0 "$BROKER0_PID" 2>/dev/null; then
        echo "  Stopping broker 0 (PID: $BROKER0_PID)..."
        kill -s TERM "$BROKER0_PID" 2>/dev/null || true
        wait_for_process "$BROKER0_PID" "Broker 0"
    fi

    echo "All brokers shut down."
}

# Trap Ctrl+C and call cleanup
trap cleanup INT TERM

# Define cluster membership
# All nodes need to know about each other for proper Raft cluster formation
RAFT_PEER_0="0=127.0.0.1:9093"
RAFT_PEER_1="1=127.0.0.1:9095"
RAFT_PEER_2="2=127.0.0.1:9097"
ALL_PEERS="${RAFT_PEER_0},${RAFT_PEER_1},${RAFT_PEER_2}"

# Start brokers in the background
# Broker 0 is the initial leader (lowest ID), but all brokers know about all peers
echo "Starting broker 0 (will initialize cluster)..."
BROKER_ID=0 PORT=9092 HEALTH_PORT=8080 RAFT_LISTEN_ADDR=127.0.0.1:9093 RAFT_PEERS="${RAFT_PEER_1},${RAFT_PEER_2}" target/release/examples/cluster &
BROKER0_PID=$!

# Give broker 0 time to initialize the cluster and become leader
sleep 3

echo "Starting broker 1 (joining cluster)..."
BROKER_ID=1 PORT=9094 HEALTH_PORT=8081 RAFT_LISTEN_ADDR=127.0.0.1:9095 RAFT_PEERS="${RAFT_PEER_0},${RAFT_PEER_2}" target/release/examples/cluster &
BROKER1_PID=$!
sleep 2

echo "Starting broker 2 (joining cluster)..."
BROKER_ID=2 PORT=9096 HEALTH_PORT=8082 RAFT_LISTEN_ADDR=127.0.0.1:9097 RAFT_PEERS="${RAFT_PEER_0},${RAFT_PEER_1}" target/release/examples/cluster &
BROKER2_PID=$!
sleep 5 # Wait for cluster to stabilize

echo ""
echo "====================================="
echo "Multi-broker cluster is running!"
echo "====================================="
echo ""
echo "Broker 0: localhost:9092 (PID: $BROKER0_PID)"
echo "Broker 1: localhost:9094 (PID: $BROKER1_PID)"
echo "Broker 2: localhost:9096 (PID: $BROKER2_PID)"
echo ""
echo "Test with:"
echo "  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test"
echo "  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning"
echo ""
echo "Press Ctrl+C to shut down."

# Wait for all background processes to finish
# This will keep the script running until it's interrupted
wait
