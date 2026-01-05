#!/bin/sh
# E2E Tests for Kafkaesque
# Runs single-node E2E tests, with optional cluster tests
#
# Usage:
#   ./run-e2e.sh              # Single-node tests only
#   ./run-e2e.sh --cluster    # Include cluster tests

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

RUN_CLUSTER=false
if [ "$1" = "--cluster" ]; then
    RUN_CLUSTER=true
fi

# Use kafkacat or kcat
if command -v kcat >/dev/null 2>&1; then
    KCAT="kcat"
elif command -v kafkacat >/dev/null 2>&1; then
    KCAT="kafkacat"
else
    echo -e "${RED}Neither kcat nor kafkacat found!${NC}"
    exit 1
fi

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Kafkaesque E2E Tests (unit/integration passed at build)   ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

KCAT_OPTS="-X api.version.request=false -X broker.version.fallback=2.0.0"
E2E_FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo -n "  $name... "
    if eval "$cmd" > /tmp/test_out.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/test_out.txt
        E2E_FAILED=1
    fi
}

cleanup() {
    echo "Cleaning up..."
    kill $SERVER_PID 2>/dev/null || true
    kill $BROKER0_PID $BROKER1_PID $BROKER2_PID 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf /tmp/kafkaesque-data /tmp/kafkaesque-cluster-test
}
trap cleanup EXIT

# =============================================================================
# Single-Node Tests
# =============================================================================
echo -e "${YELLOW}[Single-Node Tests]${NC}"

# Start Kafkaesque server
echo "Starting Kafkaesque server..."
mkdir -p /tmp/kafkaesque-data
BROKER_ID=0 \
HOST=127.0.0.1 \
PORT=9092 \
RAFT_LISTEN_ADDR=127.0.0.1:9093 \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-data \
AUTO_CREATE_TOPICS=true \
RUST_LOG=kafkaesque=info \
/app/target/release/examples/cluster > /tmp/server.log 2>&1 &
SERVER_PID=$!

# Wait for server
echo "Waiting for server..."
for i in $(seq 1 30); do
    if nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
        echo -e "${GREEN}Server ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Server failed to start after 30 seconds${NC}"
        echo "Server log:"
        cat /tmp/server.log
        exit 1
    fi
    sleep 1
done

run_test "Metadata" "$KCAT -b 127.0.0.1:9092 -L $KCAT_OPTS"
run_test "Produce" "echo -e 'msg1\nmsg2\nmsg3' | $KCAT -b 127.0.0.1:9092 -t test-topic -P $KCAT_OPTS"
sleep 2
run_test "Consume" "timeout 10 $KCAT -b 127.0.0.1:9092 -t test-topic -C -c 3 -q $KCAT_OPTS | grep -q msg1"
run_test "Keyed produce" "echo 'key1:value1' | $KCAT -b 127.0.0.1:9092 -t keyed-topic -P -K: $KCAT_OPTS"
sleep 2
run_test "Keyed consume" "timeout 10 $KCAT -b 127.0.0.1:9092 -t keyed-topic -C -c 1 -q -f '%k:%s\n' $KCAT_OPTS | grep -q key1:value1"
run_test "Large message" "dd if=/dev/zero bs=1024 count=100 2>/dev/null | base64 | $KCAT -b 127.0.0.1:9092 -t large-topic -P $KCAT_OPTS"
run_test "Message ordering" "for i in \$(seq 1 100); do echo \$i; done | $KCAT -b 127.0.0.1:9092 -t order-topic -P $KCAT_OPTS"
sleep 2
run_test "Verify ordering" "timeout 10 $KCAT -b 127.0.0.1:9092 -t order-topic -C -c 1 -q $KCAT_OPTS | head -1 | grep -q '^1$'"

# Stop single-node server
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

# =============================================================================
# Cluster Tests (optional)
# =============================================================================
if [ "$RUN_CLUSTER" = true ]; then
    echo ""
    echo -e "${YELLOW}[Cluster Tests - 3 Node Raft]${NC}"

    # Wait for port 9092 to be released from single-node server
    echo -n "  Waiting for port 9092 to be released... "
    for i in $(seq 1 15); do
        if ! nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
            echo -e "${GREEN}free${NC}"
            break
        fi
        if [ $i -eq 15 ]; then
            echo -e "${YELLOW}proceeding anyway${NC}"
        fi
        sleep 1
    done

    rm -rf /tmp/kafkaesque-cluster-test
    mkdir -p /tmp/kafkaesque-cluster-test/broker{0,1,2}

    # Start 3-node cluster
    echo "Starting 3-node Raft cluster..."

    BROKER_ID=0 HOST=127.0.0.1 PORT=9092 \
    RAFT_LISTEN_ADDR=127.0.0.1:9093 \
    RAFT_PEERS="1=127.0.0.1:9095,2=127.0.0.1:9097" \
    OBJECT_STORE_TYPE=local DATA_PATH=/tmp/kafkaesque-cluster-test/broker0 \
    AUTO_CREATE_TOPICS=true RUST_LOG=kafkaesque=info \
    /app/target/release/examples/cluster > /tmp/kafkaesque-cluster-test/broker0.log 2>&1 &
    BROKER0_PID=$!

    BROKER_ID=1 HOST=127.0.0.1 PORT=9094 \
    RAFT_LISTEN_ADDR=127.0.0.1:9095 \
    RAFT_PEERS="0=127.0.0.1:9093,2=127.0.0.1:9097" \
    OBJECT_STORE_TYPE=local DATA_PATH=/tmp/kafkaesque-cluster-test/broker1 \
    AUTO_CREATE_TOPICS=true RUST_LOG=kafkaesque=info \
    /app/target/release/examples/cluster > /tmp/kafkaesque-cluster-test/broker1.log 2>&1 &
    BROKER1_PID=$!

    BROKER_ID=2 HOST=127.0.0.1 PORT=9096 \
    RAFT_LISTEN_ADDR=127.0.0.1:9097 \
    RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" \
    OBJECT_STORE_TYPE=local DATA_PATH=/tmp/kafkaesque-cluster-test/broker2 \
    AUTO_CREATE_TOPICS=true RUST_LOG=kafkaesque=info \
    /app/target/release/examples/cluster > /tmp/kafkaesque-cluster-test/broker2.log 2>&1 &
    BROKER2_PID=$!

    # Wait for all brokers
    for port in 9092 9094 9096; do
        echo -n "  Waiting for port $port... "
        for i in $(seq 1 30); do
            if nc -z 127.0.0.1 $port >/dev/null 2>&1; then
                echo -e "${GREEN}ready${NC}"
                break
            fi
            if [ $i -eq 30 ]; then
                echo -e "${RED}timeout${NC}"
                echo ""
                echo "Broker logs:"
                echo "--- Broker 0 (port 9092) ---"
                cat /tmp/kafkaesque-cluster-test/broker0.log 2>/dev/null || echo "(no log)"
                echo "--- Broker 1 (port 9094) ---"
                cat /tmp/kafkaesque-cluster-test/broker1.log 2>/dev/null || echo "(no log)"
                echo "--- Broker 2 (port 9096) ---"
                cat /tmp/kafkaesque-cluster-test/broker2.log 2>/dev/null || echo "(no log)"
                exit 1
            fi
            sleep 1
        done
    done

    echo "  Waiting for Raft leader election..."
    sleep 5

    run_test "Cluster metadata (broker 0)" "$KCAT -b 127.0.0.1:9092 -L $KCAT_OPTS"
    run_test "Cluster metadata (broker 1)" "$KCAT -b 127.0.0.1:9094 -L $KCAT_OPTS"
    run_test "Cluster metadata (broker 2)" "$KCAT -b 127.0.0.1:9096 -L $KCAT_OPTS"

    run_test "Cross-broker produce" "echo 'cross-msg' | $KCAT -b 127.0.0.1:9092 -t cluster-test -P $KCAT_OPTS"
    sleep 2
    run_test "Cross-broker consume (broker 1)" "timeout 10 $KCAT -b 127.0.0.1:9094 -t cluster-test -C -c 1 -q $KCAT_OPTS | grep -q cross-msg"
    run_test "Cross-broker consume (broker 2)" "timeout 10 $KCAT -b 127.0.0.1:9096 -t cluster-test -C -c 1 -q $KCAT_OPTS | grep -q cross-msg"

    # Failover test
    echo "  Testing broker failover..."
    kill $BROKER2_PID 2>/dev/null || true
    wait $BROKER2_PID 2>/dev/null || true
    sleep 2

    run_test "Produce after failover" "echo 'failover-msg' | $KCAT -b 127.0.0.1:9092 -t failover-test -P $KCAT_OPTS"
    sleep 2
    run_test "Consume after failover" "timeout 10 $KCAT -b 127.0.0.1:9094 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"

    kill $BROKER0_PID $BROKER1_PID 2>/dev/null || true
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
if [ $E2E_FAILED -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    ALL E2E TESTS PASSED                      ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║                    E2E TESTS FAILED                          ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
    cat /tmp/server.log 2>/dev/null || true
    exit 1
fi
