#!/bin/bash
# Cluster E2E Tests for Kafkaesque
# Tests multi-broker Raft cluster functionality
#
# Requirements:
# - kafkacat/kcat installed
# - Kafkaesque binary built (cargo build --release --example cluster)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Use kafkacat or kcat
if command -v kcat &> /dev/null; then
    KCAT="kcat"
elif command -v kafkacat &> /dev/null; then
    KCAT="kafkacat"
else
    echo -e "${RED}Neither kcat nor kafkacat found!${NC}"
    exit 1
fi

BINARY="${KAFKAESQUE_BINARY:-./target/release/examples/cluster}"
if [ ! -f "$BINARY" ]; then
    BINARY="./target/debug/examples/cluster"
fi
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}Kafkaesque binary not found. Run: cargo build --release --example cluster${NC}"
    exit 1
fi

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              Kafkaesque Cluster E2E Tests                          ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $BROKER0_PID $BROKER1_PID $BROKER2_PID 2>/dev/null || true
    wait $BROKER0_PID $BROKER1_PID $BROKER2_PID 2>/dev/null || true
    rm -rf /tmp/kafkaesque-cluster-test
    echo "Cleanup complete"
}
trap cleanup EXIT

# Create data directories
rm -rf /tmp/kafkaesque-cluster-test
mkdir -p /tmp/kafkaesque-cluster-test/broker0
mkdir -p /tmp/kafkaesque-cluster-test/broker1
mkdir -p /tmp/kafkaesque-cluster-test/broker2

KCAT_OPTS="-X api.version.request=false -X broker.version.fallback=2.0.0"
E2E_FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo -n "  $name... "
    if eval "$cmd" > /tmp/kafkaesque-cluster-test/test_out.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        return 0
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/kafkaesque-cluster-test/test_out.txt
        E2E_FAILED=1
        return 1
    fi
}

wait_for_broker() {
    local port=$1
    local name=$2
    echo -n "  Waiting for $name (port $port)... "
    for i in $(seq 1 30); do
        if nc -z 127.0.0.1 $port 2>/dev/null; then
            echo -e "${GREEN}ready${NC}"
            return 0
        fi
        sleep 1
    done
    echo -e "${RED}timeout${NC}"
    echo ""
    echo "Broker logs:"
    echo "--- Broker 0 (port 9092) ---"
    cat /tmp/kafkaesque-cluster-test/broker0.log 2>/dev/null || echo "(no log)"
    echo "--- Broker 1 (port 9094) ---"
    cat /tmp/kafkaesque-cluster-test/broker1.log 2>/dev/null || echo "(no log)"
    echo "--- Broker 2 (port 9096) ---"
    cat /tmp/kafkaesque-cluster-test/broker2.log 2>/dev/null || echo "(no log)"
    return 1
}

# =============================================================================
# Start 3-node Raft cluster
# =============================================================================
echo -e "${YELLOW}Starting 3-node Raft cluster...${NC}"

# Broker 0 (leader candidate)
BROKER_ID=0 \
HOST=127.0.0.1 \
PORT=9092 \
RAFT_LISTEN_ADDR=127.0.0.1:9093 \
RAFT_PEERS="1=127.0.0.1:9095,2=127.0.0.1:9097" \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-cluster-test/broker0 \
AUTO_CREATE_TOPICS=true \
RUST_LOG=kafkaesque=info \
$BINARY > /tmp/kafkaesque-cluster-test/broker0.log 2>&1 &
BROKER0_PID=$!

# Broker 1
BROKER_ID=1 \
HOST=127.0.0.1 \
PORT=9094 \
RAFT_LISTEN_ADDR=127.0.0.1:9095 \
RAFT_PEERS="0=127.0.0.1:9093,2=127.0.0.1:9097" \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-cluster-test/broker1 \
AUTO_CREATE_TOPICS=true \
RUST_LOG=kafkaesque=info \
$BINARY > /tmp/kafkaesque-cluster-test/broker1.log 2>&1 &
BROKER1_PID=$!

# Broker 2
BROKER_ID=2 \
HOST=127.0.0.1 \
PORT=9096 \
RAFT_LISTEN_ADDR=127.0.0.1:9097 \
RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-cluster-test/broker2 \
AUTO_CREATE_TOPICS=true \
RUST_LOG=kafkaesque=info \
$BINARY > /tmp/kafkaesque-cluster-test/broker2.log 2>&1 &
BROKER2_PID=$!

# Wait for all brokers to start
wait_for_broker 9092 "Broker 0" || exit 1
wait_for_broker 9094 "Broker 1" || exit 1
wait_for_broker 9096 "Broker 2" || exit 1

# Give Raft time to elect a leader
echo "  Waiting for Raft leader election..."
sleep 5

echo ""
echo -e "${YELLOW}Running cluster tests...${NC}"

# =============================================================================
# Test 1: Metadata from all brokers
# =============================================================================
echo ""
echo -e "${BLUE}[Cluster Discovery]${NC}"
run_test "Metadata from broker 0" "$KCAT -b 127.0.0.1:9092 -L $KCAT_OPTS"
run_test "Metadata from broker 1" "$KCAT -b 127.0.0.1:9094 -L $KCAT_OPTS"
run_test "Metadata from broker 2" "$KCAT -b 127.0.0.1:9096 -L $KCAT_OPTS"

# =============================================================================
# Test 2: Produce to one broker, consume from another
# =============================================================================
echo ""
echo -e "${BLUE}[Cross-Broker Produce/Consume]${NC}"
run_test "Produce to broker 0" "echo 'cross-broker-msg' | $KCAT -b 127.0.0.1:9092 -t cross-test -P $KCAT_OPTS"
sleep 2
run_test "Consume from broker 1" "timeout 10 $KCAT -b 127.0.0.1:9094 -t cross-test -C -c 1 -q $KCAT_OPTS | grep -q cross-broker-msg"
run_test "Consume from broker 2" "timeout 10 $KCAT -b 127.0.0.1:9096 -t cross-test -C -c 1 -q $KCAT_OPTS | grep -q cross-broker-msg"

# =============================================================================
# Test 3: Multi-partition topic
# =============================================================================
echo ""
echo -e "${BLUE}[Multi-Partition Topics]${NC}"

# Produce messages with keys to distribute across partitions
for i in $(seq 1 10); do
    echo "key$i:value$i" | $KCAT -b 127.0.0.1:9092 -t multi-partition -P -K: $KCAT_OPTS
done
sleep 2

run_test "Consume multi-partition" "timeout 10 $KCAT -b 127.0.0.1:9092 -t multi-partition -C -c 10 -q $KCAT_OPTS | wc -l | grep -q 10"

# =============================================================================
# Test 4: Consumer group across cluster
# =============================================================================
echo ""
echo -e "${BLUE}[Consumer Groups]${NC}"

# Produce messages
for i in $(seq 1 5); do
    echo "group-msg-$i" | $KCAT -b 127.0.0.1:9092 -t group-test -P $KCAT_OPTS
done
sleep 2

# Consume with consumer group from different brokers
run_test "Consumer group from broker 0" "timeout 10 $KCAT -b 127.0.0.1:9092 -G test-group group-test -c 5 -q $KCAT_OPTS 2>/dev/null | wc -l | grep -q 5"

# =============================================================================
# Test 5: High throughput
# =============================================================================
echo ""
echo -e "${BLUE}[High Throughput]${NC}"

# Produce 1000 messages rapidly
run_test "Produce 1000 messages" "for i in \$(seq 1 1000); do echo msg-\$i; done | $KCAT -b 127.0.0.1:9092 -t throughput-test -P $KCAT_OPTS"
sleep 3
run_test "Consume 1000 messages" "timeout 30 $KCAT -b 127.0.0.1:9094 -t throughput-test -C -c 1000 -q $KCAT_OPTS | wc -l | grep -q 1000"

# =============================================================================
# Test 6: Broker failover (kill one broker, verify others still work)
# =============================================================================
echo ""
echo -e "${BLUE}[Broker Failover]${NC}"

echo "  Killing broker 2..."
kill $BROKER2_PID 2>/dev/null || true
wait $BROKER2_PID 2>/dev/null || true
sleep 2

run_test "Produce after broker 2 down" "echo 'failover-msg' | $KCAT -b 127.0.0.1:9092 -t failover-test -P $KCAT_OPTS"
sleep 2
run_test "Consume from broker 0" "timeout 10 $KCAT -b 127.0.0.1:9092 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"
run_test "Consume from broker 1" "timeout 10 $KCAT -b 127.0.0.1:9094 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"

# Restart broker 2
echo "  Restarting broker 2..."
BROKER_ID=2 \
HOST=127.0.0.1 \
PORT=9096 \
RAFT_LISTEN_ADDR=127.0.0.1:9097 \
RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-cluster-test/broker2 \
AUTO_CREATE_TOPICS=true \
RUST_LOG=kafkaesque=info \
$BINARY > /tmp/kafkaesque-cluster-test/broker2.log 2>&1 &
BROKER2_PID=$!

wait_for_broker 9096 "Broker 2 (restart)"
sleep 3

run_test "Consume from restarted broker 2" "timeout 10 $KCAT -b 127.0.0.1:9096 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"

# =============================================================================
# Test 7: Large messages across cluster
# =============================================================================
echo ""
echo -e "${BLUE}[Large Messages]${NC}"

run_test "Produce 1MB message" "dd if=/dev/zero bs=1024 count=1024 2>/dev/null | base64 | $KCAT -b 127.0.0.1:9092 -t large-msg-test -P $KCAT_OPTS"
sleep 2
run_test "Consume 1MB from another broker" "timeout 30 $KCAT -b 127.0.0.1:9094 -t large-msg-test -C -c 1 -q $KCAT_OPTS | wc -c | awk '{exit (\$1 > 1000000 ? 0 : 1)}'"

# =============================================================================
# Test 8: Multiple topics simultaneously
# =============================================================================
echo ""
echo -e "${BLUE}[Multiple Topics]${NC}"

for topic in topic-a topic-b topic-c topic-d topic-e; do
    echo "msg-for-$topic" | $KCAT -b 127.0.0.1:9092 -t $topic -P $KCAT_OPTS &
done
wait
sleep 2

run_test "Verify topic-a" "timeout 10 $KCAT -b 127.0.0.1:9094 -t topic-a -C -c 1 -q $KCAT_OPTS | grep -q topic-a"
run_test "Verify topic-c" "timeout 10 $KCAT -b 127.0.0.1:9096 -t topic-c -C -c 1 -q $KCAT_OPTS | grep -q topic-c"
run_test "Verify topic-e" "timeout 10 $KCAT -b 127.0.0.1:9092 -t topic-e -C -c 1 -q $KCAT_OPTS | grep -q topic-e"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ $E2E_FAILED -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              ALL CLUSTER E2E TESTS PASSED                    ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║              SOME CLUSTER TESTS FAILED                       ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Broker logs:"
    echo "--- Broker 0 ---"
    tail -50 /tmp/kafkaesque-cluster-test/broker0.log
    echo "--- Broker 1 ---"
    tail -50 /tmp/kafkaesque-cluster-test/broker1.log
    echo "--- Broker 2 ---"
    tail -50 /tmp/kafkaesque-cluster-test/broker2.log
    exit 1
fi
