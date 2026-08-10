#!/bin/bash
# Cluster E2E Tests for Kafkaesque
# Tests multi-broker Raft cluster functionality
#
# Requirements:
# - kafkacat/kcat installed
# - Kafkaesque binary built (cargo build --release -p kafkaesque-bin --bin kafkaesque)

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

BINARY="${KAFKAESQUE_BINARY:-./target/release/kafkaesque}"
if [ ! -f "$BINARY" ]; then
    BINARY="./target/debug/kafkaesque"
fi

# E2E scripts drive the wire protocol via kcat. Use the development profile
# so tests do not require production ACL/join-token bootstrap.
export CLUSTER_PROFILE="${CLUSTER_PROFILE:-development}"
export RAFT_CLUSTER_SECRET="${RAFT_CLUSTER_SECRET:-kafkaesque-e2e-shared-secret-min32bytes}"
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}Kafkaesque binary not found. Run: cargo build --release -p kafkaesque-bin --bin kafkaesque${NC}"
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

# Create data directories — all brokers share one local object-store root so
# partition data survives ownership handoff (mirrors shared S3 in production).
rm -rf /tmp/kafkaesque-cluster-test
mkdir -p /tmp/kafkaesque-cluster-test/shared
SHARED_DATA_PATH=/tmp/kafkaesque-cluster-test/shared

KCAT_OPTS="-X api.version.request=false -X broker.version.fallback=2.0.0"
E2E_FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo -n "  $name... "
    if eval "$cmd" > /tmp/kafkaesque-cluster-test/test_out.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/kafkaesque-cluster-test/test_out.txt
        E2E_FAILED=1
    fi
    # Always succeed: the script runs under `set -e`, so returning non-zero
    # here would abort at the first failing test and hide every later one.
    # E2E_FAILED is what drives the exit code in the summary below.
    return 0
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

# Block until every partition of $1 reports a leader (as seen from broker $2),
# or until $3 seconds elapse.
#
# A freshly auto-created topic has DEFAULT_NUM_PARTITIONS partitions, and each
# is acquired by whichever broker the consistent-hash ring assigns it to. That
# is a distributed convergence, not an atomic step: partitions appear as
# `Leader not available` in metadata until their owner picks them up. Consuming
# with kcat spans *all* partitions, so a consume issued mid-convergence stalls
# on the not-yet-owned ones and returns nothing.
#
# The previous shape here was a flat `sleep 2`, which is a bet on convergence
# latency rather than an observation of it — it failed reliably in CI. Waiting
# on the actual condition removes the race in both directions: it does not
# flake when convergence is slow, and it does not waste 2s when it is fast.
wait_for_partition_leaders() {
    local topic=$1
    local broker=$2
    local timeout_secs=${3:-30}
    echo -n "  Waiting for all partitions of $topic to have leaders... "
    for i in $(seq 1 "$timeout_secs"); do
        # `-L -t <topic>` lists partitions with their leader; kcat prints
        # "Leader not available" for any partition whose owner is unknown.
        if ! timeout 10 $KCAT -b 127.0.0.1:$broker -L -t "$topic" $KCAT_OPTS 2>/dev/null \
             | grep -q "Leader not available"; then
            echo -e "${GREEN}converged (${i}s)${NC}"
            return 0
        fi
        sleep 1
    done
    echo -e "${YELLOW}timeout after ${timeout_secs}s${NC}"
    echo "  --- metadata for $topic at timeout ---"
    timeout 15 $KCAT -b 127.0.0.1:$broker -L -t "$topic" $KCAT_OPTS 2>&1 | sed 's/^/    /' || true
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
DATA_PATH="$SHARED_DATA_PATH" \
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
DATA_PATH="$SHARED_DATA_PATH" \
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
DATA_PATH="$SHARED_DATA_PATH" \
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

# Consume with consumer group from different brokers.
# Two kcat details matter here: in -G mode every trailing argument is treated
# as a topic name (so flags come first), and a brand-new group has no
# committed offsets, which librdkafka resets to *latest* by default — i.e. it
# would skip the messages produced above.
run_test "Consumer group from broker 0" "timeout 20 $KCAT -b 127.0.0.1:9092 -G test-group -c 5 -q -X auto.offset.reset=earliest $KCAT_OPTS group-test 2>/dev/null | wc -l | grep -q 5"

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

# Restart broker 2 — same DATA_PATH it started with, so it recovers its own
# Raft log ({DATA_PATH}/raft/log/2) and rejoins as the member it already is
# instead of coming back as an amnesiac voter.
echo "  Restarting broker 2..."
BROKER_ID=2 \
HOST=127.0.0.1 \
PORT=9096 \
RAFT_LISTEN_ADDR=127.0.0.1:9097 \
RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" \
OBJECT_STORE_TYPE=local \
DATA_PATH="$SHARED_DATA_PATH" \
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

# One 500KB message, produced as a single newline-free line.
#
# The payload has to be built without `base64`: GNU base64 wraps at 76
# columns, so piping it into `kcat -P` (newline-delimited) sends thousands of
# tiny messages instead of one big one, while BSD base64 doesn't wrap and
# blows past kcat's own 1000001-byte input cap. `tr` over /dev/zero gives the
# same payload on both.
LARGE_MSG_BYTES=500000
run_test "Produce 500KB message" "head -c $LARGE_MSG_BYTES /dev/zero | tr '\\0' 'x' | $KCAT -b 127.0.0.1:9092 -t large-msg-test -P $KCAT_OPTS"
sleep 2
run_test "Consume 500KB from another broker" "timeout 30 $KCAT -b 127.0.0.1:9094 -t large-msg-test -C -c 1 -q $KCAT_OPTS | wc -c | awk '{exit (\$1 >= $LARGE_MSG_BYTES ? 0 : 1)}'"

# =============================================================================
# Test 8: Multiple topics simultaneously
# =============================================================================
echo ""
echo -e "${BLUE}[Multiple Topics]${NC}"

# Bounded per-produce: a stuck producer here would otherwise park the wait
# below and the whole job would die on the CI step timeout with no output.
# Wait on the producer PIDs specifically — a bare `wait` would also wait on
# the three broker processes started with `&` above, i.e. forever.
#
# Each produce is asserted. The previous shape backgrounded all five and
# swallowed their exit codes with `|| true`, so a failed produce was
# indistinguishable from a failed consume — the suite reported "Verify
# topic-a ✗" while the actual fault was upstream. Produce serially through
# run_test so the failing side is named.
for topic in topic-a topic-b topic-c topic-d topic-e; do
    run_test "Produce to $topic" \
      "echo msg-for-$topic | timeout 30 $KCAT -b 127.0.0.1:9092 -t $topic -P $KCAT_OPTS"
done

# Wait for leadership to converge on each topic we are about to read, from
# the broker we are about to read it from. This is an assertion too: a topic
# that never converges fails the suite instead of silently producing an
# empty consume.
for spec in "topic-a 9094" "topic-c 9096" "topic-e 9092"; do
    set -- $spec
    run_test "Leaders converged for $1" "wait_for_partition_leaders $1 $2 30"
done

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
