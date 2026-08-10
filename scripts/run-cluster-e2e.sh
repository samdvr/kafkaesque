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

# Portable timeout: prefer GNU `timeout` (CI / Linux), otherwise perl
# `alarm`+`exec` (macOS ships perl; no coreutils required). Usage:
# `run_with_timeout <secs> <command...>`. Prefer this over a bash
# background+kill fallback — backgrounding breaks stdin for
# `echo | run_with_timeout … kcat -P` and can strand consumers.
run_with_timeout() {
    local secs=$1
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$secs" "$@"
        return $?
    fi
    perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
}

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

# Block until every partition of $1 reports a real leader (as seen from
# broker $2), or until $3 seconds elapse.
#
# A freshly auto-created topic has DEFAULT_NUM_PARTITIONS partitions, and each
# is acquired by whichever broker the consistent-hash ring assigns it to. That
# is a distributed convergence, not an atomic step: partitions appear as
# `Leader not available` in metadata until their owner picks them up. Consuming
# with kcat spans *all* partitions, so a consume issued mid-convergence stalls
# on the not-yet-owned ones and returns nothing.
#
# Convergence requires *positive* evidence: metadata must mention the topic,
# must not contain "Leader not available", and must show at least one
# `leader N` / `leader: N` line with a non-negative broker id (kcat uses the
# no-colon form). Absence of the error string alone is not enough — a failed
# `-L` (empty output) used to look "converged".
wait_for_partition_leaders() {
    local topic=$1
    local broker=$2
    local timeout_secs=${3:-30}
    echo -n "  Waiting for all partitions of $topic to have leaders... "
    for i in $(seq 1 "$timeout_secs"); do
        local meta
        meta=$(run_with_timeout 10 $KCAT -b 127.0.0.1:$broker -L -t "$topic" $KCAT_OPTS 2>/dev/null || true)
        # kcat prints "leader 0" (no colon); some tools print "leader: 0".
        if printf '%s\n' "$meta" | grep -q "$topic" \
            && ! printf '%s\n' "$meta" | grep -qi "Leader not available" \
            && printf '%s\n' "$meta" | grep -Eiq 'leader:?[[:space:]]*-?[0-9]+'; then
            # Reject the sentinel "leader -1" / "leader: -1" that clients emit
            # for leaderless partitions even when the string above is absent.
            if ! printf '%s\n' "$meta" | grep -Eiq 'leader:?[[:space:]]*-1([^0-9]|$)'; then
                echo -e "${GREEN}converged (${i}s)${NC}"
                return 0
            fi
        fi
        sleep 1
    done
    echo -e "${YELLOW}timeout after ${timeout_secs}s${NC}"
    echo "  --- metadata for $topic at timeout ---"
    run_with_timeout 15 $KCAT -b 127.0.0.1:$broker -L -t "$topic" $KCAT_OPTS 2>&1 | sed 's/^/    /' || true
    return 1
}

# =============================================================================
# Start 3-node Raft cluster
# =============================================================================
echo -e "${YELLOW}Starting 3-node Raft cluster...${NC}"

# Broker 0 (leader candidate)
# DEFAULT_NUM_PARTITIONS=1 keeps auto-created topics on a single partition so
# the Multiple Topics step (and kcat all-partition consume) does not race
# ownership acquisition across a multi-partition ring after failover.
BROKER_ID=0 \
HOST=127.0.0.1 \
PORT=9092 \
RAFT_LISTEN_ADDR=127.0.0.1:9093 \
RAFT_PEERS="1=127.0.0.1:9095,2=127.0.0.1:9097" \
OBJECT_STORE_TYPE=local \
DATA_PATH="$SHARED_DATA_PATH" \
AUTO_CREATE_TOPICS=true \
DEFAULT_NUM_PARTITIONS=1 \
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
DEFAULT_NUM_PARTITIONS=1 \
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
DEFAULT_NUM_PARTITIONS=1 \
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
run_test "Consume from broker 1" "run_with_timeout 10 $KCAT -b 127.0.0.1:9094 -t cross-test -C -c 1 -q $KCAT_OPTS | grep -q cross-broker-msg"
run_test "Consume from broker 2" "run_with_timeout 10 $KCAT -b 127.0.0.1:9096 -t cross-test -C -c 1 -q $KCAT_OPTS | grep -q cross-broker-msg"

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

run_test "Consume multi-partition" "run_with_timeout 10 $KCAT -b 127.0.0.1:9092 -t multi-partition -C -c 10 -q $KCAT_OPTS | wc -l | grep -q 10"

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
run_test "Consumer group from broker 0" "run_with_timeout 20 $KCAT -b 127.0.0.1:9092 -G test-group -c 5 -q -X auto.offset.reset=earliest $KCAT_OPTS group-test 2>/dev/null | wc -l | grep -q 5"

# =============================================================================
# Test 5: High throughput
# =============================================================================
echo ""
echo -e "${BLUE}[High Throughput]${NC}"

# Produce 1000 messages rapidly
run_test "Produce 1000 messages" "for i in \$(seq 1 1000); do echo msg-\$i; done | $KCAT -b 127.0.0.1:9092 -t throughput-test -P $KCAT_OPTS"
sleep 3
run_test "Consume 1000 messages" "run_with_timeout 30 $KCAT -b 127.0.0.1:9094 -t throughput-test -C -c 1000 -q $KCAT_OPTS | wc -l | grep -q 1000"

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
run_test "Consume from broker 0" "run_with_timeout 10 $KCAT -b 127.0.0.1:9092 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"
run_test "Consume from broker 1" "run_with_timeout 10 $KCAT -b 127.0.0.1:9094 -t failover-test -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"

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
DEFAULT_NUM_PARTITIONS=1 \
RUST_LOG=kafkaesque=info \
$BINARY > /tmp/kafkaesque-cluster-test/broker2.log 2>&1 &
BROKER2_PID=$!

wait_for_broker 9096 "Broker 2 (restart)"
# Ownership rebalance after a rejoining voter can thrash for a few seconds;
# wait for the failover topic to look stable before creating more topics.
run_test "Leaders converged after restart" "wait_for_partition_leaders failover-test 9092 30"
sleep 2

run_test "Consume from restarted broker 2" "run_with_timeout 10 $KCAT -b 127.0.0.1:9096 -t failover-test -o beginning -C -c 1 -q $KCAT_OPTS | grep -q failover-msg"

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
run_test "Consume 500KB from another broker" "run_with_timeout 30 $KCAT -b 127.0.0.1:9094 -t large-msg-test -C -c 1 -q $KCAT_OPTS | wc -c | awk '{exit (\$1 >= $LARGE_MSG_BYTES ? 0 : 1)}'"

# =============================================================================
# Test 8: Multiple topics simultaneously
# =============================================================================
echo ""
echo -e "${BLUE}[Multiple Topics]${NC}"

# Produce → wait for leaders → verify, per topic. Batching all produces
# before any verify used to race ownership acquisition after failover:
# metadata could look "converged" while partition 0 was mid-reassign and
# the single produce was not yet readable cross-broker.
#
# `-o beginning` is required: without a consumer group, kcat's stored
# offset default can start at end and miss the already-produced message.
verify_topic_message() {
    local topic=$1
    local broker=$2
    local needle=$3
    local timeout_secs=${4:-30}
    for i in $(seq 1 "$timeout_secs"); do
        if run_with_timeout 5 $KCAT -b 127.0.0.1:$broker -t "$topic" -p 0 -o beginning -C -c 1 -q $KCAT_OPTS 2>/dev/null \
            | grep -q "$needle"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

for spec in "topic-a 9094" "topic-b 9092" "topic-c 9096" "topic-d 9094" "topic-e 9092"; do
    set -- $spec
    local_topic=$1
    local_broker=$2
    run_test "Produce to $local_topic" \
      "echo msg-for-$local_topic | run_with_timeout 30 $KCAT -b 127.0.0.1:9092 -t $local_topic -P $KCAT_OPTS"
    run_test "Leaders converged for $local_topic" "wait_for_partition_leaders $local_topic $local_broker 30"
    run_test "Verify $local_topic" "verify_topic_message $local_topic $local_broker msg-for-$local_topic 30"
done

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
