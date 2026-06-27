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

# Locate the broker binary. The CI image lays the release binary at
# `/app/target/release/kafkaesque`; a local checkout lands it under the
# repo's `target/release/`. Override with `KAFKAESQUE_BIN=...` for any
# other layout. Failing here with a clear error beats running with an
# stale binary on $PATH.
if [ -z "${KAFKAESQUE_BIN:-}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    for candidate in \
        "/app/target/release/kafkaesque" \
        "$REPO_ROOT/target/release/kafkaesque" \
        "$REPO_ROOT/target/x86_64-unknown-linux-musl/release/kafkaesque"; do
        if [ -x "$candidate" ]; then
            KAFKAESQUE_BIN="$candidate"
            break
        fi
    done
fi

if [ -z "${KAFKAESQUE_BIN:-}" ] || [ ! -x "$KAFKAESQUE_BIN" ]; then
    echo -e "${RED}Could not locate kafkaesque binary.${NC}"
    echo "  Tried: /app/target/release/kafkaesque, target/release/kafkaesque, target/x86_64-unknown-linux-musl/release/kafkaesque"
    echo "  Set KAFKAESQUE_BIN=/path/to/kafkaesque to override, or run 'cargo build --release' first."
    exit 1
fi
echo "Using broker binary: $KAFKAESQUE_BIN"
echo ""

KCAT_OPTS="-X api.version.request=false -X broker.version.fallback=2.0.0"
E2E_FAILED=0

# E2E scripts drive the wire protocol via kcat. Use the development profile
# so tests do not require production ACL/join-token bootstrap; production
# config gates are covered by tests/config_env_tests.rs.
export CLUSTER_PROFILE="${CLUSTER_PROFILE:-development}"
export RAFT_CLUSTER_SECRET="${RAFT_CLUSTER_SECRET:-kafkaesque-e2e-shared-secret-min32bytes}"
export RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE="${RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE:-true}"

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
#
# DEFAULT_NUM_PARTITIONS=1: this is a wire-protocol smoke test, not a
# partitioning test. On a multi-partition topic only partition 0 is owned the
# instant the topic is auto-created; the remaining partitions are acquired
# lazily by the background ownership loop (ownership_check_interval, ~5s) and
# until then report LeaderNotAvailable in metadata. A real librdkafka client
# (kcat) then churns on metadata refreshes and a keyless/keyed message can hash
# to a not-yet-owned partition, so produce stalls and the tight consume
# timeouts below flake. Pinning every topic to a single partition removes that
# race so the smoke test deterministically exercises produce/consume/metadata.
# (The in-process rdkafka_e2e tests use single-partition topics for the same
# reason.) Multi-partition behavior is covered by run-cluster-e2e.sh.
echo "Starting Kafkaesque server..."
mkdir -p /tmp/kafkaesque-data
BROKER_ID=0 \
HOST=127.0.0.1 \
PORT=9092 \
RAFT_LISTEN_ADDR=127.0.0.1:9093 \
OBJECT_STORE_TYPE=local \
DATA_PATH=/tmp/kafkaesque-data \
AUTO_CREATE_TOPICS=true \
DEFAULT_NUM_PARTITIONS=1 \
RUST_LOG=kafkaesque=info \
"$KAFKAESQUE_BIN" > /tmp/server.log 2>&1 &
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
run_test "Consume" "timeout 10 $KCAT -b 127.0.0.1:9092 -t test-topic -C -o beginning -c 3 -q $KCAT_OPTS | grep -q msg1"
run_test "Keyed produce" "echo 'key1:value1' | $KCAT -b 127.0.0.1:9092 -t keyed-topic -P -K: $KCAT_OPTS"
sleep 2
run_test "Keyed consume" "timeout 10 $KCAT -b 127.0.0.1:9092 -t keyed-topic -C -o beginning -c 1 -q -f '%k:%s\n' $KCAT_OPTS | grep -q key1:value1"
run_test "Large message" "dd if=/dev/zero bs=1024 count=100 2>/dev/null | base64 | $KCAT -b 127.0.0.1:9092 -t large-topic -P $KCAT_OPTS"
# Server runs with DEFAULT_NUM_PARTITIONS=1 (see server start above), so every
# topic has exactly one partition: keyless and keyed messages alike all land in
# partition 0, and reading offset 0 deterministically yields the first message
# produced. (Kafka only orders within a partition.)
run_test "Message ordering" "for i in \$(seq 1 100); do echo \"k:\$i\"; done | $KCAT -b 127.0.0.1:9092 -t order-topic -P -K: $KCAT_OPTS"
sleep 2
run_test "Verify ordering" "timeout 10 $KCAT -b 127.0.0.1:9092 -t order-topic -C -o beginning -c 1 -q $KCAT_OPTS | head -1 | grep -q '^1$'"

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
    "$KAFKAESQUE_BIN" > /tmp/kafkaesque-cluster-test/broker0.log 2>&1 &
    BROKER0_PID=$!

    BROKER_ID=1 HOST=127.0.0.1 PORT=9094 \
    RAFT_LISTEN_ADDR=127.0.0.1:9095 \
    RAFT_PEERS="0=127.0.0.1:9093,2=127.0.0.1:9097" \
    OBJECT_STORE_TYPE=local DATA_PATH=/tmp/kafkaesque-cluster-test/broker1 \
    AUTO_CREATE_TOPICS=true RUST_LOG=kafkaesque=info \
    "$KAFKAESQUE_BIN" > /tmp/kafkaesque-cluster-test/broker1.log 2>&1 &
    BROKER1_PID=$!

    BROKER_ID=2 HOST=127.0.0.1 PORT=9096 \
    RAFT_LISTEN_ADDR=127.0.0.1:9097 \
    RAFT_PEERS="0=127.0.0.1:9093,1=127.0.0.1:9095" \
    OBJECT_STORE_TYPE=local DATA_PATH=/tmp/kafkaesque-cluster-test/broker2 \
    AUTO_CREATE_TOPICS=true RUST_LOG=kafkaesque=info \
    "$KAFKAESQUE_BIN" > /tmp/kafkaesque-cluster-test/broker2.log 2>&1 &
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
