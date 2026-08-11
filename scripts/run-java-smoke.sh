#!/usr/bin/env bash
# Java kafka-clients smoke against a Kafkaesque broker.
#
# Starts an in-process broker if KAFKAESQUE_BOOTSTRAP is unset (requires a
# built `kafkaesque` binary), then runs produce → consume → OffsetCommit
# via kafka-clients 3.7.
#
# Usage:
#   cargo build -p kafkaesque-bin --release
#   ./scripts/run-java-smoke.sh
#   # or against an already-running broker:
#   KAFKAESQUE_BOOTSTRAP=localhost:9092 ./scripts/run-java-smoke.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAVA_SMOKE_DIR="$REPO_ROOT/java-smoke"

if ! command -v mvn >/dev/null 2>&1; then
  echo "run-java-smoke.sh: Maven (mvn) is required" >&2
  exit 1
fi
if ! command -v java >/dev/null 2>&1; then
  echo "run-java-smoke.sh: Java 17+ is required" >&2
  exit 1
fi

BROKER_PID=""
cleanup() {
  if [[ -n "${BROKER_PID}" ]] && kill -0 "${BROKER_PID}" 2>/dev/null; then
    kill "${BROKER_PID}" 2>/dev/null || true
    wait "${BROKER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${BROKER_DATA:-}" ]]; then
    rm -rf "${BROKER_DATA}"
  fi
}
trap cleanup EXIT

if [[ -z "${KAFKAESQUE_BOOTSTRAP:-}" ]]; then
  KAFKAESQUE_BIN="${KAFKAESQUE_BIN:-${KAFKAESQUE_BINARY:-}}"
  if [[ -z "${KAFKAESQUE_BIN}" ]]; then
    for candidate in \
      "$REPO_ROOT/target/release/kafkaesque" \
      "$REPO_ROOT/target/debug/kafkaesque"; do
      if [[ -x "$candidate" ]]; then
        KAFKAESQUE_BIN="$candidate"
        break
      fi
    done
  fi
  if [[ -z "${KAFKAESQUE_BIN}" ]] || [[ ! -x "${KAFKAESQUE_BIN}" ]]; then
    echo "run-java-smoke.sh: build the broker first (cargo build -p kafkaesque-bin --release)" >&2
    exit 1
  fi

  BROKER_DATA="$(mktemp -d -t kafkaesque-java-smoke.XXXXXX)"
  # Ephemeral ports: ask the OS.
  KAFKA_PORT="$(python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
)"
  RAFT_PORT="$(python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
)"

  export CLUSTER_PROFILE="${CLUSTER_PROFILE:-development}"
  export RAFT_CLUSTER_SECRET="${RAFT_CLUSTER_SECRET:-kafkaesque-e2e-shared-secret-min32bytes}"
  export RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE="${RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE:-true}"
  export BROKER_ID=0
  export HOST=127.0.0.1
  export ADVERTISED_HOST=127.0.0.1
  export PORT="${KAFKA_PORT}"
  export RAFT_LISTEN_ADDR="127.0.0.1:${RAFT_PORT}"
  export OBJECT_STORE_TYPE=local
  export DATA_PATH="${BROKER_DATA}"
  export AUTO_CREATE_TOPICS=true
  export DEFAULT_NUM_PARTITIONS=1
  export RUST_LOG="${RUST_LOG:-kafkaesque=info}"

  "${KAFKAESQUE_BIN}" >"${BROKER_DATA}/broker.log" 2>&1 &
  BROKER_PID=$!
  KAFKAESQUE_BOOTSTRAP="127.0.0.1:${KAFKA_PORT}"

  # Wait for TCP accept.
  for _ in $(seq 1 100); do
    if python3 - <<PY
import socket, sys
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", int("${KAFKA_PORT}")))
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
    then
      break
    fi
    if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
      echo "run-java-smoke.sh: broker exited early; log:" >&2
      cat "${BROKER_DATA}/broker.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
fi

export KAFKAESQUE_BOOTSTRAP
echo "run-java-smoke.sh: bootstrap=${KAFKAESQUE_BOOTSTRAP}"

(
  cd "${JAVA_SMOKE_DIR}"
  mvn -q -DskipTests package
  java -jar target/java-smoke-0.1.0.jar
)
