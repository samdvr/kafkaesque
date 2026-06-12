#!/bin/bash
# Run every cargo-fuzz target sequentially for a fixed time budget.
#
# Usage:
#   ./scripts/fuzz-all.sh              # 60s per target (default)
#   ./scripts/fuzz-all.sh 300          # 300s per target
#   FUZZ_TARGETS="api_produce frame_reader" ./scripts/fuzz-all.sh 30
#
# Exits non-zero on the first target that crashes or hits the libFuzzer
# slow-input threshold, so this is safe to wire into CI.

set -euo pipefail

PER_TARGET_SECONDS="${1:-60}"

cd "$(dirname "$0")/.."

if ! command -v cargo-fuzz >/dev/null 2>&1; then
    echo "cargo-fuzz not installed. Run: cargo install cargo-fuzz" >&2
    exit 1
fi

TARGETS=()
if [ -n "${FUZZ_TARGETS:-}" ]; then
    # Caller supplied an explicit list — honour it verbatim.
    read -r -a TARGETS <<< "$FUZZ_TARGETS"
else
    # `mapfile` would be cleaner but isn't in macOS's bash 3.2.
    while IFS= read -r line; do
        TARGETS+=("$line")
    done < <(cargo +nightly fuzz list)
fi

if [ "${#TARGETS[@]}" -eq 0 ]; then
    echo "No fuzz targets found." >&2
    exit 1
fi

echo "Running ${#TARGETS[@]} targets, ${PER_TARGET_SECONDS}s each."
echo

FAILED=()
for t in "${TARGETS[@]}"; do
    echo "=== $t ==="
    if ! cargo +nightly fuzz run "$t" -- -max_total_time="$PER_TARGET_SECONDS"; then
        FAILED+=("$t")
        echo "FAIL: $t" >&2
    fi
    echo
done

if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "Failed targets: ${FAILED[*]}" >&2
    exit 1
fi

echo "All ${#TARGETS[@]} targets passed."
