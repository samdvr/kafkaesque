#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Building benchmark example..."
cargo build --release --example benchmark

# Set the list of brokers for the benchmark client
export BROKER="127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"

echo "Running benchmark against multi-broker cluster ($BROKER)..."

# You can customize benchmark parameters here by uncommenting and setting them:
# export MESSAGE_COUNT=100000
# export MESSAGE_SIZE=1024
# export TOPIC="my-benchmark"
# export CONCURRENCY=16

target/release/examples/benchmark
