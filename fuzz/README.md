# Fuzzing

This directory contains [`cargo-fuzz`](https://rust-fuzz.github.io/book/cargo-fuzz.html)
targets covering every attacker-reachable parser, decoder, and validator in the
broker. Each target is documented at the top of its source file with the
property it asserts and why a panic in that surface matters.

## Prerequisites

Fuzzing uses libFuzzer, which requires a nightly toolchain.

```bash
# One-time install of cargo-fuzz.
cargo install cargo-fuzz

# Nightly is required by libfuzzer-sys.
rustup toolchain install nightly
```

You don't need to switch the project's default toolchain — the commands below
pin nightly only for the fuzz invocation.

## Listing Targets

```bash
cargo +nightly fuzz list
```

## Running a Target

From the repo root:

```bash
# Run forever (Ctrl-C to stop). libFuzzer will mutate inputs from `corpus/<target>/`
# in-place, growing the corpus as it discovers new coverage.
cargo +nightly fuzz run api_produce

# Time-bounded run — useful for CI and quick smoke checks.
cargo +nightly fuzz run api_produce -- -max_total_time=60

# Bounded by iteration count.
cargo +nightly fuzz run frame_reader -- -runs=1000000

# Parallelise across cores (one worker per job).
cargo +nightly fuzz run record_batch -- -jobs=8 -workers=8

# Cap input size (some targets already enforce their own MAX_INPUT;
# this is a coarser libFuzzer-level cap).
cargo +nightly fuzz run validators -- -max_len=4096
```

Anything after `--` is forwarded to libFuzzer; see
[`man libFuzzer`](https://llvm.org/docs/LibFuzzer.html#options) for the full
list.

## Running All Targets

`cargo-fuzz` has no built-in "run all" command (libFuzzer is one process per
target), so a helper script loops `cargo +nightly fuzz list` with a per-target
time budget:

```bash
# 60s per target (default).
./scripts/fuzz-all.sh

# 5 minutes per target.
./scripts/fuzz-all.sh 300

# Subset only.
FUZZ_TARGETS="api_produce frame_reader" ./scripts/fuzz-all.sh 30

# Same thing via the Makefile.
make fuzz                # 60s per target
make fuzz FUZZ_TIME=300  # 5 minutes per target
```

The script exits non-zero on the first failing target, which makes it suitable
for CI smoke runs. For longer campaigns, target a single fuzzer directly so
libFuzzer can spend its time mutating one corpus.

To just check that every target still compiles after a refactor (no
mutation runs):

```bash
cargo +nightly fuzz check
```

## Reproducing a Crash

When libFuzzer finds a crash it writes the offending input to
`fuzz/artifacts/<target>/crash-<sha1>` and prints the stack trace. To rerun a
single artifact:

```bash
cargo +nightly fuzz run api_produce fuzz/artifacts/api_produce/crash-abc123...
```

The artifact is also a regular file — `xxd` it to inspect the bytes, or feed it
to a debugger via `cargo +nightly fuzz run --dev <target> <artifact>`.

To shrink a crashing input to its minimal form before filing:

```bash
cargo +nightly fuzz tmin api_produce fuzz/artifacts/api_produce/crash-abc123...
```

## Coverage and Corpus Management

```bash
# Generate an HTML coverage report for a target's current corpus.
cargo +nightly fuzz coverage api_produce
# Output lands in `coverage/api_produce/`.

# Merge / minimise the on-disk corpus, dropping inputs that don't add coverage.
cargo +nightly fuzz cmin api_produce
```

Corpora live under `fuzz/corpus/<target>/` and are checked into git so CI
runs start with non-empty seeds.

## Targets

Targets are grouped by what they cover. Anything in the first three groups is
on the broker's unauthenticated network path and should be run for at least
an hour after any parser change.

### Per-API request parsers (`api_*`)

One target per Kafka API key. Each derives a `(version, body)` pair, clamps
`version` to the API's advertised `SUPPORTED_VERSIONS` range, and feeds the
body to its parser. Per-target coverage maps catch shape-specific bugs that a
shared `request_parse` target would smear across APIs.

| Target | API |
|---|---|
| `api_produce`, `api_fetch` | Produce / Fetch |
| `api_metadata`, `api_list_offsets` | Metadata, ListOffsets |
| `api_offset_commit`, `api_offset_fetch` | Offset commit / fetch |
| `api_find_coordinator`, `api_join_group`, `api_sync_group` | Group coordination |
| `api_heartbeat`, `api_leave_group` | Group liveness |
| `api_describe_groups`, `api_list_groups`, `api_delete_groups` | Group admin |
| `api_create_topics`, `api_delete_topics` | Topic admin |
| `api_init_producer_id` | Idempotent producer init |
| `api_versions` | API version negotiation |
| `api_sasl_handshake`, `api_sasl_authenticate` | SASL framing |

### Connection frame reader (`frame_reader`)

Drives `read_kafka_frame_for_fuzz` over a chunked mock `AsyncRead`. Asserts no
panic, no over-allocation past the configured cap, and that the returned body
length matches the wire-declared `size`.

### RecordBatch header (`record_batch`)

Exercises CRC validation, record-count cross-check, producer-info extraction,
and timestamp parsing on adversarial v2 batch headers. Property: a bit flip
inside the CRC-covered range must change the validation outcome.

### SCRAM-SHA-256 (`scram_client_first`, `scram_client_final`)

Drives `handle_client_first` / `handle_client_final` with arbitrary bytes and
both the user-found and anti-enumeration code paths. SCRAM is the auth
boundary — a panic here is a pre-auth DoS.

### Postcard decoders (`postcard_*`)

| Target | Decoder |
|---|---|
| `postcard_coordination_command` | Cluster control-plane requests |
| `postcard_coordination_response` | Cluster control-plane responses |
| `postcard_raft_rpc_message` | Raft RPC requests |
| `postcard_raft_rpc_response` | Raft RPC responses |

Raft frames are HMAC-authenticated on the wire, but postcard decode runs
*before* HMAC verification on the response path. Each target asserts a
canonical-form roundtrip: a successful decode must re-encode to bytes that
decode back to the same canonical form.

### Compact encoding (`flexible_encoding`)

Compact / tagged-fields edge cases shared by every flexible-version Kafka API.

### Response encoder (`response_encode`)

Encoder safety on adversarially constructed response data — guards against
panics where the broker writes a structurally invalid response back to a peer.

### Identifier validators (`validators`)

`validate_topic_name` / `validate_group_id`. These names flow into SlateDB
filesystem paths, so any string the validator accepts MUST satisfy the
documented charset and length rules — anything else is a path-traversal vector.

### Config parsing (`config_parse`)

`ClusterProfile::from_str`, `ObjectStoreType` selection, and a stub driver for
`ClusterConfig::from_env`. Acceptance: no panics on any input.

### Misc

| Target | Covers |
|---|---|
| `request_parse` | Shared request parser entry point |
| `request_parse_structured` | Structured-input variant of the above |
| `parser_primitives` | Lower-level primitive decoders (varint, compact strings) |
| `record_batch_header` | Header-only subset of `record_batch` (kept for older corpora) |

## CI

CI runs each network-reachable target for a short fixed time on every PR
using the `-max_total_time=N` flag, seeded from the checked-in corpus.
Long-running fuzz campaigns are run out-of-band on the maintainer's machine
and any new findings are minimised, added to the corpus, and committed.
