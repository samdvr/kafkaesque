# Fuzzy Testing Expansion Plan

A prioritized roadmap for expanding fuzz and property-based testing to lift true
confidence on wire-protocol compatibility, codec robustness, distributed
state-machine determinism, and security boundaries.

## Audit summary (current state, 2026-06-11)

**libFuzzer / cargo-fuzz targets** (`fuzz/fuzz_targets/`):

| Target                     | Surface covered                                                                               |
| -------------------------- | --------------------------------------------------------------------------------------------- |
| `request_parse`            | Raw bytes → `Request::parse` (random api_key/version)                                         |
| `request_parse_structured` | First 4 bytes → (api_key, api_version); rest is body                                          |
| `parser_primitives`        | `parse_string`, `parse_nullable_string`, `parse_unsigned_varint`, `parse_array(parse_string)` |
| `record_batch_header`      | `parse_record_count_checked` on a v2 RecordBatch header                                       |

**Property tests** (`proptest`):

| Module                | Coverage                                                  |
| --------------------- | --------------------------------------------------------- |
| `src/cluster/keys.rs` | record-key, leader-epoch, producer-state-value roundtrips |

**Significant gaps**:

- 20 per-API request parsers exist; structured fuzzing only nudges them via
  random api_key bytes (low yield).
- Zero fuzz coverage on response encoders, the connection frame reader,
  CRC/RecordBatch validation, `parse_producer_info`, `parse_batch_max_timestamp`,
  `parse_compact_nullable_string`, `skip_tagged_fields`, `parse_kafka_string{,_opt}`.
- Zero fuzz coverage on the **non-wire** attack surfaces that take untrusted
  bytes: SCRAM/SASL message parsing, Raft RPC postcard decode, on-disk Raft
  storage decode, ACL evaluator, identifier validators, env/config parsers.
- No CI job runs the fuzzers — they only run when a developer thinks to.
- No corpus seeding, no dictionaries, no sanitizers beyond the default ASan.
- No differential fuzzing against the reference Kafka client (rdkafka).
- No structured-fuzzing via `arbitrary` derives (the one "structured" target
  uses an ad-hoc 4-byte split, not a real `Arbitrary` impl).

## libFuzzer infrastructure (applies to every target below)

The project already uses `cargo-fuzz` + `libfuzzer-sys = "0.4"`. The plan keeps
that — it is the most practical fuzzing harness for Rust today. To get more
signal per CPU-hour:

- **`arbitrary` derives.** Add `arbitrary = { version = "1", features = ["derive"] }`
  to `fuzz/Cargo.toml`. Each new structured target should accept
  `Arbitrary`-derived inputs (not raw `&[u8]`) so libFuzzer mutates fields
  rather than bytes — much higher coverage growth on per-API parsers, ACL
  bindings, etc.
- **Dictionaries.** Ship `fuzz/dictionaries/kafka.dict` with API keys, version
  numbers, magic bytes (0x02 for v2 batches), the SCRAM tokens (`n=`, `r=`,
  `p=`, `,,`, `n,,`), Kafka error codes, postcard variant tags. libFuzzer
  picks these up via `cargo fuzz run <target> -- -dict=path/to/file`.
- **Seed corpora.** Capture real client frames once with `tcpdump`/kcat and
  save under `fuzz/corpus/<target>/`. A 5-minute librdkafka capture covers
  the entire advertised version matrix; it raises baseline coverage by an
  order of magnitude.
- **Sanitizer matrix.** Run each target under ASan (default), UBSan, and
  (where relevant) the experimental MSan — different sanitizers find different
  classes (UB on int overflow, uninit reads in unsafe deps).
- **Length cap.** Every target should `if data.len() > MAX { return; }` near
  the protocol's real frame ceiling (100 MB for Kafka requests; smaller for
  per-API targets) so libFuzzer doesn't waste cycles on payloads that can
  never appear on the wire.
- **Assertion-style invariants.** Where a roundtrip property holds (parse →
  encode → parse), `assert_eq!` it. libFuzzer treats assertion failures the
  same as crashes, turning fuzzing into a property-test fleet.
- **CI integration.** Add a `fuzz-smoke` job (5 minutes/target on PRs) and a
  nightly `fuzz-extended` job (1 hour/target). Persist the corpus as a cache
  artifact across runs so coverage compounds.
- **OSS-Fuzz integration (longer-term).** Once targets stabilize, contribute
  them to OSS-Fuzz for free continuous fuzzing on Google's fleet — the
  highest-leverage move for a security-sensitive parser library.

---

## Tasks (priority-ordered)

> **Status update (2026-06-11).** All P0 and P1 items below are
> implemented. The fuzz crate now ships 30 binaries (4 pre-existing + 20
> per-API + 1 frame reader + 1 RecordBatch + 2 SCRAM + 4 postcard + 1
> flexible encoding + 1 response encoder); `cargo check --manifest-path
fuzz/Cargo.toml --bins` is green. Two new property-test files cover
> hand-rolled roundtrip (`tests/wire_roundtrip_props.rs`, 18 tests) and
> differential against the `kafka-protocol` crate
> (`tests/wire_differential.rs`, 10 tests); both pass. Running fuzz
> targets requires `cargo install cargo-fuzz` and `cargo +nightly fuzz
run <target>`; the P4 CI hookup turns this into continuous coverage.
>
> Side effects of P0 + P1 implementation:
>
> - `parser`, `cluster::keys`, and `encode` modules became `pub mod`
>   (they expose pure wire-format utilities and were already de-facto
>   public via the existing broken `parser_primitives` target).
> - `server::read_kafka_frame_for_fuzz` is a thin public wrapper around
>   the internal frame reader so fuzzers don't have to reimplement the
>   length-prefix logic.
> - `fuzz/Cargo.toml` enables the `sasl` feature on the `kafkaesque`
>   path-dep so SCRAM is reachable; the feature has no extra deps.
> - `Cargo.toml` adds `kafka-protocol = "0.17"` as a dev-dependency for
>   the differential tests. Older 0.x releases (≤0.13) fail to compile
>   against `bytes 1.11+` due to a `try_get_u8` ambiguity.
> - The pre-existing `parser_primitives` target was broken (referenced a
>   private module + missing `bytes`/`nombytes` deps). It now compiles
>   and covers the full set of parser primitives instead of only four.

### P0 — High-impact, security-critical parsers

These take adversarial bytes from the network and feed them into parsers/decoders
where a panic, OOM, or accept-of-invalid bug becomes a remote DoS or worse.

#### P0-1. Per-API structured request fuzzers (one target per API key) ✅ implemented

**Why now.** The single `request_parse_structured` target is shared across 20
API parsers; libFuzzer's coverage signal on any one parser is diluted. Each
parser has version-specific layouts, and Kafka's flexible-encoding rules
(KIP-482, KIP-511) make framing subtle — exactly the territory where parsers
silently misalign.

**What to build.** One target per API in `fuzz/fuzz_targets/`:
`fuzz_produce.rs`, `fuzz_fetch.rs`, `fuzz_metadata.rs`, `fuzz_list_offsets.rs`,
`fuzz_offset_commit.rs`, `fuzz_offset_fetch.rs`, `fuzz_find_coordinator.rs`,
`fuzz_join_group.rs`, `fuzz_heartbeat.rs`, `fuzz_leave_group.rs`,
`fuzz_sync_group.rs`, `fuzz_describe_groups.rs`, `fuzz_list_groups.rs`,
`fuzz_sasl_handshake.rs`, `fuzz_sasl_authenticate.rs`, `fuzz_api_versions.rs`,
`fuzz_create_topics.rs`, `fuzz_delete_topics.rs`, `fuzz_init_producer_id.rs`,
`fuzz_delete_groups.rs`. Each derives `Arbitrary` for an `Input { version: i16,
body: &[u8] }`, clamps `version` to the advertised `(min, max)` from
`SUPPORTED_VERSIONS`, and calls the corresponding `parse_<api>_request` directly.

**Acceptance.** Each target runs cleanly for 1 hour with `-len_control=0` and
no new findings. Corpus committed under `fuzz/corpus/<target>/`.

#### P0-2. Connection frame reader + length-prefix bounds ✅ implemented

**Why now.** `src/server/connection.rs:172` `read_kafka_frame` is the first
thing that touches a TCP byte. An integer overflow or oversized allocation
here is a remote DoS. The 100 MB cap exists in code but isn't fuzz-checked
against arbitrary length prefixes.

**What to build.** `fuzz/fuzz_targets/frame_reader.rs` driving a mock
`AsyncRead` that returns the fuzz input in arbitrary chunks (use `Cursor` or
a hand-rolled chunking reader). Assert: returns `Err` for length prefixes
beyond the cap and for short reads, never panics, never reserves a
`BytesMut` larger than the cap.

**Acceptance.** No allocations >100 MB, no panics, the request body returned
matches the input bytes after the 4-byte length prefix.

#### P0-3. RecordBatch header validation + producer info extraction ✅ implemented

**Why now.** `validate_batch_crc`, `parse_producer_info`, and
`parse_batch_max_timestamp` operate on fully attacker-controlled bytes (the
RecordBatch is sent verbatim by the producer). Existing fuzzing only covers
`parse_record_count_checked`. A panic in any of these takes down a broker.

**What to build.** `fuzz/fuzz_targets/record_batch.rs` that calls all four
header functions on the same input, plus a roundtrip target that builds a
batch via a small builder, mutates a single byte, and asserts CRC mismatch
is detected.

**Acceptance.** No panics across `i64::MIN..=i64::MAX` for any header field;
CRC mismatches are always detected (reference oracle: re-implement
`crc32c` byte-at-a-time inline and assert agreement).

#### P0-4. SCRAM-SHA-256 message parsers ✅ implemented

**Why now.** `src/cluster/scram.rs::handle_client_first` and `handle_client_final`
parse attacker-supplied UTF-8 with hand-rolled `split_once`/`strip_prefix`
logic — the exact pattern where edge cases hide. SASL is the broker's auth
boundary; a panic here lets unauthenticated clients crash the broker.

**What to build.** Two targets that derive `Arbitrary` for a small
`(username, password, client_first_bytes)` triple, run a credentials lookup
that returns derived creds for the username, and feed `client_first_bytes`
to `handle_client_first`. A second target seeds with a valid first-message
state and feeds `client_final_bytes` to `handle_client_final`.

**Acceptance.** No panics; `Failed(_)` is the worst outcome for malformed
input; valid RFC 5802 vectors still authenticate.

#### P0-5. Postcard decode of cluster RPC + on-disk Raft state ✅ implemented

**Why now.** `RaftRpcMessage`, `RaftRpcResponse`, `CoordinationCommand`,
`CoordinationResponse`, `Vote<RaftNodeId>`, `Entry<TypeConfig>`, `LogMeta`,
`SnapshotPointer`, and `SnapshotMetadata` are all `postcard::from_bytes`
decoded from bytes that travel over the cluster's internal network or sit on
disk. HMAC-authentication on the wire reduces reach, but on-disk corruption
and the snapshot recovery path (which falls back to the legacy format) read
unauthenticated bytes.

**What to build.** `fuzz/fuzz_targets/postcard_*.rs` — one per
serde-deserializable type. Each calls `postcard::from_bytes::<T>(data)` and,
on success, re-encodes and asserts byte-for-byte equality (postcard is a
canonical format, so non-canonical encodings should fail to round-trip).

**Acceptance.** No panics; successful decodes round-trip exactly.

---

### P1 — Wire-format correctness (compatibility with real Kafka clients)

#### P1-1. Differential fuzzing: kafkaesque parser vs. rdkafka encoder ✅ implemented (against `kafka-protocol`)

**Why now.** This is the single highest-confidence wire-compatibility check
the project can have. `tests/integration_tests.rs` already pulls in
`rdkafka`. By using rdkafka to _construct_ random-but-well-formed requests
and feeding them through `Request::parse`, we verify shape compatibility
across the entire matrix. Conversely, we encode our responses and feed them
through a kafka-protocol decoder (e.g. `kafka-protocol` crate) to check the
opposite direction.

**What to build.** A non-libfuzzer harness in `tests/wire_differential.rs`
under `proptest!`: generate random structured request data, encode via
rdkafka's protocol writer (or a vendored copy), parse back via
`Request::parse`, assert structural equality. Mirror for responses.

**Acceptance.** 10k iterations green per (api_key, version) advertised
in `SUPPORTED_VERSIONS`.

#### P1-2. Roundtrip property tests for every parsed type ✅ implemented (representative subset)

**Why now.** Parser/encoder pairs that don't round-trip are silent bugs:
clients stop seeing fields they sent, brokers respond with subtly different
shapes. A failure here is exactly the class of issue that makes "looks like
Kafka" implementations subtly incompatible.

**What to build.** In each `src/server/request/*.rs` and
`src/server/response/*.rs`, add a `proptest!` block:

1. Derive `Arbitrary` (manually if needed, since several types are
   public crate types) for the request/response data type.
2. For each version in the advertised range: encode → parse → assert eq.

**Acceptance.** All 20 APIs × every advertised version pass 10k cases.

#### P1-3. Compact-encoding + tagged-fields edge cases ✅ implemented

**Why now.** `parse_compact_nullable_string` and `skip_tagged_fields` are
covered by a single fuzz target only via `parse_array(parse_string)` (which
uses non-compact encoding!). KIP-482 flexible encoding has overflow corners:
varint shift > 28 bits, varint MSB chains, tagged-field counts at
`MAX_PROTOCOL_ARRAY_SIZE`.

**What to build.** `fuzz/fuzz_targets/flexible_encoding.rs` calling
`parse_compact_nullable_string`, `skip_tagged_fields`, and
`parse_unsigned_varint` chained with attacker-chosen counts.

**Acceptance.** No panics; varint shift overflow returns Err
(currently `>28` triggers `ErrorKind::TooLarge`; verify it always does).

#### P1-4. Encoder safety on adversarial response data ✅ implemented

**Why now.** The encoder side is currently untested against pathological
inputs (e.g., a `String` of length `i16::MAX + 1` from a handler bug). The
existing `encode_string_len` fails cleanly, but other paths may panic via
`BytesMut::reserve` overflow (mitigated by `bytes 1.11.1+` per Cargo.toml,
but the integration is unverified).

**What to build.** `fuzz/fuzz_targets/response_encode.rs` that derives
`Arbitrary` for `*ResponseData` types (or constructs them via builder) with
boundary-sized strings/arrays and calls `encode_versioned` for every
advertised version.

**Acceptance.** Errors propagate as `Error::Config`, no panics.

---

### P2 — State machine and authorization correctness

> **Status update (2026-06-11).** All P2 items below are implemented as
> three new test files in `tests/`:
> - `tests/state_machine_determinism_props.rs` (P2-1, 2 properties)
> - `tests/acl_evaluator_props.rs` (P2-2, 3 properties)
> - `tests/consistent_hash_props.rs` (P2-3, 3 properties)
>
> All eight properties pass under `cargo test`. P2-1 deviates from the
> literal "byte-equal serialized state" wording: the state machine's
> domains use `HashMap`/`HashSet` with random per-instance hash seeds, so
> postcard byte-equality across two fresh state machines is never
> guaranteed even with deterministic apply. Instead the test compares a
> sorted, content-based fingerprint — same property, achievable bound.
> The reorder variant compares the planned invariants (broker count,
> topic count) over distinct-broker-id register and distinct-topic create
> commands.

#### P2-1. Raft state-machine determinism (proptest) ✅ implemented

**Why now.** `CoordinationStateMachine::apply` is the single most important
piece of code in the cluster: any non-determinism leads to replica
divergence. The current `state_machine_tests.rs` covers point cases but
doesn't fuzz arbitrary command sequences.

**What to build.** A proptest harness that generates `Vec<CoordinationCommand>`
(via `Arbitrary` derives), applies the same sequence to two fresh state
machines, and asserts byte-equal serialized state. Add a second variant that
shuffles independent commands and asserts equality of _invariants_
(broker count, owned-partition count) since strict equality wouldn't hold.

**Acceptance.** 10k random sequences × two state machines = identical
postcard-serialized state.

#### P2-2. ACL evaluator decision properties ✅ implemented

**Why now.** `AclDomainState::is_authorized` mixes wildcard, prefix, and
literal patterns with deny-wins precedence. A misordered check is a silent
auth bypass.

**What to build.** A proptest in `src/cluster/raft/domains/acl.rs` with
`Arbitrary` impls for `AclBinding` and `AclFilter`. Properties:

- Adding a Deny binding never _increases_ the set of allowed (principal,
  host, op, resource) tuples.
- Adding an Allow binding never _decreases_ it.
- `DescribeAcls` filter results are exactly the bindings matching the filter.

**Acceptance.** No counterexamples in 10k cases.

#### P2-3. Consistent-hash assignment stability ✅ implemented

**Why now.** Partition reassignment moves data; a non-stable hash (one that
moves more than `~1/n` partitions on broker change) is a reliability bug.

**What to build.** Proptest in `src/cluster/coordinator/mod.rs`:

- For random `(brokers_before, brokers_after)` differing by one broker,
  the share of `(topic, partition)` keys that change owner is bounded by
  `2 * 1/min(before, after)` (consistent-hashing's theoretical bound).
- Assignment is deterministic across calls.

**Acceptance.** 10k cases, no violations.

---

### P3 — Edge-case identifier and config inputs

> **Status update (2026-06-11).** All P3 items below are implemented.
> Two new fuzz targets ship in `fuzz/fuzz_targets/`
> (`validators.rs`, `config_parse.rs`) and four new proptests extend the
> existing producer-state coverage in `src/cluster/keys.rs`.
> 30-second smoke runs of both new fuzz targets are green
> (validators: 200k runs, 145 cov edges; config_parse: 100k runs,
> 1706 cov edges).
>
> The validators target found and fixed one real bug on its first run:
> `cluster::validation::truncate_for_display` byte-sliced a `&str`
> without snapping to a char boundary, panicking when the leading-hyphen
> error path stringified an identifier whose multi-byte UTF-8 character
> straddled byte index 50. Fix snaps the index down to the nearest char
> boundary.

#### P3-1. Identifier validators (`validate_topic_name`, `validate_group_id`) ✅ implemented

**Why now.** Topic names and group IDs flow into filesystem paths under
SlateDB. A bypassed check is a path-traversal vector. Validators currently
have unit tests for fixed examples but no exhaustive fuzzing.

**What to build.** `fuzz/fuzz_targets/validators.rs` calling each validator
on arbitrary UTF-8. Property: any `Ok(_)` string contains only the documented
character set, is ≤ the documented length cap, and is not `.`/`..`.

**Acceptance.** No counterexamples; no panics.

#### P3-2. Config parsing (`from_env`, `ObjectStoreType::from_str`,

`ClusterProfile::from_str`) ✅ implemented

**Why now.** Config is parsed from environment variables — usually
operator-controlled, but environment manipulation is a real attack vector
in containerized deployments. Parser panics on env input would crash the
broker on startup.

**What to build.** `fuzz/fuzz_targets/config_parse.rs` that calls each
`from_str` and a stub `from_env` driver with arbitrary `(key, value)`
pairs.

**Acceptance.** No panics; all errors are `Error::Config`.

#### P3-3. Producer-state value decode + key codecs (extend existing proptests) ✅ implemented

**Why now.** `keys.rs` already has solid proptests, but they don't cover the
mixed-format decode paths (legacy 6-byte vs current 18-byte values). A bad
decode here is silent data loss for idempotent producers.

**What to build.** Extend `keys.rs` proptests:

- For arbitrary byte vectors of length `[0, 32]`: `decode_producer_state_value`
  never panics.
- For length 6: returns `Some` with `last_first_sequence == -1, last_base_offset == -1`.
- For length ≥18: prefix matches the parsed legacy decode.

**Acceptance.** 10k cases, no violations.

---

### P4 — Continuous fuzzing infrastructure

#### P4-1. CI smoke fuzz job

**Why now.** Targets that exist but never run drift to brittle. A 5-minute
PR job catches regressions in newly added parser branches.

**What to build.** Add a `fuzz-smoke` job to `.github/workflows/ci.yml`:

```yaml
fuzz-smoke:
  name: Fuzz smoke (5 min/target)
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@nightly
    - run: cargo install cargo-fuzz --locked
    - run: |
        for t in $(ls fuzz/fuzz_targets/*.rs | xargs -n1 basename -s .rs); do
          cargo fuzz run $t -- -max_total_time=300
        done
```

Cache the fuzz corpus across runs (`actions/cache` keyed on workflow file
hash) so coverage compounds.

---

### P5 — Stretch goals

#### P5-2. Differential fuzzing against Apache Kafka itself

Spawn a JVM Kafka broker in a sidecar; for every request the libFuzzer
target generates, send it to both kafkaesque and Kafka, compare response
bytes. The highest-confidence test of wire compatibility there is.

#### P5-3. Stateful fuzzing of the broker via `proptest-state-machine`

Drive the full server (via a `Handler` impl over an in-memory backend) with
a generated sequence of `Request`s and assert linearizability invariants
(per `tests/linearizability_tests.rs`) hold under arbitrary client
interleavings.

---

## Suggested execution order

1. **Week 1.** P0-1 (per-API targets), P0-2 (frame reader), P4-1 (CI smoke).
   This gets us continuous coverage on the largest attack surface and
   guarantees regressions are caught on every PR.
2. **Week 2.** P0-3 (RecordBatch), P0-4 (SCRAM), P1-2 (roundtrip props),
   P4-3 (seed corpora). Closes the worst remaining wire-side gaps and
   gives every target a seeded corpus.
3. **Week 3.** P0-5 (postcard decoders), P1-1 (rdkafka differential),
   P4-2 (nightly extended). Locks in cross-implementation compatibility
   and starts producing deep findings.
4. **Week 4.** P2 series (state-machine + ACL), P1-3, P1-4, P3 series,
   P4-4. Rounds out coverage; everything below is value-add.
5. **Backlog.** P5-1..3.
