# Kafka wire-protocol messages to implement

## Tier 1 — new APIs (modern clients expect these on every connection)

- [x] **OffsetForLeaderEpoch** — API key `23`
  - Versions implemented: `0..=3` (classic). v4 (flexible) is deferred.
  - Why: librdkafka + Java client call this on every consumer rebalance for log-truncation fencing (KIP-320). Without it, clients log warnings and skip the safety check.
  - Notes: handler reads `(owner, leader_epoch)` from the replicated partition state, returns `(broker_epoch, log_end_offset)` for partitions this broker owns. Client epoch mismatches surface as `FencedLeaderEpoch` (client older) or `UnknownLeaderEpoch` (client newer). Non-owned partitions return `NotLeaderForPartition`.

- [x] **CreatePartitions** — API key `37`
  - Versions implemented: `0..=1` (classic). v2+ (flexible) is deferred.
  - Why: `kafka-topics.sh --alter --partitions N` and AdminClient `createPartitions()` use only this; no fallback to CreateTopics. Today scaling an existing topic from a Kafka client is impossible.
  - Notes: new `TopicRegistryCommand::GrowPartitions` enforces strict-greater-than semantics in the state machine. Handler best-effort acquires partitions that hash to this broker locally; the rest fall to the ownership loop. Same per-topic and cluster-wide caps as CreateTopics.

- [x] **IncrementalAlterConfigs** — API key `44`
  - Versions implemented: `0` (classic). v1 (flexible) is deferred.
  - Why: AdminClient default since Kafka 2.3 (KIP-339); `kafka-configs.sh --alter --add-config` ships Set/Append/Subtract/Delete ops here, not via legacy AlterConfigs.
  - Notes: ops applied in request order onto the existing config map, then validated and persisted via the same path AlterConfigs uses. Append/Subtract treat values as comma-separated lists (idempotent, order-preserving). Set with NULL is a Delete (matches Kafka).

## Tier 2 — version bumps on APIs we already have

- [x] **Fetch** — bumped from v4 only to **v4..=v11**
  - Why: v7 introduces incremental fetch sessions (KIP-227) — re-shipping full partition state on every fetch is a real throughput tax for consumers with many partitions. v11 adds rack-aware replica selection.
  - Skippable intermediates: v5 (log_start_offset), v6 (no wire change).
  - Notes: parser handles `session_id` / `session_epoch` / `forgotten_topics_data` (v7+), per-partition `current_leader_epoch` (v9+), `log_start_offset` (v5+), and top-level `rack_id` (v11+). Response encoder gates `last_stable_offset` + `aborted_transactions` (v4+), `log_start_offset` (v5+), top-level `error_code` + `session_id` (v7+), and per-partition `preferred_read_replica` (v11+). v12+ (flexible) is out of scope.

- [x] **Metadata** — bumped from v0..=v1 to **v0..=v9**
  - Why: v9 adds `cluster_id` (clients use this for client-id allocation and idempotency) and topic IDs (KIP-516). Some newer admin tools degrade gracefully, others do not.
  - Notes: v9+ uses flexible encoding (compact arrays / strings, trailing tagged fields). Response carries `cluster_id` (v2+), `throttle_time_ms` (v3+), per-partition `offline_replicas` (v5+), `leader_epoch` (v7+), and `cluster_authorized_operations` + per-topic `topic_authorized_operations` (v8..=v10). v10+ adds topic `topic_id` UUIDs and is intentionally deferred.

- [x] **Produce** — bumped from v3 only to **v3..=v9**
  - Why: v9 is the modern flexible version; carries `current_leader` (leader-epoch) on the response so clients can refresh metadata without a separate round-trip after a leader change.
  - Notes: v9+ flexible request body (`COMPACT_NULLABLE_STRING` transactional_id, compact arrays, `COMPACT_NULLABLE_BYTES` records, trailing tagged fields). Response gains per-partition `log_start_offset` (v5+) and `record_errors` + `error_message` (v8+). v10+ (`current_leader`) is deferred.

## Out of scope (intentional)

Per `README.md` — transactions, log compaction, and idempotent-producer dedup beyond per-session `producer_id` are not goals. Inter-broker APIs (LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown), log-dir APIs (Describe/AlterLogDirs), partition reassignment (Alter/ListPartitionReassignments), and ElectLeaders have no place in the object-store + Raft architecture.
