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

- [ ] **Fetch** — currently v4 only; bump to at least **v7** (target `4..=11`)
  - Why: v7 introduces incremental fetch sessions (KIP-227) — re-shipping full partition state on every fetch is a real throughput tax for consumers with many partitions. v11 adds rack-aware replica selection.
  - Skippable intermediates: v5 (log_start_offset), v6 (no wire change).

- [ ] **Metadata** — currently v0-1; bump to at least **v9** (target `0..=12`)
  - Why: v9 adds `cluster_id` (clients use this for client-id allocation and idempotency) and topic IDs (KIP-516). Some newer admin tools degrade gracefully, others do not.
  - Notes: v9+ uses flexible encoding.

- [ ] **Produce** — currently v3 only; bump to **v9** (target `3..=9`)
  - Why: v9 is the modern flexible version; carries `current_leader` (leader-epoch) on the response so clients can refresh metadata without a separate round-trip after a leader change.
  - Notes: not load-bearing for correctness today, but completes the set with Fetch/Metadata bumps.

## Out of scope (intentional)

Per `README.md` — transactions, log compaction, and idempotent-producer dedup beyond per-session `producer_id` are not goals. Inter-broker APIs (LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown), log-dir APIs (Describe/AlterLogDirs), partition reassignment (Alter/ListPartitionReassignments), and ElectLeaders have no place in the object-store + Raft architecture.
