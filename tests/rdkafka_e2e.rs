//! End-to-end smoke tests against a real `rdkafka` (librdkafka) client.
//!
//! Wire-protocol tests and byte-for-byte differential tests exercise the
//! parser/encoder against a reference encoder, but they do not exercise the
//! state machine of a real Kafka client. Real librdkafka producers and
//! consumers carry behavior the broker cannot afford to ignore: sticky
//! partitioner, idempotent producer sequence ranges, fetch session epochs,
//! consumer group v2 embedded payloads, request-timeout retries.
//!
//! These tests run in CI on the `rdkafka-e2e` job (requires
//! `librdkafka-dev` / system librdkafka via pkg-config). Locally they need
//! the same: `brew install librdkafka` on macOS, `apt install librdkafka-dev`
//! on Debian/Ubuntu.
//!
//! Each test:
//!   1. Spins up a `SlateDBClusterHandler` on an `InMemory` object store
//!      under a tempdir, bound to a random TCP port.
//!   2. Connects an `rdkafka` client and exercises one Kafka API surface.
//!   3. Asserts the observable client-side behavior, not just the wire bytes.
//!
//! The starter scenarios cover the smallest set of "things a real client
//! does that bytes-only tests miss": produce round-trip, consumer group,
//! and admin create+delete. Add scenarios as gaps are discovered.

use std::sync::Arc;
use std::time::Duration;

use kafkaesque::cluster::{ClusterConfig, ObjectStoreType, SlateDBClusterHandler};
use kafkaesque::server::KafkaServer;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use tempfile::TempDir;

mod common;
use common::{enable_single_node_bootstrap, next_port};

/// Spin up an in-memory single-broker server on a fixed ephemeral port.
///
/// The Kafka listen port, `ClusterConfig::port`, and the advertised host/port
/// must agree — librdkafka reconnects to whatever Metadata returns, so binding
/// `:0` while advertising the default `9092` makes produce hang until
/// `message.timeout.ms`.
/// Returns `(addr, server, _tempdir)`. The tempdir is held for the test's
/// lifetime; dropping it cleans up the SlateDB tree.
async fn start_in_memory_broker()
-> Option<(String, Arc<KafkaServer<SlateDBClusterHandler>>, TempDir)> {
    enable_single_node_bootstrap();

    let tempdir = TempDir::new().expect("tempdir");
    let data_path = tempdir.path().to_string_lossy().to_string();
    let kafka_port = next_port();
    let raft_port = next_port();

    let config = ClusterConfig {
        broker_id: 0,
        host: "127.0.0.1".to_string(),
        advertised_host: "127.0.0.1".to_string(),
        port: kafka_port as i32,
        raft_listen_addr: format!("127.0.0.1:{raft_port}"),
        auto_create_topics: true,
        // Single-partition topics keep the produce/consume path off the
        // multi-partition ownership race that AutoMQ-style leases introduce
        // under auto-create.
        default_num_partitions: 1,
        object_store: ObjectStoreType::Local {
            path: data_path.clone(),
        },
        object_store_path: data_path,
        ..Default::default()
    };

    let handler = SlateDBClusterHandler::new(config)
        .await
        .expect("handler init");

    let bind = format!("127.0.0.1:{kafka_port}");
    let server = match KafkaServer::new(&bind, handler).await {
        Ok(s) => s,
        Err(e) => {
            // Skip in sandboxed environments without TCP bind permission.
            if e.to_string().to_lowercase().contains("permission denied") {
                return None;
            }
            panic!("KafkaServer::new failed: {e}");
        }
    };

    let addr = server.local_addr().expect("addr").to_string();
    let server = Arc::new(server);
    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run().await;
    });

    // Wait until the listener accepts connections.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Some((addr, server, tempdir));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("broker did not accept connections within 5s")
}

/// Round-trip: produce one record, consume it, and verify the value.
///
/// Catches the most basic Kafka-client incompatibilities (metadata response
/// shape, produce ack handling, fetch session lifecycle) — failures here
/// mean librdkafka cannot talk to the broker at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdkafka_produce_consume_round_trip() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let topic = "rdkafka-roundtrip";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");

    let payload = b"hello-from-rdkafka";
    let key = b"k1";
    producer
        .send(
            FutureRecord::to(topic).payload(payload).key(key),
            Duration::from_secs(5),
        )
        .await
        .expect("produce");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .set("group.id", "rdkafka-roundtrip-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");

    consumer.subscribe(&[topic]).expect("subscribe");

    let recv = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .expect("recv timeout")
        .expect("recv error");

    assert_eq!(recv.payload().expect("payload"), payload);
    assert_eq!(recv.key().expect("key"), key);
    assert_eq!(recv.topic(), topic);
}

/// Idempotent producer: produce three records with `enable.idempotence=true`
/// and verify each lands at a unique offset. This exercises the
/// `InitProducerId` handler, the producer-id allocator, and the per-batch
/// sequence number tracking — none of which are reachable via byte-diff
/// tests since the encoder doesn't write a producer_id by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdkafka_idempotent_producer_offsets_advance() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let topic = "rdkafka-idem";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .create()
        .expect("producer");

    let mut offsets = Vec::new();
    for i in 0..3 {
        let payload = format!("idem-{i}");
        let delivery = producer
            .send(
                FutureRecord::<(), _>::to(topic).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
        offsets.push(delivery.offset);
    }

    // Offsets must be strictly increasing within a single partition.
    for window in offsets.windows(2) {
        assert!(
            window[1] > window[0],
            "idempotent producer offsets must advance: {offsets:?}"
        );
    }
}

/// Consumer group offset commit: produce, consume with a group, commit,
/// and verify the committed offset via `committed()`. Exercises
/// FindCoordinator + Join/Sync/Heartbeat + OffsetCommit/Fetch through
/// librdkafka's negotiated API versions.
///
/// Deliberately avoids a second same-group consumer: librdkafka requires
/// all borrowed messages to be destroyed before the consumer handle, and
/// overlapping two group members while a `BorrowedMessage` is live has
/// SIGSEGV'd in CI (`rd_kafka_message` / rebalance). Verify the commit
/// with the same consumer after dropping the message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdkafka_consumer_group_commit_round_trip() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let topic = "rdkafka-group-commit";
    let group = "rdkafka-group-commit-g";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");

    let payload = b"group-commit-payload";
    producer
        .send(
            FutureRecord::to(topic).payload(payload).key(b"gk"),
            Duration::from_secs(5),
        )
        .await
        .expect("produce");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("consumer");

    consumer.subscribe(&[topic]).expect("subscribe");

    let recv = tokio::time::timeout(Duration::from_secs(15), consumer.recv())
        .await
        .expect("recv timeout")
        .expect("recv error");
    assert_eq!(recv.payload().expect("payload"), payload);

    let partition = recv.partition();
    // Kafka commits the *next* offset to fetch.
    let committed_next = recv.offset() + 1;

    consumer
        .commit_message(&recv, rdkafka::consumer::CommitMode::Sync)
        .expect("commit_message");
    // librdkafka: destroy BorrowedMessage before further consumer ops /
    // close. Holding it across rebalance or drop is undefined (SIGSEGV).
    drop(recv);

    let committed = consumer
        .committed(Duration::from_secs(5))
        .expect("committed()");
    let entry = committed
        .find_partition(topic, partition)
        .expect("committed partition entry");
    assert_eq!(
        entry.offset(),
        Offset::Offset(committed_next),
        "OffsetCommit must stick; got {:?}",
        entry.offset()
    );
}

/// AdminClient: create a topic, list metadata, and delete it. Catches
/// regressions in `CreateTopics` / `DeleteTopics` request shape and the
/// metadata-refresh flow that `auto.create.topics.enable=true` would
/// otherwise hide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdkafka_admin_create_delete_topic() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let topic = "rdkafka-admin";
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &addr)
        .create()
        .expect("admin");

    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new();
    let create_result = admin
        .create_topics(&[new_topic], &opts)
        .await
        .expect("create_topics");
    for r in create_result {
        r.expect("topic create result");
    }

    let delete_result = admin
        .delete_topics(&[topic], &opts)
        .await
        .expect("delete_topics");
    for r in delete_result {
        r.expect("topic delete result");
    }
}
