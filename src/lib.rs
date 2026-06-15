//! # Kafkaesque
//! Rust-native Kafka/Redpanda protocol server implementation.
//!
//! This crate provides a Rust implementation of the Apache Kafka wire protocol,
//! allowing you to build Kafka-compatible servers. This is pure Rust all the way
//! down; meaning memory safety, safe concurrency, low resource usage, and speed.
//!
//! [Documentation](https://docs.rs/kafkaesque/latest/kafkaesque/)
//!
//! # Goals
//! - Easy to understand code
//! - Leverage best in class libraries such as [Tokio](https://tokio.rs/), [Nom](https://docs.rs/nom/latest/nom/)
//! - Provide a pure Rust implementation of the Kafka protocol
//! - Be a building block for Kafka-compatible services
//!
//! # Workspace layout
//!
//! Kafkaesque ships as a Cargo workspace:
//!
//! - [`kafkaesque_protocol`] — runtime-independent wire-protocol layer
//!   (parser, encoder, types, constants, wire-error). No tokio, no
//!   storage backends. Re-exported here under the same module names so
//!   existing `use kafkaesque::{parser, encode, types, ...}` paths
//!   keep working.
//! - `kafkaesque-bin` — the production broker binary.
//! - This crate (`kafkaesque`) — cluster, server, runtime,
//!   telemetry, plus the `MockCoordinator` test helper (gated behind
//!   the `test-utilities` feature). The umbrella that ties everything
//!   together.
//!
//! ## Getting started
//! Install `kafkaesque` to your rust project with `cargo add kafkaesque` or include the following snippet in your `Cargo.toml` dependencies:
//! ```toml
//! kafkaesque = "0.1"
//! ```
//!
//! ### Building a Kafka-compatible server
//! The [`KafkaServer`](server::KafkaServer) provides a TCP server that speaks the Kafka wire protocol.
//! To create your own Kafka-compatible service, implement the [`Handler`](server::Handler) trait
//! to define how your server responds to Kafka protocol requests.
//!
//! ```rust,no_run
//! use kafkaesque::prelude::*;
//! use async_trait::async_trait;
//!
//! struct MyHandler;
//!
//! #[async_trait]
//! impl server::Handler for MyHandler {
//!     async fn handle_metadata(
//!         &self,
//!         _ctx: &server::RequestContext,
//!         _request: server::request::MetadataRequestData,
//!     ) -> server::response::MetadataResponseData {
//!         server::response::MetadataResponseData {
//!             brokers: vec![server::response::BrokerData {
//!                 node_id: 0,
//!                 host: "localhost".to_string(),
//!                 port: 9092,
//!                 rack: None,
//!             }],
//!             controller_id: 0,
//!             topics: vec![],
//!         }
//!     }
//!
//!     async fn handle_produce(
//!         &self,
//!         _ctx: &server::RequestContext,
//!         _request: server::request::ProduceRequestData,
//!     ) -> server::response::ProduceResponseData {
//!         server::response::ProduceResponseData {
//!             responses: vec![],
//!             throttle_time_ms: 0,
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
//!     let server = server::KafkaServer::new("127.0.0.1:9092", MyHandler).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! See `examples/server.rs` for a complete in-memory Kafka implementation.
//!
//! For a production-ready, horizontally scalable broker using SlateDB and
//! Raft coordination, see the `kafkaesque-bin` crate
//! (`crates/kafkaesque-bin/`).
//!
//! ## Resources
//! - [Kafka Protocol Spec](https://kafka.apache.org/protocol.html)
//! - [Confluence Docs](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

#![forbid(unsafe_code)]

// Re-export the wire-protocol layer at the same module paths it used
// to live at, so `use kafkaesque::{parser, encode, types, ...}`
// continues to resolve. The actual code lives in
// `kafkaesque-protocol`.
pub use kafkaesque_protocol::{bytes_chain, constants, encode, error, parser, types};

mod protocol_async;

/// Re-export of [`kafkaesque_protocol::protocol`] augmented with the
/// tokio-flavored [`validate_batch_crc_async`] helper used on the
/// produce hot path. Callers see the same `crate::protocol::*` they
/// did before the workspace split.
pub mod protocol {
    pub use crate::protocol_async::validate_batch_crc_async;
    pub use kafkaesque_protocol::protocol::*;
}

pub mod cluster;
pub mod runtime;
pub mod telemetry;

pub mod server;

pub mod prelude {
    //! Main export of server structures
    //!
    //! # Server
    //!
    //! The server module provides infrastructure for building Kafka-compatible services.
    //! Use [`KafkaServer`](server::KafkaServer) to create a TCP server and implement
    //! the [`Handler`](server::Handler) trait to define your server's behavior.
    //!
    //! ## Example
    //! ```rust,no_run
    //! use kafkaesque::prelude::*;
    //! use async_trait::async_trait;
    //!
    //! struct MyHandler;
    //!
    //! #[async_trait]
    //! impl server::Handler for MyHandler {
    //!     async fn handle_metadata(
    //!         &self,
    //!         _ctx: &server::RequestContext,
    //!         _request: server::request::MetadataRequestData,
    //!     ) -> server::response::MetadataResponseData {
    //!         server::response::MetadataResponseData {
    //!             brokers: vec![],
    //!             controller_id: 0,
    //!             topics: vec![],
    //!         }
    //!     }
    //! }
    //!
    //! #[tokio::main]
    //! async fn main() {
    //!     let server = server::KafkaServer::new("127.0.0.1:9092", MyHandler).await.unwrap();
    //!     server.run().await.unwrap();
    //! }
    //! ```
    pub use crate::error::{Error, KafkaCode, Result};
    pub use crate::protocol::{
        CrcValidationResult, parse_record_count_checked, patch_base_offset, validate_batch_crc,
    };
    pub use crate::types::{
        BrokerId, CorrelationId, GenerationId, Offset, PartitionId, PartitionIndex, ProducerEpoch,
        ProducerId,
    };

    pub use bytes;

    pub mod server {
        //! Kafka-compatible server implementation.
        //!
        //! Use this module to build a server that speaks the Kafka wire protocol.
        //! See [`KafkaServer`] for the main entry point.
        pub use crate::server::*;
    }
}
