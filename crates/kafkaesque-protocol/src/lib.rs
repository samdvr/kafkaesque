//! # Kafkaesque protocol
//!
//! Runtime-independent Kafka wire-protocol layer extracted from the
//! `kafkaesque` umbrella crate. This crate is intentionally small and
//! has NO dependency on tokio, slatedb, or openraft so it can be
//! consumed by clients/servers independently of the broker
//! implementation.
//!
//! Re-exports preserved at `kafkaesque::{constants, encode, error,
//! parser, protocol, types}` mean existing code paths continue to
//! resolve through the umbrella crate.

#![forbid(unsafe_code)]

pub mod bytes_chain;
pub mod constants;
pub mod encode;
pub mod error;
pub mod parser;
pub mod protocol;
pub mod types;
