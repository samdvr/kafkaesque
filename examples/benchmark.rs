//! Benchmarking tool for Kafkaesque broker performance.
//!
//! Measures produce and fetch throughput and latency.
//!
//! ## Running
//!
//! First, start a Kafkaesque broker:
//! ```bash
//! cargo run --example cluster
//! ```
//!
//! Then run the benchmark in another terminal:
//! ```bash
//! cargo run --example benchmark --release
//! ```
//!
//! With custom parameters:
//! ```bash
//! BROKER=localhost:9092 TOPIC=bench MESSAGE_COUNT=100000 MESSAGE_SIZE=1000 \
//!     cargo run --example benchmark --release
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::Semaphore;

#[derive(Debug)]
struct BenchmarkConfig {
    broker: String,
    topic: String,
    message_count: u64,
    message_size: usize,
    concurrency: usize,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        Self {
            // Use 127.0.0.1 explicitly to avoid IPv6 resolution issues with "localhost"
            // For multi-broker clusters, set BROKER to all brokers: "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"
            broker: std::env::var("BROKER").unwrap_or_else(|_| "127.0.0.1:9092".into()),
            topic: std::env::var("TOPIC").unwrap_or_else(|_| "benchmark-topic".into()),
            message_count: std::env::var("MESSAGE_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
            message_size: std::env::var("MESSAGE_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            concurrency: std::env::var("CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100), // Increased from 10 for higher throughput
        }
    }
}

#[derive(Debug, Default)]
struct LatencyStats {
    samples: Vec<Duration>,
}

impl LatencyStats {
    fn add(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn summary(&mut self) -> String {
        if self.samples.is_empty() {
            return "no samples".into();
        }
        self.samples.sort();
        let len = self.samples.len();
        let p50 = self.samples[len / 2];
        let p99 = self.samples[len * 99 / 100];
        let p999 = self.samples[len * 999 / 1000];
        let max = self.samples[len - 1];
        format!("p50={:?} p99={:?} p99.9={:?} max={:?}", p50, p99, p999, max)
    }
}

async fn run_produce_benchmark(config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Produce Benchmark ===");
    println!(
        "Producing {} messages of {} bytes each to {}",
        config.message_count, config.message_size, config.topic
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", "30000")
        .set("linger.ms", "1") // Reduced from 5ms for higher throughput
        .set("batch.size", "65536")
        .set("compression.type", "none")
        .set("acks", "1") // Wait for leader only (not ISR) - faster acknowledgments
        .set("queue.buffering.max.messages", "100000") // Allow more in-flight
        .set("metadata.max.age.ms", "5000") // Refresh metadata more frequently
        .create()?;

    // Warm-up phase: send one message to each partition to ensure all brokers
    // have acquired their partitions and metadata is propagated
    println!("Warming up (sending to each partition)...");
    let warmup_payload = vec![b'w'; 10];
    for partition in 0..10 {
        let key = format!("warmup-{}", partition);
        let record = FutureRecord::to(&config.topic)
            .payload(&warmup_payload)
            .key(&key)
            .partition(partition);
        let _ = producer.send(record, Duration::from_secs(10)).await;
    }
    // Wait for metadata to propagate and partitions to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Warm-up complete, starting benchmark...");

    let payload: Vec<u8> = vec![b'x'; config.message_size];
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..config.message_count {
        let permit = semaphore.clone().acquire_owned().await?;
        let producer = producer.clone();
        let payload = payload.clone();
        let topic = config.topic.clone();
        let sent = sent.clone();
        let errors = errors.clone();
        let key = format!("key-{}", i % 100);

        let handle = tokio::spawn(async move {
            let record = FutureRecord::to(&topic).payload(&payload).key(&key);

            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
                Err((e, _)) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Produce error: {}", e);
                }
            }
            drop(permit);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total_sent = sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let total_bytes = total_sent * config.message_size as u64;
    let msgs_per_sec = total_sent as f64 / elapsed.as_secs_f64();
    let mb_per_sec = total_bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("Results:");
    println!("  Messages sent: {}", total_sent);
    println!("  Errors: {}", total_errors);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.0} msgs/sec", msgs_per_sec);
    println!("  Throughput: {:.2} MB/sec", mb_per_sec);

    Ok(())
}

async fn run_fetch_benchmark(config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Fetch Benchmark ===");
    println!(
        "Fetching messages from {} (reading what was produced)",
        config.topic
    );

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .set("group.id", "benchmark-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.bytes", "52428800")
        .create()?;

    consumer.subscribe(&[&config.topic])?;

    let start = Instant::now();
    let mut count = 0u64;
    let mut bytes = 0u64;
    let timeout = Duration::from_secs(30);
    let fetch_start = Instant::now();

    loop {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(msg)) => {
                count += 1;
                bytes += msg.payload().map(|p| p.len()).unwrap_or(0) as u64;

                if count >= config.message_count {
                    break;
                }
            }
            Ok(Err(e)) => {
                eprintln!("Consumer error: {}", e);
            }
            Err(_) => {
                // Timeout - check if we've been waiting too long overall
                if fetch_start.elapsed() > timeout {
                    println!(
                        "Timeout waiting for messages (got {} of {})",
                        count, config.message_count
                    );
                    break;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    let mb_per_sec = bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("Results:");
    println!("  Messages fetched: {}", count);
    println!("  Bytes fetched: {}", bytes);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.0} msgs/sec", msgs_per_sec);
    println!("  Throughput: {:.2} MB/sec", mb_per_sec);

    Ok(())
}

async fn run_latency_benchmark(config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Latency Benchmark ===");
    println!("Measuring end-to-end latency with synchronous produces");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", "30000")
        .set("linger.ms", "0") // No batching for latency measurement
        .set("acks", "all")
        .create()?;

    let payload: Vec<u8> = vec![b'y'; config.message_size];
    let latency_topic = format!("{}-latency", config.topic);
    let sample_count = 1000.min(config.message_count);

    // Warm up: send one message to trigger topic creation, then wait for partitions to stabilize
    println!("Warming up (creating topic and waiting for partition assignment)...");
    let warmup_record = FutureRecord::to(&latency_topic)
        .payload(&payload)
        .key("warmup");
    let _ = producer.send(warmup_record, Duration::from_secs(30)).await;

    // Wait for partition assignments to stabilize after topic creation
    // This avoids measuring latency during the initial partition handoff period
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("Warm-up complete, starting latency measurements...");

    let mut stats = LatencyStats::default();

    for i in 0..sample_count {
        let key = format!("latency-{}", i);
        let record = FutureRecord::to(&latency_topic).payload(&payload).key(&key);

        let start = Instant::now();
        match producer.send(record, Duration::from_secs(10)).await {
            Ok(_) => {
                stats.add(start.elapsed());
            }
            Err((e, _)) => {
                eprintln!("Latency test error: {}", e);
            }
        }
    }

    println!("Results ({} samples):", sample_count);
    println!("  Latency: {}", stats.summary());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchmarkConfig::from_env();

    println!("Kafkaesque Broker Benchmark");
    println!("=====================");
    println!("Configuration:");
    println!("  Broker: {}", config.broker);
    println!("  Topic: {}", config.topic);
    println!("  Message count: {}", config.message_count);
    println!("  Message size: {} bytes", config.message_size);
    println!("  Concurrency: {}", config.concurrency);

    // Run benchmarks
    run_produce_benchmark(&config).await?;
    run_fetch_benchmark(&config).await?;
    run_latency_benchmark(&config).await?;

    println!("\n=== Benchmark Complete ===");

    Ok(())
}
