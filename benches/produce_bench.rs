//! Criterion micro-benchmarks for Kafkaesque produce path.
//!
//! These benchmarks measure allocation and processing overhead for:
//! - Batch rewriting (patching base_offset)
//! - Buffer allocation patterns
//! - Key encoding
//!
//! Run with: `cargo bench --bench produce_bench`

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

/// Simulate batch rewriting with allocation (current approach).
fn batch_rewrite_with_alloc(records: &[u8], base_offset: i64) -> Vec<u8> {
    let new_hwm = base_offset + 10; // Assume 10 records
    let mut value_with_metadata = Vec::with_capacity(8 + records.len());
    value_with_metadata.extend_from_slice(&new_hwm.to_be_bytes());
    value_with_metadata.extend_from_slice(records);

    // Patch base_offset at offset 8 (where batch starts) + 8 (base_offset position)
    if value_with_metadata.len() >= 16 {
        value_with_metadata[8..16].copy_from_slice(&base_offset.to_be_bytes());
    }

    value_with_metadata
}

/// Simulate batch rewriting with pre-allocated buffer (optimized).
fn batch_rewrite_with_pool(records: &[u8], base_offset: i64, buffer: &mut Vec<u8>) {
    buffer.clear();
    let new_hwm = base_offset + 10;
    buffer.reserve(8 + records.len());
    buffer.extend_from_slice(&new_hwm.to_be_bytes());
    buffer.extend_from_slice(records);

    if buffer.len() >= 16 {
        buffer[8..16].copy_from_slice(&base_offset.to_be_bytes());
    }
}

/// Encode a record key (offset-based).
fn encode_record_key(offset: i64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = b'r'; // RECORD_KEY_PREFIX
    key[1..9].copy_from_slice(&offset.to_be_bytes());
    key
}

/// Benchmark batch rewriting with different payload sizes.
fn bench_batch_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_rewrite");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let records = vec![0u8; *size];

        group.throughput(Throughput::Bytes(*size as u64));

        // Current approach: allocate new Vec each time
        group.bench_with_input(BenchmarkId::new("alloc", size), size, |b, _| {
            b.iter(|| batch_rewrite_with_alloc(black_box(&records), black_box(12345)));
        });

        // Optimized approach: reuse buffer
        let mut buffer = Vec::with_capacity(*size + 8);
        group.bench_with_input(BenchmarkId::new("pooled", size), size, |b, _| {
            b.iter(|| batch_rewrite_with_pool(black_box(&records), black_box(12345), &mut buffer));
        });
    }

    group.finish();
}

/// Benchmark key encoding.
fn bench_key_encoding(c: &mut Criterion) {
    c.bench_function("encode_record_key", |b| {
        b.iter(|| encode_record_key(black_box(123456789)));
    });
}

/// Benchmark Vec allocation for common batch sizes.
fn bench_vec_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_allocation");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("with_capacity", size), size, |b, &size| {
            b.iter(|| {
                let v: Vec<u8> = Vec::with_capacity(size);
                black_box(v);
            });
        });

        group.bench_with_input(BenchmarkId::new("zeroed", size), size, |b, &size| {
            b.iter(|| {
                let v = vec![0u8; size];
                black_box(v);
            });
        });
    }

    group.finish();
}

/// Benchmark CAS operations for HWM allocation simulation.
fn bench_atomic_cas(c: &mut Criterion) {
    use std::sync::atomic::{AtomicI64, Ordering};

    let hwm = AtomicI64::new(0);

    c.bench_function("atomic_cas_hwm", |b| {
        b.iter(|| {
            let current = hwm.load(Ordering::SeqCst);
            let new_hwm = current + 10;
            let _ = hwm.compare_exchange(current, new_hwm, Ordering::SeqCst, Ordering::SeqCst);
            black_box(new_hwm);
        });
    });
}

/// Benchmark Bytes::from vs copy_from_slice.
fn bench_bytes_creation(c: &mut Criterion) {
    use bytes::Bytes;

    let mut group = c.benchmark_group("bytes_creation");

    for size in [100, 1_000, 10_000].iter() {
        let data = vec![0u8; *size];

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("from_vec", size), size, |b, _| {
            b.iter(|| {
                let v = data.clone();
                let bytes = Bytes::from(v);
                black_box(bytes);
            });
        });

        group.bench_with_input(BenchmarkId::new("copy_from_slice", size), size, |b, _| {
            b.iter(|| {
                let bytes = Bytes::copy_from_slice(&data);
                black_box(bytes);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_batch_rewrite,
    bench_key_encoding,
    bench_vec_allocation,
    bench_atomic_cas,
    bench_bytes_creation,
);
criterion_main!(benches);
