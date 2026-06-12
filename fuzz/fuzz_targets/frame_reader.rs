#![no_main]

//! P0-2: Connection frame reader fuzzer.
//!
//! Drives [`kafkaesque::server::read_kafka_frame_for_fuzz`] over a mock
//! `AsyncRead` that returns the fuzz input in arbitrary chunks. The reader
//! is the broker's first contact with attacker-controlled bytes — a panic
//! or unbounded allocation here is a remote DoS.
//!
//! Properties checked:
//!   - Never panics on any input.
//!   - Never allocates more than `MAX_FRAME` bytes (the cap we pass in).
//!     This is enforced indirectly: an attacker-supplied `size > MAX_FRAME`
//!     must return `Err`, never reach the body `Vec::with_capacity`.
//!   - Returned body length matches the wire-declared `size` field.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use libfuzzer_sys::fuzz_target;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Builder;

use kafkaesque::server::read_kafka_frame_for_fuzz;

/// Cap for the *fuzz input itself* — well above any plausible frame size
/// libFuzzer would still mutate productively.
const MAX_INPUT: usize = 8 * 1024 * 1024;

/// The frame-size cap we pass to the reader. Real brokers pass
/// `DEFAULT_MAX_MESSAGE_SIZE` (100 MiB) but we use a smaller value so
/// libFuzzer can detect the cap-enforcement path quickly.
const MAX_FRAME: usize = 256 * 1024;

/// Mock `AsyncRead` that returns the input in chunks of pseudo-random size,
/// then returns `UnexpectedEof`. Chunking matters: the production reader
/// uses `read_exact`, which loops on partial reads, and a chunked source
/// shakes out any bug that assumes a single read returns the full buffer.
struct ChunkedReader<'a> {
    data: &'a [u8],
    pos: usize,
    /// Cycles through `chunk_sizes` to vary read sizes per call.
    chunk_idx: usize,
    chunk_sizes: &'a [u8],
}

impl<'a> ChunkedReader<'a> {
    fn new(data: &'a [u8], chunk_sizes: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            chunk_idx: 0,
            chunk_sizes,
        }
    }
}

impl<'a> AsyncRead for ChunkedReader<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pos >= self.data.len() {
            // EOF — the reader maps this to `Error::MissingData`, which we
            // expect (and don't treat as a bug).
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "fuzz EOF",
            )));
        }

        // Pick a chunk size in [1, 32]. A zero-sized read would be a spurious
        // wakeup; the production loop tolerates it but it's not what we're
        // testing here.
        let chunk = if self.chunk_sizes.is_empty() {
            16
        } else {
            let idx = self.chunk_idx % self.chunk_sizes.len();
            self.chunk_idx = self.chunk_idx.wrapping_add(1);
            (self.chunk_sizes[idx] as usize % 32) + 1
        };

        let remaining = self.data.len() - self.pos;
        let to_read = chunk.min(remaining).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        Poll::Ready(Ok(()))
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    // Split the input: the first 16 bytes (if available) drive the chunk
    // size schedule, the rest is the wire bytes the reader sees.
    let (sched, wire) = if data.len() >= 16 {
        data.split_at(16)
    } else {
        (&[][..], data)
    };

    let rt = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("build current-thread runtime");

    rt.block_on(async {
        let mut reader = ChunkedReader::new(wire, sched);
        match read_kafka_frame_for_fuzz(&mut reader, MAX_FRAME).await {
            Ok(body) => {
                // The first 4 bytes of `wire` are the size prefix; the
                // returned body must be exactly that length.
                assert!(wire.len() >= 4, "Ok with too-short input is impossible");
                let declared = i32::from_be_bytes([wire[0], wire[1], wire[2], wire[3]]);
                assert!(declared >= 0, "Ok with negative size is impossible");
                assert_eq!(
                    body.len(),
                    declared as usize,
                    "returned body length must match declared size",
                );
                assert!(
                    body.len() <= MAX_FRAME,
                    "returned body must respect MAX_FRAME cap",
                );
            }
            Err(_) => {
                // Errors are fine — the reader's contract is "Err on any
                // malformed input, never panic, never over-allocate."
            }
        }
    });
});
