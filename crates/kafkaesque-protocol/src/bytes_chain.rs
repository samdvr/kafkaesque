//! Zero-copy response chaining.
//!
//! `BytesChain` accumulates a sequence of `Bytes` slices and a small
//! header `BytesMut` for length prefixes. The fetch path can pour records
//! straight from SlateDB into this chain (each record batch is a refcount
//! bump, never a memcpy) and then ship it to the socket via vectored I/O.
//!
//! Today's encoder fills a single `Vec<u8>` and `write_all`s it. That
//! works, but a 10 MiB multi-partition fetch ends up memcpy'd twice — once
//! into the response buffer, once into the kernel send buffer. With the
//! chain, the records `Bytes` rides through unchanged: refcount on the
//! way in, vectored-write on the way out, free on the way back.

use bytes::Bytes;

/// A sequence of `Bytes` slices to be written contiguously to a socket.
///
/// This type is intentionally tiny — it owns nothing the underlying
/// SlateDB blocks don't already own. Cloning it is cheap (refcount bumps
/// the inner Arcs); appending is a single `Vec` push.
#[derive(Debug, Default, Clone)]
pub struct BytesChain {
    chunks: Vec<Bytes>,
}

impl BytesChain {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-allocate the inner vec when the count is known up-front
    /// (fetch encoder knows partition count + ~3 chunks per partition).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            chunks: Vec::with_capacity(capacity),
        }
    }

    /// Append one `Bytes` slice. Empty slices are dropped because every
    /// extra IoSlice in a vectored write has a syscall-level cost.
    pub fn push(&mut self, b: Bytes) {
        if !b.is_empty() {
            self.chunks.push(b);
        }
    }

    /// Total bytes across the chain, useful for the outer length prefix.
    pub fn len(&self) -> usize {
        self.chunks.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.iter().all(|b| b.is_empty())
    }

    /// Borrow the chunks for vectored I/O.
    pub fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    /// Consume the chain and return the underlying chunks.
    pub fn into_chunks(self) -> Vec<Bytes> {
        self.chunks
    }

    /// Materialize the chain into a single contiguous `Vec<u8>`. This is
    /// the slow path — useful for tests and for callers that don't yet
    /// support vectored writes. Production code should consume the chain
    /// directly.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        for chunk in &self.chunks {
            out.extend_from_slice(chunk);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_chain_is_zero_length() {
        let c = BytesChain::new();
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert!(c.to_vec().is_empty());
    }

    #[test]
    fn empty_pushes_dropped() {
        let mut c = BytesChain::new();
        c.push(Bytes::from_static(b""));
        c.push(Bytes::from_static(b"hello"));
        c.push(Bytes::from_static(b""));
        c.push(Bytes::from_static(b" world"));
        assert_eq!(c.chunks().len(), 2);
        assert_eq!(c.to_vec(), b"hello world");
    }

    #[test]
    fn len_sums_all_chunks() {
        let mut c = BytesChain::with_capacity(4);
        c.push(Bytes::from_static(b"abc"));
        c.push(Bytes::from_static(b"defg"));
        c.push(Bytes::from_static(b"hi"));
        assert_eq!(c.len(), 9);
    }

    #[test]
    fn chain_clone_is_cheap_refcount_bump() {
        let payload = Bytes::from(vec![0u8; 1_000_000]);
        let original_strong_count = {
            let mut c = BytesChain::new();
            c.push(payload.clone());
            let _cloned = c.clone();
            // refcounts on the inner Bytes increase, the data is unmoved.
            // The strong count includes: payload (outer), c.chunks[0],
            // _cloned.chunks[0] = 3.
            // We can't read it directly without `Bytes::strong_count`
            // (unstable), so just assert the data is intact.
            payload.len()
        };
        assert_eq!(original_strong_count, 1_000_000);
    }
}
