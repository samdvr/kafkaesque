//! RAII guard for rolling back an in-flight offset reservation.

use std::sync::atomic::{AtomicI64, Ordering};

/// RAII guard that rolls back an offset reservation made via `next_offset.fetch_add`
/// unless explicitly dismissed.
///
/// `append_batch_inner` reserves offsets up-front so concurrent appenders can't
/// race for the same range. Every reject path *before* SlateDB sees the batch
/// must restore `next_offset` to its pre-reservation value, otherwise the
/// partition log accumulates permanent gaps. Threading a guard through the
/// function makes that automatic: any `?` or early return triggers Drop, which
/// rolls back. Only the success path calls `dismiss()`.
///
/// SAFETY — single-writer rollback. Callers must hold the partition's
/// `write_lock` for the entire lifetime of the guard. Without that, a
/// concurrent appender could advance `next_offset` past `base_offset` between
/// the failed early-validation step and Drop, and the rollback would clobber
/// that newer reservation. `append_batch_inner` holds `write_lock` from before
/// `fetch_add` until function exit, so the guard is dropped before the lock
/// is released.
///
/// SAFETY — cancellation hand-off. The guard MUST be disarmed (via
/// `disarm`) before `db.write_with_options` is called. Once SlateDB has
/// queued the WriteBatch, the queued task can complete asynchronously even
/// if our future is cancelled and Drop runs. A rolled-back `next_offset`
/// then lets the next appender re-take `base_offset` while SlateDB persists
/// our cancelled batch at the same key — silently clobbering one of the two
/// writes. We accept the alternative trade-off (a permanent offset gap on
/// transient write failures) so the clobber can never happen; the
/// `fail_on_recovery_gap` config gate is the operator's policy on whether
/// such gaps abort recovery.
pub(super) struct OffsetReservation<'a> {
    next_offset: &'a AtomicI64,
    base_offset: i64,
    armed: bool,
}

impl<'a> OffsetReservation<'a> {
    pub(super) fn new(next_offset: &'a AtomicI64, base_offset: i64) -> Self {
        Self {
            next_offset,
            base_offset,
            armed: true,
        }
    }

    /// Disarm in place. Call this before any operation that may complete
    /// asynchronously after our future is dropped (in practice, just before
    /// `db.write_with_options`).
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }

    /// Mark the reservation as committed; Drop becomes a no-op.
    pub(super) fn dismiss(mut self) {
        self.armed = false;
    }
}

impl Drop for OffsetReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.next_offset.store(self.base_offset, Ordering::SeqCst);
        }
    }
}
