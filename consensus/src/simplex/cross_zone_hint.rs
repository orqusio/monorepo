//! Cross-zone finalization hint sink for marshal recovery catch-up.
//!
//! When simplex batcher receives a [`crate::simplex::types::Finalization`] from another
//! consensus zone, it forwards the unverified certificate here so marshal can discover
//! the network tip and schedule forward finalized backfill.

use crate::simplex::types::Finalization;
use commonware_cryptography::{certificate::Scheme, Digest};
use commonware_utils::channel::{fallible::AsyncFallibleExt, mpsc};

/// Forwards unverified cross-zone finalization hints to marshal (or a bridge task).
#[derive(Clone)]
pub struct CrossZoneFinalizationHintSink<S: Scheme, D: Digest> {
    sender: mpsc::Sender<Finalization<S, D>>,
}

impl<S: Scheme, D: Digest> CrossZoneFinalizationHintSink<S, D> {
    /// Creates a sink backed by the given channel.
    pub const fn new(sender: mpsc::Sender<Finalization<S, D>>) -> Self {
        Self { sender }
    }

    /// Fire-and-forget hint delivery.
    pub fn hint(&self, finalization: Finalization<S, D>) {
        let _ = self.sender.try_send_lossy(finalization);
    }
}
