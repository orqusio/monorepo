//! Recovery-aware gap sync gate for marshal backfill (Orqus emergency recovery).
//!
//! See `orqus-reth/docs/marshal-recovery-gap-sync.md` for the full design.

use std::future::Future;
use std::pin::Pin;

/// Identity of an on-chain recovery command (slot 3), aligned with recency keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryIdentity {
    pub failing_height: u64,
    pub nonce: u64,
    pub consensus_digest: [u8; 32],
    pub is_admin: bool,
}

/// Active forward-fill session while catching up through recovery epochs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingGap {
    pub recovery_target: u64,
    pub failing_height: u64,
    pub fill_start: u64,
    /// Set after [`RecoverySyncGate::on_switch_complete`]; allows storing the execution
    /// block at `failing_height` that was deferred during the notify/switch handshake.
    pub switch_completed: bool,
}

/// Switch in flight after `notify_*` until `RecoverySwitchComplete`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryAwaiting;

/// Marshal-side gate coordinating recovery deliver, notify, and forward fetch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverySyncGate {
    pub awaiting: Option<RecoveryAwaiting>,
    pub pending_gap: Option<PendingGap>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RecoveryNotifyError {
    #[error("recovery switch notifier not configured")]
    NotConfigured,
    #[error("recovery notify failed: {0}")]
    Failed(String),
}

/// Notifies epoch manager / recovery actor to run `emergency_enter` before marshal stores
/// recovery-related blocks.
pub trait RecoverySwitchNotifier: Send + Sync {
    type NotifyFut<'a>: Future<Output = Result<(), RecoveryNotifyError>> + Send + 'a
    where
        Self: 'a;

    fn notify_execution_recovery<'a>(
        &'a self,
        extra: &'a [u8],
        block_height: u64,
    ) -> Self::NotifyFut<'a>;

    fn notify_boundary_recovery_replay<'a>(
        &'a self,
        extra: &'a [u8],
        block_height: u64,
    ) -> Self::NotifyFut<'a>;
}

impl RecoverySwitchNotifier for () {
    type NotifyFut<'a> = std::future::Ready<Result<(), RecoveryNotifyError>>;

    fn notify_execution_recovery<'a>(
        &'a self,
        _extra: &'a [u8],
        _block_height: u64,
    ) -> Self::NotifyFut<'a> {
        std::future::ready(Err(RecoveryNotifyError::NotConfigured))
    }

    fn notify_boundary_recovery_replay<'a>(
        &'a self,
        _extra: &'a [u8],
        _block_height: u64,
    ) -> Self::NotifyFut<'a> {
        std::future::ready(Err(RecoveryNotifyError::NotConfigured))
    }
}

/// Completion payload from recovery actor after `emergency_enter` finishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverySwitchComplete {
    pub identity: RecoveryIdentity,
}

/// Whether the anchor block's first deliver should notify before forward fill.
///
/// Requires the local processed prefix to end exactly at `F - 1` (hot restart /
/// tail fill). Out-of-order network delivers are cached separately; notify runs
/// when `height == last_processed + 1` via sequential catch-up.
pub fn anchor_first_deliver_should_notify(last_processed: u64, failing_height: u64) -> bool {
    last_processed == failing_height.saturating_sub(1)
}

/// Whether marshal should notify EL / recovery actor to run `emergency_enter`.
///
/// Only execution blocks (`height == failing_height`) when the local prefix has
/// been processed through `failing_height - 1`. Boundary replay anchors
/// (`failing_height < height`) never notify; they are cached until sequential
/// catch-up reaches each execution height.
pub fn should_notify_recovery_execution(
    last_processed: u64,
    height: u64,
    failing_height: u64,
) -> bool {
    height == failing_height && last_processed.saturating_add(1) == failing_height
}

/// Whether marshal should store (not re-notify) the execution block at `F` after switch.
///
/// The first deliver at `F` triggers notify and returns `Deferred` without storing.
/// Once `switch_completed` is set, the same block must be stored on re-deliver.
pub fn should_store_execution_after_switch(
    switch_completed: bool,
    last_processed: u64,
    height: u64,
    failing_height: u64,
) -> bool {
    switch_completed
        && height == failing_height
        && last_processed.saturating_add(1) == failing_height
}

/// Whether a recovery deliver at `height` should only be cached (not notify yet)
/// because blocks between `last_processed` and `height` have not been processed.
///
/// Requires an active `pending_gap` and `height > last_processed + 1`. First-anchor
/// delivers (no `pending_gap` yet) use the cold-start / notify paths in
/// [`super::core::actor::Actor::evaluate_recovery_finalized`].
pub fn should_cache_recovery_out_of_order(
    last_processed: u64,
    height: u64,
    has_pending_gap: bool,
) -> bool {
    has_pending_gap && height > last_processed.saturating_add(1)
}

/// Whether to skip backward `Block(parent)` repair during recovery catch-up.
///
/// While `pending_gap` is active the epoch is filling forward only via
/// `missing_items` / `Finalized` fetch (execution blocks and boundary replay anchors).
pub fn should_skip_backward_gap_repair(has_pending_gap: bool) -> bool {
    has_pending_gap
}

pub fn forward_fill_start(last_processed: u64, gap_start: u64, failing_height: u64) -> u64 {
    if last_processed < failing_height.saturating_sub(1) {
        gap_start.max(1)
    } else {
        last_processed.saturating_add(1)
    }
}

pub fn is_epoch_boundary(height: u64, epoch_length: u64) -> bool {
    epoch_length > 0 && (height.saturating_add(1)) % epoch_length == 0
}

pub fn is_recovery_completion_deliver(
    height: u64,
    last_processed: u64,
    gap: &PendingGap,
) -> bool {
    gap.recovery_target == height && last_processed.saturating_add(1) == height
}

impl RecoverySyncGate {
    pub fn on_switch_complete(&mut self) {
        self.awaiting = None;
        if let Some(gap) = &mut self.pending_gap {
            gap.switch_completed = true;
        }
    }

    pub fn continuation_fetch_start(&self, last_processed: u64) -> u64 {
        if let Some(gap) = &self.pending_gap {
            forward_fill_start(last_processed, gap.fill_start, gap.failing_height)
        } else {
            last_processed.saturating_add(1)
        }
    }

    /// Snapshot for executor / observability.
    pub fn status(&self) -> RecoverySyncGateStatus {
        RecoverySyncGateStatus {
            awaiting: self.awaiting.is_some(),
            pending_recovery_target: self.pending_gap.as_ref().map(|g| g.recovery_target),
            pending_failing_height: self.pending_gap.as_ref().map(|g| g.failing_height),
            last_processed_height: 0,
        }
    }

    /// Whether marshal / executor should defer `Update::Tip` and Tip FCU for `height`.
    pub fn should_defer_tip_fcu(&self, last_processed: u64, height: u64) -> bool {
        should_defer_tip_fcu(
            self.awaiting.is_some(),
            self.pending_gap.as_ref().map(|g| g.recovery_target),
            last_processed,
            height,
        )
    }
}

/// Whether marshal / executor should defer `Update::Tip` and Tip FCU for `height`.
///
/// - `awaiting`: defer all tip advances (switch in flight).
/// - `pending_gap`: defer tip at or above `recovery_target` (anchor) until completion.
/// - defer tip more than one height ahead of `last_processed` so EL FCU stays aligned
///   with sequential `Update::Block` dispatch (recovery forward fill and cold catch-up).
pub fn should_defer_tip_fcu(
    awaiting: bool,
    pending_recovery_target: Option<u64>,
    last_processed: u64,
    height: u64,
) -> bool {
    if awaiting {
        return true;
    }
    if pending_recovery_target.is_some_and(|target| height >= target) {
        return true;
    }
    height > last_processed.saturating_add(1)
}

/// Observable snapshot of [`RecoverySyncGate`] for executor FCU gating.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverySyncGateStatus {
    pub awaiting: bool,
    pub pending_recovery_target: Option<u64>,
    pub pending_failing_height: Option<u64>,
    pub last_processed_height: u64,
}

impl RecoverySyncGateStatus {
    pub fn should_defer_tip_fcu(&self, height: u64) -> bool {
        should_defer_tip_fcu(
            self.awaiting,
            self.pending_recovery_target,
            self.last_processed_height,
            height,
        )
    }

    pub fn active(&self) -> bool {
        self.awaiting || self.pending_recovery_target.is_some()
    }

    /// Whether marshal is still catching up sequentially (live tip ahead of processed prefix).
    pub fn has_sequential_gap(&self, height: u64) -> bool {
        height > self.last_processed_height.saturating_add(1)
    }
}

/// Boxed future helper for async notifier implementations (used by orqus-reth).
#[allow(dead_code)]
pub type BoxedNotifyFut<'a> =
    Pin<Box<dyn Future<Output = Result<(), RecoveryNotifyError>> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_before_switch_store_after_switch() {
        let last = 269u64;
        let f = 270u64;
        assert!(should_notify_recovery_execution(last, f, f));
        assert!(!should_store_execution_after_switch(false, last, f, f));
        assert!(should_store_execution_after_switch(true, last, f, f));
    }

    #[test]
    fn on_switch_complete_sets_flag_on_pending_gap() {
        let mut gate = RecoverySyncGate {
            awaiting: Some(RecoveryAwaiting),
            pending_gap: Some(PendingGap {
                recovery_target: 279,
                failing_height: 270,
                fill_start: 270,
                switch_completed: false,
            }),
        };
        gate.on_switch_complete();
        assert!(gate.awaiting.is_none());
        assert!(gate.pending_gap.as_ref().unwrap().switch_completed);
    }
}
