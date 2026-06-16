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

/// Kind of recovery pending at an anchor boundary or execution height.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingRecoveryKind {
    Execution,
    BoundaryExecution,
    BoundaryReplay,
}

/// Active forward-fill session while catching up through recovery epochs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingGap {
    pub recovery_target: u64,
    pub failing_height: u64,
    pub kind: PendingRecoveryKind,
    pub fill_start: u64,
}

/// Switch in flight after `notify_*` until `RecoverySwitchComplete`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryAwaiting {
    pub identity: RecoveryIdentity,
}

/// Marshal-side gate coordinating recovery deliver, notify, and forward fetch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverySyncGate {
    pub awaiting: Option<RecoveryAwaiting>,
    pub pending_gap: Option<PendingGap>,
    pub last_applied_recovery: Option<RecoveryIdentity>,
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

/// Whether a cold-start anchor should skip notify and only record `pending_gap`.
pub fn anchor_first_deliver_should_notify(last_processed: u64, failing_height: u64) -> bool {
    last_processed >= failing_height.saturating_sub(1)
}

pub fn forward_fill_start(last_processed: u64, gap_start: u64, pending: &PendingGap) -> u64 {
    if last_processed < pending.failing_height.saturating_sub(1) {
        gap_start.max(1)
    } else {
        last_processed.saturating_add(1)
    }
}

pub fn is_epoch_boundary(height: u64, epoch_length: u64) -> bool {
    epoch_length > 0 && (height.saturating_add(1)) % epoch_length == 0
}

pub fn classify_pending_kind(
    block_height: u64,
    failing_height: u64,
    epoch_length: u64,
) -> PendingRecoveryKind {
    let is_boundary = is_epoch_boundary(block_height, epoch_length);
    if is_boundary && failing_height < block_height {
        PendingRecoveryKind::BoundaryReplay
    } else if is_boundary && failing_height == block_height {
        PendingRecoveryKind::BoundaryExecution
    } else {
        PendingRecoveryKind::Execution
    }
}

pub fn is_recovery_completion_deliver(
    height: u64,
    last_processed: u64,
    pending: &Option<PendingGap>,
    awaiting: bool,
    block_identity: Option<RecoveryIdentity>,
    last_applied: &Option<RecoveryIdentity>,
) -> bool {
    let Some(gap) = pending else {
        return false;
    };
    if awaiting || gap.recovery_target != height || last_processed.saturating_add(1) != height {
        return false;
    }
    let Some(id) = block_identity else {
        return true;
    };
    last_applied.as_ref() == Some(&id)
}

impl RecoverySyncGate {
    pub fn on_switch_complete(&mut self, identity: RecoveryIdentity) {
        self.awaiting = None;
        self.last_applied_recovery = Some(identity);
    }

    pub fn continuation_fetch_start(&self, last_processed: u64) -> u64 {
        if let Some(gap) = &self.pending_gap {
            forward_fill_start(last_processed, gap.fill_start, gap)
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
/// - `pending_gap`: defer tip more than one height ahead of `last_processed` so EL FCU
///   stays aligned with sequential `Update::Block` dispatch during forward fill.
pub fn should_defer_tip_fcu(
    awaiting: bool,
    pending_recovery_target: Option<u64>,
    last_processed: u64,
    height: u64,
) -> bool {
    if awaiting {
        return true;
    }
    let Some(target) = pending_recovery_target else {
        return false;
    };
    if height >= target {
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
}

/// Boxed future helper for async notifier implementations (used by orqus-reth).
#[allow(dead_code)]
pub type BoxedNotifyFut<'a> =
    Pin<Box<dyn Future<Output = Result<(), RecoveryNotifyError>> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use super::should_defer_tip_fcu;

    #[test]
    fn defer_anchor_and_out_of_order_tips_during_gap() {
        assert!(should_defer_tip_fcu(false, Some(199), 159, 199));
        assert!(should_defer_tip_fcu(false, Some(199), 159, 166));
        assert!(!should_defer_tip_fcu(false, Some(199), 159, 160));
        assert!(!should_defer_tip_fcu(false, Some(199), 160, 161));
    }

    #[test]
    fn defer_all_tips_while_awaiting() {
        assert!(should_defer_tip_fcu(true, Some(199), 159, 160));
        assert!(should_defer_tip_fcu(true, None, 159, 160));
    }
}
