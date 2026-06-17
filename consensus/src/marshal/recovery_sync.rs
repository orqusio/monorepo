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

/// Blocks backward gap repair until the anchor execution at `last_failing_height` (failing height `F`)
/// has completed `emergency_enter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryAwaiting;

/// Marshal-side gate coordinating recovery deliver, notify, and gap repair.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverySyncGate {
    pub awaiting: Option<RecoveryAwaiting>,
    /// Anchor height `H` from the first recovery deliver; used for Tip defer / observability
    /// until `last_processed >= H`. Independent of [`Self::last_failing_height`] lifetime.
    pub recovery_block_height: Option<u64>,
    /// Active only until anchor execution `F` finishes switch (failing height).
    pub last_failing_height: Option<u64>,
    /// Latest execution `F` that finished switch; suppresses duplicate notify until stored.
    pub recent_switch_failing: Option<u64>,
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
}

/// Completion payload from recovery actor after `emergency_enter` finishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverySwitchComplete {
    pub identity: RecoveryIdentity,
}

/// Whether marshal should notify EL / recovery actor to run `emergency_enter`.
///
/// Only execution blocks (`height == failing_height`) when the local prefix has
/// been processed through `failing_height - 1`. Boundary replay anchors
/// (`failing_height < height`) never notify.
pub fn should_notify_recovery_execution(
    last_processed: u64,
    height: u64,
    failing_height: u64,
) -> bool {
    height == failing_height && last_processed.saturating_add(1) == failing_height
}

impl RecoverySyncGate {
    /// Clears `awaiting` and clears `last_failing_height` when the anchor execution at `F`
    /// has switched. Idempotent for duplicate [`RecoverySwitchComplete`] mailbox deliveries.
    pub fn on_switch_complete(&mut self, switch_failing_height: u64) {
        self.awaiting = None;
        self.recent_switch_failing = Some(switch_failing_height);
        if self
            .last_failing_height
            .is_some_and(|failing_height| switch_failing_height == failing_height)
        {
            self.last_failing_height = None;
        }
    }

    /// Snapshot for executor / observability (`last_processed_height` filled by caller).
    pub fn status(&self) -> RecoverySyncGateStatus {
        self.status_at(0)
    }

    pub fn status_at(&self, last_processed: u64) -> RecoverySyncGateStatus {
        RecoverySyncGateStatus {
            awaiting: self.awaiting.is_some(),
            recovery_block_height: self.recovery_block_height,
            last_failing_height: self.last_failing_height,
            last_processed_height: last_processed,
        }
    }
}

/// Observable snapshot of [`RecoverySyncGate`] for executor FCU gating.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverySyncGateStatus {
    pub awaiting: bool,
    pub recovery_block_height: Option<u64>,
    pub last_failing_height: Option<u64>,
    pub last_processed_height: u64,
}

impl RecoverySyncGateStatus {
    /// Whether marshal / executor should defer `Update::Tip` and Tip FCU for `height`.
    pub fn should_defer_tip_fcu(&self, height: u64) -> bool {
        if self.awaiting {
            return true;
        }
        if self.recovery_block_height.is_some_and(|target| {
            self.last_processed_height < target && height >= target
        }) {
            return true;
        }
        height > self.last_processed_height.saturating_add(1)
    }

    pub fn active(&self) -> bool {
        self.awaiting || self.recovery_block_height.is_some()
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
