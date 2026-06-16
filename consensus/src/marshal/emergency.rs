//! Recovery QC bypass for marshal certificate verification (v2 five-slot `extra_data`).
//!
//! Applications with execution-layer validator sets inject [`EmergencyQcVerifier`] via
//! [`super::Config::emergency_qc_verifier`]. The default [`()`] implementation never
//! bypasses BLS.

use super::recovery_sync::RecoveryIdentity;

/// Verifies recovery QC in block `extra_data` (slots 2–4) for marshal backfill.
///
/// Return semantics in [`Self::verify_emergency_extra_data`]:
/// - `None` — no recovery payload; marshal falls through to BLS.
/// - `Some(true)` — recovery verified; skip BLS for this block.
/// - `Some(false)` — recovery present but invalid; fall through to BLS.
pub trait EmergencyQcVerifier: Send + Sync {
    fn verify_emergency_extra_data(&self, extra: &[u8], block_height: u64) -> Option<bool>;

    /// Returns true when slot 3 `recovery_consensus` is present.
    fn has_recovery_slots(&self, extra: &[u8]) -> bool {
        let _ = extra;
        false
    }

    /// Parse `failing_height` from slot 3 `recovery_consensus`.
    fn parse_recovery_failing_height(&self, extra: &[u8]) -> Option<u64> {
        self.parse_recovery_identity(extra)
            .map(|identity| identity.failing_height)
    }

    /// Parse recovery identity from `extra_data` for gate / replay matching.
    fn parse_recovery_identity(&self, extra: &[u8]) -> Option<RecoveryIdentity> {
        let _ = extra;
        None
    }
}

impl EmergencyQcVerifier for () {
    fn verify_emergency_extra_data(&self, _extra: &[u8], _block_height: u64) -> Option<bool> {
        None
    }
}
