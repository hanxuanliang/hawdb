use crate::error::{Result, SkeinError};
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

pub const DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES: NonZeroUsize =
    NonZeroUsize::new(16).expect("WAL group commit entry bound is non-zero");
pub const DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES: NonZeroU64 =
    NonZeroU64::new(1024 * 1024).expect("WAL group commit byte bound is non-zero");
pub const DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(250);
const MAX_WAL_GROUP_COMMIT_ENTRIES: usize = 256;
const MAX_WAL_GROUP_COMMIT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_WAL_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalGroupCommitActivation {
    #[default]
    Disabled,
    BenchmarkCandidate,
    EvidenceValidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitEvidence {
    pub commit_count: usize,
    pub baseline_elapsed_micros: u64,
    pub baseline_fsync_count: u64,
    pub grouped_elapsed_micros: u64,
    pub grouped_fsync_count: u64,
    pub grouped_p95_commit_micros: u64,
    pub max_accepted_p95_commit_micros: u64,
    pub single_writer_commit_count: usize,
    pub single_writer_grouped_p95_commit_micros: u64,
    pub max_accepted_single_writer_p95_commit_micros: u64,
    pub strict_recovery_verified: bool,
    pub wal_order_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitConfig {
    activation: WalGroupCommitActivation,
    /// Exact upper bound on commit requests sharing one durability barrier.
    max_entries: NonZeroUsize,
    /// Coalescing target checked after each unchanged, individually bounded WAL record.
    /// The hard group byte bound is this target plus one configured WAL record.
    max_bytes: NonZeroU64,
    /// Exact upper bound on the initial coalescing wait.
    max_delay: Duration,
}

impl WalGroupCommitConfig {
    pub const fn disabled() -> Self {
        Self {
            activation: WalGroupCommitActivation::Disabled,
            max_entries: DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
            max_bytes: DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES,
            max_delay: DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
        }
    }

    /// Enables the candidate path only for collecting benchmark evidence.
    /// Production callers should use `enabled_after_evidence`.
    pub fn benchmark_candidate(
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        Self::with_activation(
            WalGroupCommitActivation::BenchmarkCandidate,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    pub fn enabled_after_evidence(
        evidence: WalGroupCommitEvidence,
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        validate_evidence(evidence)?;
        Self::with_activation(
            WalGroupCommitActivation::EvidenceValidated,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    fn with_activation(
        activation: WalGroupCommitActivation,
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        if max_entries.get() > MAX_WAL_GROUP_COMMIT_ENTRIES {
            return Err(SkeinError::Execution(format!(
                "WAL group commit max_entries must be <= {MAX_WAL_GROUP_COMMIT_ENTRIES}"
            )));
        }
        if max_bytes.get() > MAX_WAL_GROUP_COMMIT_BYTES {
            return Err(SkeinError::Execution(format!(
                "WAL group commit max_bytes must be <= {MAX_WAL_GROUP_COMMIT_BYTES}"
            )));
        }
        if max_delay > MAX_WAL_GROUP_COMMIT_DELAY {
            return Err(SkeinError::Execution(format!(
                "WAL group commit max_delay must be <= {} ms",
                MAX_WAL_GROUP_COMMIT_DELAY.as_millis()
            )));
        }
        Ok(Self {
            activation,
            max_entries,
            max_bytes,
            max_delay,
        })
    }

    pub const fn activation(self) -> WalGroupCommitActivation {
        self.activation
    }

    pub const fn is_enabled(self) -> bool {
        !matches!(self.activation, WalGroupCommitActivation::Disabled)
    }

    pub const fn max_entries(self) -> NonZeroUsize {
        self.max_entries
    }

    pub const fn max_bytes(self) -> NonZeroU64 {
        self.max_bytes
    }

    pub const fn max_delay(self) -> Duration {
        self.max_delay
    }
}

impl Default for WalGroupCommitConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalGroupCommitSnapshot {
    pub activation: WalGroupCommitActivation,
    pub submitted_commits: u64,
    pub completed_commits: u64,
    pub group_count: u64,
    pub coalescing_wait_count: u64,
    pub shared_sync_count: u64,
    pub grouped_wal_entries: u64,
    pub grouped_wal_bytes: u64,
    pub max_observed_group_entries: usize,
    pub max_observed_group_bytes: u64,
    pub total_fsync_micros: u64,
}

fn validate_evidence(evidence: WalGroupCommitEvidence) -> Result<()> {
    let mut blockers = Vec::new();
    if evidence.commit_count < 2 {
        blockers.push("insufficient_commit_count");
    }
    if evidence.grouped_fsync_count >= evidence.baseline_fsync_count
        || evidence.grouped_fsync_count >= evidence.commit_count as u64
    {
        blockers.push("fsync_reduction_not_proven");
    }
    if evidence.grouped_elapsed_micros >= evidence.baseline_elapsed_micros {
        blockers.push("throughput_improvement_not_proven");
    }
    if evidence.grouped_p95_commit_micros > evidence.max_accepted_p95_commit_micros {
        blockers.push("tail_latency_budget_exceeded");
    }
    if evidence.single_writer_commit_count == 0 {
        blockers.push("single_writer_evidence_missing");
    }
    if evidence.single_writer_grouped_p95_commit_micros
        > evidence.max_accepted_single_writer_p95_commit_micros
    {
        blockers.push("single_writer_tail_latency_budget_exceeded");
    }
    if !evidence.strict_recovery_verified {
        blockers.push("strict_recovery_not_verified");
    }
    if !evidence.wal_order_verified {
        blockers.push("wal_order_not_verified");
    }
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(SkeinError::Execution(format!(
            "WAL group commit evidence rejected: {}",
            blockers.join(",")
        )))
    }
}
