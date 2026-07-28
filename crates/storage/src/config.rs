#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityPolicy {
    #[default]
    SyncOnCheckpoint,
    SyncOnEveryWrite,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecoveryMode {
    #[default]
    TolerateTornTail,
    Strict,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableCompression {
    #[default]
    Zstd,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalReplayConfig {
    pub recovery_mode: RecoveryMode,
    pub max_entries: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_recoverable() {
        assert_eq!(
            DurabilityPolicy::default(),
            DurabilityPolicy::SyncOnCheckpoint
        );
        assert_eq!(RecoveryMode::default(), RecoveryMode::TolerateTornTail);
        assert_eq!(DurableCompression::default(), DurableCompression::Zstd);
        assert_eq!(WalReplayConfig::default().max_entries, None);
    }
}
