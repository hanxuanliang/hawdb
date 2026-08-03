#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageBackupReport {
    pub generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub file_count: usize,
    pub total_bytes: u64,
    pub manifest_checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageRestoreReport {
    pub generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub file_count: usize,
    pub total_bytes: u64,
    pub manifest_checksum: u64,
}
