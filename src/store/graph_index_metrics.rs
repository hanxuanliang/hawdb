use skein_storage::{CanonicalAdjacencyReadReport, PersistentPropertyProjectionReadReport};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum PersistentGraphIndexClass {
    NodeEquality = 0,
    NodeRange = 1,
    NodeFullText = 2,
    NodeCompositeEquality = 3,
    RelationshipEquality = 4,
    RelationshipRange = 5,
    ForwardAdjacency = 6,
    ReverseAdjacency = 7,
}

impl PersistentGraphIndexClass {
    pub const ALL: [Self; 8] = [
        Self::NodeEquality,
        Self::NodeRange,
        Self::NodeFullText,
        Self::NodeCompositeEquality,
        Self::RelationshipEquality,
        Self::RelationshipRange,
        Self::ForwardAdjacency,
        Self::ReverseAdjacency,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeEquality => "node_equality",
            Self::NodeRange => "node_range",
            Self::NodeFullText => "node_full_text",
            Self::NodeCompositeEquality => "node_composite_equality",
            Self::RelationshipEquality => "relationship_equality",
            Self::RelationshipRange => "relationship_range",
            Self::ForwardAdjacency => "forward_adjacency",
            Self::ReverseAdjacency => "reverse_adjacency",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphIndexReadMetricsSnapshot {
    operation_counts: [u64; PersistentGraphIndexClass::ALL.len()],
    block_read_counts: [u64; PersistentGraphIndexClass::ALL.len()],
    byte_read_counts: [u64; PersistentGraphIndexClass::ALL.len()],
    pub property_blocks_considered: u64,
    pub property_blocks_pruned: u64,
    pub property_blocks_read: u64,
    pub property_bytes_read: u64,
    pub property_entries_decoded: u64,
    pub property_candidates_returned: u64,
    pub adjacency_blocks_considered: u64,
    pub adjacency_blocks_read: u64,
    pub adjacency_bytes_read: u64,
    pub adjacency_records_decoded: u64,
    pub adjacency_sparse_blocks_read: u64,
    pub adjacency_dense_blocks_read: u64,
}

impl GraphIndexReadMetricsSnapshot {
    pub fn operation_count(self, class: PersistentGraphIndexClass) -> u64 {
        self.operation_counts[class as usize]
    }

    pub fn total_operation_count(self) -> u64 {
        self.operation_counts
            .into_iter()
            .fold(0, u64::saturating_add)
    }

    pub fn blocks_read(self, class: PersistentGraphIndexClass) -> u64 {
        self.block_read_counts[class as usize]
    }

    pub fn bytes_read(self, class: PersistentGraphIndexClass) -> u64 {
        self.byte_read_counts[class as usize]
    }

    pub fn delta_since(self, before: Self) -> Self {
        let mut operation_counts = [0; PersistentGraphIndexClass::ALL.len()];
        let mut block_read_counts = [0; PersistentGraphIndexClass::ALL.len()];
        let mut byte_read_counts = [0; PersistentGraphIndexClass::ALL.len()];
        for class in PersistentGraphIndexClass::ALL {
            operation_counts[class as usize] = self
                .operation_count(class)
                .saturating_sub(before.operation_count(class));
            block_read_counts[class as usize] = self
                .blocks_read(class)
                .saturating_sub(before.blocks_read(class));
            byte_read_counts[class as usize] = self
                .bytes_read(class)
                .saturating_sub(before.bytes_read(class));
        }
        Self {
            operation_counts,
            block_read_counts,
            byte_read_counts,
            property_blocks_considered: self
                .property_blocks_considered
                .saturating_sub(before.property_blocks_considered),
            property_blocks_pruned: self
                .property_blocks_pruned
                .saturating_sub(before.property_blocks_pruned),
            property_blocks_read: self
                .property_blocks_read
                .saturating_sub(before.property_blocks_read),
            property_bytes_read: self
                .property_bytes_read
                .saturating_sub(before.property_bytes_read),
            property_entries_decoded: self
                .property_entries_decoded
                .saturating_sub(before.property_entries_decoded),
            property_candidates_returned: self
                .property_candidates_returned
                .saturating_sub(before.property_candidates_returned),
            adjacency_blocks_considered: self
                .adjacency_blocks_considered
                .saturating_sub(before.adjacency_blocks_considered),
            adjacency_blocks_read: self
                .adjacency_blocks_read
                .saturating_sub(before.adjacency_blocks_read),
            adjacency_bytes_read: self
                .adjacency_bytes_read
                .saturating_sub(before.adjacency_bytes_read),
            adjacency_records_decoded: self
                .adjacency_records_decoded
                .saturating_sub(before.adjacency_records_decoded),
            adjacency_sparse_blocks_read: self
                .adjacency_sparse_blocks_read
                .saturating_sub(before.adjacency_sparse_blocks_read),
            adjacency_dense_blocks_read: self
                .adjacency_dense_blocks_read
                .saturating_sub(before.adjacency_dense_blocks_read),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct GraphIndexReadMetrics {
    operation_counts: [AtomicU64; PersistentGraphIndexClass::ALL.len()],
    block_read_counts: [AtomicU64; PersistentGraphIndexClass::ALL.len()],
    byte_read_counts: [AtomicU64; PersistentGraphIndexClass::ALL.len()],
    property_blocks_considered: AtomicU64,
    property_blocks_pruned: AtomicU64,
    property_blocks_read: AtomicU64,
    property_bytes_read: AtomicU64,
    property_entries_decoded: AtomicU64,
    property_candidates_returned: AtomicU64,
    adjacency_blocks_considered: AtomicU64,
    adjacency_blocks_read: AtomicU64,
    adjacency_bytes_read: AtomicU64,
    adjacency_records_decoded: AtomicU64,
    adjacency_sparse_blocks_read: AtomicU64,
    adjacency_dense_blocks_read: AtomicU64,
}

impl GraphIndexReadMetrics {
    pub(super) fn record_property(
        &self,
        class: PersistentGraphIndexClass,
        report: PersistentPropertyProjectionReadReport,
    ) {
        self.operation_counts[class as usize].fetch_add(1, Ordering::Relaxed);
        self.block_read_counts[class as usize].fetch_add(report.blocks_read, Ordering::Relaxed);
        self.byte_read_counts[class as usize].fetch_add(report.bytes_read, Ordering::Relaxed);
        self.property_blocks_considered
            .fetch_add(report.blocks_considered, Ordering::Relaxed);
        self.property_blocks_pruned
            .fetch_add(report.blocks_pruned, Ordering::Relaxed);
        self.property_blocks_read
            .fetch_add(report.blocks_read, Ordering::Relaxed);
        self.property_bytes_read
            .fetch_add(report.bytes_read, Ordering::Relaxed);
        self.property_entries_decoded
            .fetch_add(report.entries_decoded, Ordering::Relaxed);
        self.property_candidates_returned
            .fetch_add(report.candidates_returned, Ordering::Relaxed);
    }

    pub(super) fn record_adjacency(
        &self,
        class: PersistentGraphIndexClass,
        report: CanonicalAdjacencyReadReport,
    ) {
        self.operation_counts[class as usize].fetch_add(1, Ordering::Relaxed);
        self.block_read_counts[class as usize].fetch_add(report.blocks_read, Ordering::Relaxed);
        self.byte_read_counts[class as usize].fetch_add(report.bytes_read, Ordering::Relaxed);
        self.adjacency_blocks_considered
            .fetch_add(report.blocks_considered, Ordering::Relaxed);
        self.adjacency_blocks_read
            .fetch_add(report.blocks_read, Ordering::Relaxed);
        self.adjacency_bytes_read
            .fetch_add(report.bytes_read, Ordering::Relaxed);
        self.adjacency_records_decoded
            .fetch_add(report.records_decoded, Ordering::Relaxed);
        self.adjacency_sparse_blocks_read
            .fetch_add(report.sparse_blocks_read, Ordering::Relaxed);
        self.adjacency_dense_blocks_read
            .fetch_add(report.dense_blocks_read, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> GraphIndexReadMetricsSnapshot {
        let mut operation_counts = [0; PersistentGraphIndexClass::ALL.len()];
        let mut block_read_counts = [0; PersistentGraphIndexClass::ALL.len()];
        let mut byte_read_counts = [0; PersistentGraphIndexClass::ALL.len()];
        for class in PersistentGraphIndexClass::ALL {
            operation_counts[class as usize] =
                self.operation_counts[class as usize].load(Ordering::Relaxed);
            block_read_counts[class as usize] =
                self.block_read_counts[class as usize].load(Ordering::Relaxed);
            byte_read_counts[class as usize] =
                self.byte_read_counts[class as usize].load(Ordering::Relaxed);
        }
        GraphIndexReadMetricsSnapshot {
            operation_counts,
            block_read_counts,
            byte_read_counts,
            property_blocks_considered: self.property_blocks_considered.load(Ordering::Relaxed),
            property_blocks_pruned: self.property_blocks_pruned.load(Ordering::Relaxed),
            property_blocks_read: self.property_blocks_read.load(Ordering::Relaxed),
            property_bytes_read: self.property_bytes_read.load(Ordering::Relaxed),
            property_entries_decoded: self.property_entries_decoded.load(Ordering::Relaxed),
            property_candidates_returned: self.property_candidates_returned.load(Ordering::Relaxed),
            adjacency_blocks_considered: self.adjacency_blocks_considered.load(Ordering::Relaxed),
            adjacency_blocks_read: self.adjacency_blocks_read.load(Ordering::Relaxed),
            adjacency_bytes_read: self.adjacency_bytes_read.load(Ordering::Relaxed),
            adjacency_records_decoded: self.adjacency_records_decoded.load(Ordering::Relaxed),
            adjacency_sparse_blocks_read: self.adjacency_sparse_blocks_read.load(Ordering::Relaxed),
            adjacency_dense_blocks_read: self.adjacency_dense_blocks_read.load(Ordering::Relaxed),
        }
    }
}
