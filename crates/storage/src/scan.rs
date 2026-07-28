use skein_core::{LabelId, RelTypeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPruningStrategy {
    FullLabelScan,
    Empty,
    IdEq,
    IdIn,
    IdRange,
    PropertyEq { property: String },
    PropertyNotEq { property: String },
    PropertyMissingOrNull { property: String },
    PropertyExists { property: String },
    PropertyDefaultIfNullEq { property: String },
    PropertyDefaultIfNullNotEq { property: String },
    PropertyIn { property: String },
    PropertyRange { property: String },
    OrUnion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPruningTargetKind {
    Node,
    Relationship,
}

impl ScanPruningTargetKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPruningReport {
    pub target_kind: ScanPruningTargetKind,
    pub label_id: Option<LabelId>,
    pub rel_type_id: Option<RelTypeId>,
    pub strategy: ScanPruningStrategy,
    pub pruned: bool,
    pub exact_empty: bool,
    pub candidate_count_before_pruning: usize,
    pub pruned_candidate_count: usize,
    pub candidate_count_before_filter: usize,
    pub output_count: usize,
    pub filtered_out_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_kind_has_stable_storage_name() {
        assert_eq!(ScanPruningTargetKind::Node.as_str(), "node");
        assert_eq!(ScanPruningTargetKind::Relationship.as_str(), "relationship");
    }
}
