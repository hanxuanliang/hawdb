use super::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufWriter, Lines};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static NEXT_REFRESH_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerStatisticsRefreshOptions {
    pub memory_budget_bytes: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub max_input_records: u64,
    pub max_generated_facts: u64,
    pub max_path_expansions: u64,
    pub spill_directory: PathBuf,
}

impl OptimizerStatisticsRefreshOptions {
    fn validate(&self) -> Result<()> {
        if self.memory_budget_bytes < 4096 {
            return Err(SkeinError::Semantic(
                "optimizer statistics refresh memory_budget_bytes must be at least 4096"
                    .to_string(),
            ));
        }
        for (name, value) in [
            ("max_spill_bytes", self.max_spill_bytes),
            ("max_spill_runs", self.max_spill_runs as u64),
            ("max_input_records", self.max_input_records),
            ("max_generated_facts", self.max_generated_facts),
            ("max_path_expansions", self.max_path_expansions),
        ] {
            if value == 0 {
                return Err(SkeinError::Semantic(format!(
                    "optimizer statistics refresh {name} must be greater than zero"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerStatisticsRefreshReport {
    pub source_commit_epoch: u64,
    pub node_records_read: u64,
    pub relationship_records_read: u64,
    pub path_expansions: u64,
    pub generated_facts: u64,
    pub spill_run_count: usize,
    pub spilled_bytes: u64,
    pub peak_buffer_bytes: usize,
    pub output_statistics_bytes: usize,
    pub property_group_count: usize,
    pub relationship_property_group_count: usize,
    pub path_group_count: usize,
    pub bounded_path_group_count: usize,
    pub checkpoint_persisted: bool,
}

impl OptimizerStatisticsRefreshReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-optimizer-statistics-refresh-v1",
            "protocol_version": 1,
            "source_commit_epoch": self.source_commit_epoch,
            "node_records_read": self.node_records_read,
            "relationship_records_read": self.relationship_records_read,
            "path_expansions": self.path_expansions,
            "generated_facts": self.generated_facts,
            "spill_run_count": self.spill_run_count,
            "spilled_bytes": self.spilled_bytes,
            "peak_buffer_bytes": self.peak_buffer_bytes,
            "output_statistics_bytes": self.output_statistics_bytes,
            "property_group_count": self.property_group_count,
            "relationship_property_group_count": self.relationship_property_group_count,
            "path_group_count": self.path_group_count,
            "bounded_path_group_count": self.bounded_path_group_count,
            "checkpoint_persisted": self.checkpoint_persisted,
        })
    }
}

impl GraphStore {
    pub fn refresh_optimizer_statistics_external(
        &mut self,
        options: &OptimizerStatisticsRefreshOptions,
    ) -> Result<OptimizerStatisticsRefreshReport> {
        options.validate()?;
        if !self.canonical_base_out_of_core {
            return Err(SkeinError::Execution(
                "external optimizer statistics refresh requires out-of-core storage".to_string(),
            ));
        }
        if self.durable.is_none() {
            return Err(SkeinError::Execution(
                "external optimizer statistics refresh requires durable storage".to_string(),
            ));
        }

        let source_commit_epoch = self.commit_epoch;
        let spill = RefreshSpillDirectory::create(&options.spill_directory)?;
        let mut writer = StatsRunWriter::new(spill.path(), options);
        let mut work = RefreshWork::new(options);

        self.try_visit_nodes_owned(None, |node| {
            work.read_node()?;
            for label in &node.labels {
                for (property, value) in &node.properties {
                    writer.push(StatsRecord::NodeProperty {
                        label: *label,
                        property: property.clone(),
                        value: value.clone(),
                    })?;
                }
            }
            Ok(GraphScanControl::Continue)
        })?;

        self.try_visit_relationships_owned(None, |relationship| {
            work.read_relationship()?;
            writer.push(StatsRecord::RelSource {
                rel_type: relationship.rel_type,
                node: relationship.source,
            })?;
            writer.push(StatsRecord::RelTarget {
                rel_type: relationship.rel_type,
                node: relationship.target,
            })?;
            for (property, value) in &relationship.properties {
                writer.push(StatsRecord::RelProperty {
                    rel_type: relationship.rel_type,
                    property: property.clone(),
                    value: value.clone(),
                })?;
            }
            let source = self.node_owned(relationship.source)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing source {}",
                    relationship.id.0, relationship.source.0
                ))
            })?;
            work.read_node()?;
            let target = self.node_owned(relationship.target)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing target {}",
                    relationship.id.0, relationship.target.0
                ))
            })?;
            work.read_node()?;
            for source_label in &source.labels {
                for target_label in &target.labels {
                    writer.push(StatsRecord::PathCount {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                    })?;
                    writer.push(StatsRecord::PathSource {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                        node: relationship.source,
                    })?;
                    writer.push(StatsRecord::PathTarget {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                        node: relationship.target,
                    })?;
                }
            }
            Ok(GraphScanControl::Continue)
        })?;

        let rel_types = self
            .basic_statistics()
            .rel_type_counts
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.try_visit_nodes_owned(None, |source| {
            work.read_node()?;
            for source_label in &source.labels {
                for rel_type in &rel_types {
                    collect_bounded_path_facts(
                        self,
                        source.id,
                        1,
                        BoundedPathSpec {
                            root_source: source.id,
                            source_label: *source_label,
                            rel_type: *rel_type,
                        },
                        &mut writer,
                        &mut work,
                    )?;
                }
            }
            Ok(GraphScanControl::Continue)
        })?;

        let basic = self.basic_statistics();
        let (statistics, merge_report) = writer.finish(graph_statistics_from_basic(basic, true))?;
        if source_commit_epoch != self.commit_epoch {
            return Err(SkeinError::Execution(
                "optimizer statistics refresh source epoch changed before publication".to_string(),
            ));
        }
        self.checkpoint_statistics = statistics;

        Ok(OptimizerStatisticsRefreshReport {
            source_commit_epoch,
            node_records_read: work.node_records_read,
            relationship_records_read: work.relationship_records_read,
            path_expansions: work.path_expansions,
            generated_facts: merge_report.generated_facts,
            spill_run_count: merge_report.spill_run_count,
            spilled_bytes: merge_report.spilled_bytes,
            peak_buffer_bytes: merge_report.peak_buffer_bytes,
            output_statistics_bytes: merge_report.output_statistics_bytes,
            property_group_count: self.checkpoint_statistics.property_distinct_counts.len(),
            relationship_property_group_count: self
                .checkpoint_statistics
                .rel_property_distinct_counts
                .len(),
            path_group_count: self.checkpoint_statistics.path_counts.len(),
            bounded_path_group_count: self.checkpoint_statistics.bounded_path_counts.len(),
            checkpoint_persisted: false,
        })
    }

    pub(crate) fn replace_checkpoint_statistics(
        &mut self,
        statistics: GraphStatistics,
    ) -> GraphStatistics {
        std::mem::replace(&mut self.checkpoint_statistics, statistics)
    }

    pub(crate) fn checkpoint_statistics_snapshot(&self) -> GraphStatistics {
        self.checkpoint_statistics.clone()
    }
}

#[derive(Clone, Copy)]
struct BoundedPathSpec {
    root_source: NodeId,
    source_label: LabelId,
    rel_type: RelTypeId,
}

fn collect_bounded_path_facts(
    store: &GraphStore,
    current: NodeId,
    hop: usize,
    spec: BoundedPathSpec,
    writer: &mut StatsRunWriter<'_>,
    work: &mut RefreshWork<'_>,
) -> Result<()> {
    if hop > MAX_BOUNDED_PATH_STAT_HOPS {
        return Ok(());
    }
    store.try_visit_adjacent_relationships_owned(
        current,
        Some(spec.rel_type),
        AdjacencyDirection::Outgoing,
        |relationship| {
            work.read_relationship()?;
            work.expand_path()?;
            let target = store.node_owned(relationship.target)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing target {}",
                    relationship.id.0, relationship.target.0
                ))
            })?;
            work.read_node()?;
            for target_label in &target.labels {
                writer.push(StatsRecord::BoundedPathCount {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                })?;
                writer.push(StatsRecord::BoundedPathSource {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                    node: spec.root_source,
                })?;
                writer.push(StatsRecord::BoundedPathTarget {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                    node: relationship.target,
                })?;
            }
            collect_bounded_path_facts(store, relationship.target, hop + 1, spec, writer, work)?;
            Ok(GraphScanControl::Continue)
        },
    )?;
    Ok(())
}

struct RefreshWork<'a> {
    options: &'a OptimizerStatisticsRefreshOptions,
    node_records_read: u64,
    relationship_records_read: u64,
    path_expansions: u64,
}

impl<'a> RefreshWork<'a> {
    fn new(options: &'a OptimizerStatisticsRefreshOptions) -> Self {
        Self {
            options,
            node_records_read: 0,
            relationship_records_read: 0,
            path_expansions: 0,
        }
    }

    fn read_node(&mut self) -> Result<()> {
        self.node_records_read = self.node_records_read.saturating_add(1);
        self.check_input_budget()
    }

    fn read_relationship(&mut self) -> Result<()> {
        self.relationship_records_read = self.relationship_records_read.saturating_add(1);
        self.check_input_budget()
    }

    fn expand_path(&mut self) -> Result<()> {
        self.path_expansions = self.path_expansions.saturating_add(1);
        if self.path_expansions > self.options.max_path_expansions {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_path_expansions {}",
                self.options.max_path_expansions
            )));
        }
        Ok(())
    }

    fn check_input_budget(&self) -> Result<()> {
        let input_records = self
            .node_records_read
            .saturating_add(self.relationship_records_read);
        if input_records > self.options.max_input_records {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_input_records {}",
                self.options.max_input_records
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum StatsRecord {
    NodeProperty {
        label: LabelId,
        property: String,
        value: Value,
    },
    RelProperty {
        rel_type: RelTypeId,
        property: String,
        value: Value,
    },
    RelSource {
        rel_type: RelTypeId,
        node: NodeId,
    },
    RelTarget {
        rel_type: RelTypeId,
        node: NodeId,
    },
    PathCount {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
    },
    PathSource {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        node: NodeId,
    },
    PathTarget {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        node: NodeId,
    },
    BoundedPathCount {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
    },
    BoundedPathSource {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
        node: NodeId,
    },
    BoundedPathTarget {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
        node: NodeId,
    },
}

impl StatsRecord {
    fn estimated_bytes(&self) -> usize {
        let fixed = 64usize;
        match self {
            Self::NodeProperty {
                property, value, ..
            }
            | Self::RelProperty {
                property, value, ..
            } => fixed
                .saturating_add(property.len())
                .saturating_add(encode_value(value).len()),
            _ => fixed,
        }
    }

    fn encode(&self) -> String {
        match self {
            Self::NodeProperty {
                label,
                property,
                value,
            } => format!(
                "np\t{}\t{}\t{}",
                label.0,
                encode_string(property),
                encode_string(&encode_value(value))
            ),
            Self::RelProperty {
                rel_type,
                property,
                value,
            } => format!(
                "rp\t{}\t{}\t{}",
                rel_type.0,
                encode_string(property),
                encode_string(&encode_value(value))
            ),
            Self::RelSource { rel_type, node } => {
                format!("rs\t{}\t{}", rel_type.0, node.0)
            }
            Self::RelTarget { rel_type, node } => {
                format!("rt\t{}\t{}", rel_type.0, node.0)
            }
            Self::PathCount {
                source_label,
                rel_type,
                target_label,
            } => format!("pc\t{}\t{}\t{}", source_label.0, rel_type.0, target_label.0),
            Self::PathSource {
                source_label,
                rel_type,
                target_label,
                node,
            } => format!(
                "ps\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, node.0
            ),
            Self::PathTarget {
                source_label,
                rel_type,
                target_label,
                node,
            } => format!(
                "pt\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, node.0
            ),
            Self::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            } => format!(
                "bc\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop
            ),
            Self::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            } => format!(
                "bs\t{}\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop, node.0
            ),
            Self::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            } => format!(
                "bt\t{}\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop, node.0
            ),
        }
    }

    fn decode(line: &str) -> Result<Self> {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["np", label, property, value] => Ok(Self::NodeProperty {
                label: LabelId(parse_u32_field(label, "label id")?),
                property: decode_string(property)?,
                value: decode_value(&decode_string(value)?)?,
            }),
            ["rp", rel_type, property, value] => Ok(Self::RelProperty {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                property: decode_string(property)?,
                value: decode_value(&decode_string(value)?)?,
            }),
            ["rs", rel_type, node] => Ok(Self::RelSource {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["rt", rel_type, node] => Ok(Self::RelTarget {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["pc", source_label, rel_type, target_label] => Ok(Self::PathCount {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
            }),
            ["ps", source_label, rel_type, target_label, node] => Ok(Self::PathSource {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["pt", source_label, rel_type, target_label, node] => Ok(Self::PathTarget {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["bc", source_label, rel_type, target_label, hop] => Ok(Self::BoundedPathCount {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                hop: parse_usize_field(hop, "path hop")?,
            }),
            ["bs", source_label, rel_type, target_label, hop, node] => {
                Ok(Self::BoundedPathSource {
                    source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                    rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                    target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                    hop: parse_usize_field(hop, "path hop")?,
                    node: NodeId(parse_u64_field(node, "node id")?),
                })
            }
            ["bt", source_label, rel_type, target_label, hop, node] => {
                Ok(Self::BoundedPathTarget {
                    source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                    rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                    target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                    hop: parse_usize_field(hop, "path hop")?,
                    node: NodeId(parse_u64_field(node, "node id")?),
                })
            }
            _ => Err(SkeinError::Storage(
                "invalid optimizer statistics spill record".to_string(),
            )),
        }
    }
}

fn parse_u32_field(raw: &str, name: &str) -> Result<u32> {
    raw.parse::<u32>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

fn parse_u64_field(raw: &str, name: &str) -> Result<u64> {
    raw.parse::<u64>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

fn parse_usize_field(raw: &str, name: &str) -> Result<usize> {
    raw.parse::<usize>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

struct StatsRunWriter<'a> {
    directory: &'a Path,
    options: &'a OptimizerStatisticsRefreshOptions,
    chunk: Vec<StatsRecord>,
    chunk_bytes: usize,
    peak_buffer_bytes: usize,
    generated_facts: u64,
    spilled_bytes: u64,
    runs: Vec<PathBuf>,
}

impl<'a> StatsRunWriter<'a> {
    fn new(directory: &'a Path, options: &'a OptimizerStatisticsRefreshOptions) -> Self {
        Self {
            directory,
            options,
            chunk: Vec::new(),
            chunk_bytes: 0,
            peak_buffer_bytes: 0,
            generated_facts: 0,
            spilled_bytes: 0,
            runs: Vec::new(),
        }
    }

    fn push(&mut self, record: StatsRecord) -> Result<()> {
        self.generated_facts = self.generated_facts.saturating_add(1);
        if self.generated_facts > self.options.max_generated_facts {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_generated_facts {}",
                self.options.max_generated_facts
            )));
        }
        let record_bytes = record.estimated_bytes();
        if record_bytes > self.options.memory_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics fact uses {record_bytes} bytes, exceeding memory_budget_bytes {}",
                self.options.memory_budget_bytes
            )));
        }
        if !self.chunk.is_empty()
            && self.chunk_bytes.saturating_add(record_bytes) > self.options.memory_budget_bytes
        {
            self.flush()?;
        }
        self.chunk.push(record);
        self.chunk_bytes = self.chunk_bytes.saturating_add(record_bytes);
        self.peak_buffer_bytes = self.peak_buffer_bytes.max(self.chunk_bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        if self.runs.len() == self.options.max_spill_runs {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_spill_runs {}",
                self.options.max_spill_runs
            )));
        }
        self.chunk.sort_unstable();
        let path = self
            .directory
            .join(format!("run.{:08}.skein", self.runs.len()));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to create optimizer statistics spill run: {error}"
                ))
            })?;
        let mut output = BufWriter::new(file);
        let mut run_bytes = 0u64;
        for record in &self.chunk {
            let encoded = record.encode();
            let encoded_bytes = encoded.len().saturating_add(1) as u64;
            run_bytes = run_bytes.saturating_add(encoded_bytes);
            if self.spilled_bytes.saturating_add(run_bytes) > self.options.max_spill_bytes {
                return Err(SkeinError::Execution(format!(
                    "optimizer statistics refresh exceeded max_spill_bytes {}",
                    self.options.max_spill_bytes
                )));
            }
            output.write_all(encoded.as_bytes()).map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to write optimizer statistics spill run: {error}"
                ))
            })?;
            output.write_all(b"\n").map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to write optimizer statistics spill run: {error}"
                ))
            })?;
        }
        output.flush().map_err(|error| {
            SkeinError::Storage(format!(
                "failed to flush optimizer statistics spill run: {error}"
            ))
        })?;
        self.spilled_bytes = self.spilled_bytes.saturating_add(run_bytes);
        self.runs.push(path);
        self.chunk.clear();
        self.chunk_bytes = 0;
        Ok(())
    }

    fn finish(
        mut self,
        statistics: GraphStatistics,
    ) -> Result<(GraphStatistics, StatsMergeReport)> {
        self.flush()?;
        let mut readers = Vec::with_capacity(self.runs.len());
        for path in &self.runs {
            let file = File::open(path).map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to open optimizer statistics spill run: {error}"
                ))
            })?;
            readers.push(BufReader::new(file).lines());
        }
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = read_next_record(reader)? {
                heap.push(Reverse((record, run)));
            }
        }
        let mut accumulator = StatsAccumulator::new(statistics, self.options.memory_budget_bytes);
        while let Some(Reverse((record, run))) = heap.pop() {
            accumulator.consume(record)?;
            if let Some(next) = read_next_record(&mut readers[run])? {
                heap.push(Reverse((next, run)));
            }
        }
        let (statistics, output_statistics_bytes) = accumulator.finish()?;
        Ok((
            statistics,
            StatsMergeReport {
                generated_facts: self.generated_facts,
                spill_run_count: self.runs.len(),
                spilled_bytes: self.spilled_bytes,
                peak_buffer_bytes: self
                    .peak_buffer_bytes
                    .max(accumulator_peak_bytes(output_statistics_bytes)),
                output_statistics_bytes,
            },
        ))
    }
}

fn accumulator_peak_bytes(output_statistics_bytes: usize) -> usize {
    output_statistics_bytes
}

fn read_next_record(lines: &mut Lines<BufReader<File>>) -> Result<Option<StatsRecord>> {
    let Some(line) = lines.next() else {
        return Ok(None);
    };
    let line = line.map_err(|error| {
        SkeinError::Storage(format!(
            "failed to read optimizer statistics spill run: {error}"
        ))
    })?;
    StatsRecord::decode(&line).map(Some)
}

struct StatsMergeReport {
    generated_facts: u64,
    spill_run_count: usize,
    spilled_bytes: u64,
    peak_buffer_bytes: usize,
    output_statistics_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PropertyGroupKey {
    Node(LabelId, String),
    Relationship(RelTypeId, String),
}

struct PropertyGroup {
    key: PropertyGroupKey,
    last_value: Option<Value>,
    distinct_count: u64,
    samples: BinaryHeap<(u64, Value)>,
    sample_bytes: usize,
}

struct StatsAccumulator {
    statistics: GraphStatistics,
    memory_budget_bytes: usize,
    output_statistics_bytes: usize,
    current_property: Option<PropertyGroup>,
    last_distinct_record: Option<StatsRecord>,
}

impl StatsAccumulator {
    fn new(statistics: GraphStatistics, memory_budget_bytes: usize) -> Self {
        Self {
            statistics,
            memory_budget_bytes,
            output_statistics_bytes: 0,
            current_property: None,
            last_distinct_record: None,
        }
    }

    fn consume(&mut self, record: StatsRecord) -> Result<()> {
        match record {
            StatsRecord::NodeProperty {
                label,
                property,
                value,
            } => self.consume_property(PropertyGroupKey::Node(label, property), value),
            StatsRecord::RelProperty {
                rel_type,
                property,
                value,
            } => self.consume_property(PropertyGroupKey::Relationship(rel_type, property), value),
            record => {
                self.finish_property_group()?;
                self.consume_non_property(record)
            }
        }
    }

    fn consume_property(&mut self, key: PropertyGroupKey, value: Value) -> Result<()> {
        if self
            .current_property
            .as_ref()
            .is_some_and(|group| group.key != key)
        {
            self.finish_property_group()?;
        }
        let output_statistics_bytes = self.output_statistics_bytes;
        let memory_budget_bytes = self.memory_budget_bytes;
        let group = self.current_property.get_or_insert_with(|| PropertyGroup {
            key,
            last_value: None,
            distinct_count: 0,
            samples: BinaryHeap::new(),
            sample_bytes: 0,
        });
        if group.last_value.as_ref() == Some(&value) {
            return Ok(());
        }
        group.last_value = Some(value.clone());
        group.distinct_count = group.distinct_count.saturating_add(1);
        let encoded_bytes = encode_value(&value).len().saturating_add(32);
        let hash = stable_value_hash(&value);
        if group.samples.len() < MAX_PROPERTY_HISTOGRAM_VALUES {
            ensure_statistics_memory(
                output_statistics_bytes,
                group.sample_bytes.saturating_add(encoded_bytes),
                memory_budget_bytes,
            )?;
            group.samples.push((hash, value));
            group.sample_bytes = group.sample_bytes.saturating_add(encoded_bytes);
        } else if group
            .samples
            .peek()
            .is_some_and(|candidate| hash < candidate.0)
        {
            let removed = group.samples.pop().expect("sample heap is non-empty");
            let removed_bytes = encode_value(&removed.1).len().saturating_add(32);
            let next_sample_bytes = group
                .sample_bytes
                .saturating_sub(removed_bytes)
                .saturating_add(encoded_bytes);
            ensure_statistics_memory(
                output_statistics_bytes,
                next_sample_bytes,
                memory_budget_bytes,
            )?;
            group.samples.push((hash, value));
            group.sample_bytes = next_sample_bytes;
        }
        Ok(())
    }

    fn consume_non_property(&mut self, record: StatsRecord) -> Result<()> {
        let duplicate = self.last_distinct_record.as_ref() == Some(&record);
        match &record {
            StatsRecord::RelSource { rel_type, .. } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.rel_type_source_counts,
                    *rel_type,
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::RelTarget { rel_type, .. } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.rel_type_target_counts,
                    *rel_type,
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathCount {
                source_label,
                rel_type,
                target_label,
            } => {
                reserve_counter_entry(
                    &mut self.statistics.path_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathSource {
                source_label,
                rel_type,
                target_label,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.path_source_distinct_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathTarget {
                source_label,
                rel_type,
                target_label,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.path_target_distinct_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            } => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_source_distinct_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_target_distinct_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            _ => {}
        }
        self.last_distinct_record = Some(record);
        Ok(())
    }

    fn finish_property_group(&mut self) -> Result<()> {
        let Some(group) = self.current_property.take() else {
            return Ok(());
        };
        let distinct_count = usize::try_from(group.distinct_count).unwrap_or(usize::MAX);
        let sample_limit = adaptive_histogram_sample_limit(distinct_count);
        let mut samples = group
            .samples
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        if samples.len() > sample_limit {
            let len = samples.len();
            samples = (0..sample_limit)
                .map(|sample_index| {
                    let value_index = sample_index * (len - 1) / (sample_limit - 1);
                    samples[value_index].clone()
                })
                .collect();
        }
        let sampled = distinct_count > sample_limit;
        let histogram_bytes = samples.iter().fold(0usize, |total, value| {
            total.saturating_add(encode_value(value).len().saturating_add(32))
        });
        let key_bytes = match &group.key {
            PropertyGroupKey::Node(_, property) | PropertyGroupKey::Relationship(_, property) => {
                property.len().saturating_add(96)
            }
        };
        self.reserve_output(key_bytes.saturating_add(histogram_bytes))?;
        match group.key {
            PropertyGroupKey::Node(label, property) => {
                let key = (label, property);
                self.statistics
                    .property_distinct_counts
                    .insert(key.clone(), group.distinct_count);
                self.statistics
                    .property_histograms
                    .insert(key.clone(), samples);
                self.statistics
                    .sampled_property_histograms
                    .insert(key, sampled);
            }
            PropertyGroupKey::Relationship(rel_type, property) => {
                let key = (rel_type, property);
                self.statistics
                    .rel_property_distinct_counts
                    .insert(key.clone(), group.distinct_count);
                self.statistics
                    .rel_property_histograms
                    .insert(key.clone(), samples);
                self.statistics
                    .sampled_rel_property_histograms
                    .insert(key, sampled);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(GraphStatistics, usize)> {
        self.finish_property_group()?;
        Ok((self.statistics, self.output_statistics_bytes))
    }

    fn ensure_memory(&self, temporary_bytes: usize) -> Result<()> {
        if self.output_statistics_bytes.saturating_add(temporary_bytes) > self.memory_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh output state exceeds memory_budget_bytes {}",
                self.memory_budget_bytes
            )));
        }
        Ok(())
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<()> {
        self.ensure_memory(bytes)?;
        self.output_statistics_bytes = self.output_statistics_bytes.saturating_add(bytes);
        Ok(())
    }
}

fn reserve_counter_entry<K: Ord + Clone>(
    counts: &mut BTreeMap<K, u64>,
    key: K,
    output_bytes: &mut usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    if let Some(count) = counts.get_mut(&key) {
        *count = count.saturating_add(1);
        return Ok(());
    }
    let entry_bytes = std::mem::size_of::<K>().saturating_add(48);
    if output_bytes.saturating_add(entry_bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "optimizer statistics refresh output state exceeds memory_budget_bytes {memory_budget_bytes}"
        )));
    }
    counts.insert(key, 1);
    *output_bytes = output_bytes.saturating_add(entry_bytes);
    Ok(())
}

fn ensure_statistics_memory(
    output_bytes: usize,
    temporary_bytes: usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    if output_bytes.saturating_add(temporary_bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "optimizer statistics refresh output state exceeds memory_budget_bytes {memory_budget_bytes}"
        )));
    }
    Ok(())
}

fn stable_value_hash(value: &Value) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in encode_value(value).as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

struct RefreshSpillDirectory {
    path: PathBuf,
}

impl RefreshSpillDirectory {
    fn create(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).map_err(|error| {
            SkeinError::Storage(format!(
                "failed to create optimizer statistics spill root: {error}"
            ))
        })?;
        let id = NEXT_REFRESH_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = root.join(format!(
            "skein-statistics-refresh-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).map_err(|error| {
            SkeinError::Storage(format!(
                "failed to create optimizer statistics spill directory: {error}"
            ))
        })?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RefreshSpillDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
