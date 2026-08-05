use super::{latency_percentiles, LatencyPercentiles};
use crate::production_graph::validate_production_identity_for_current_target;
use sha2::{Digest, Sha256};
use skein::{
    CompressedVectorSearchMode, NowledgeMemSearchCandidateRequest, ProcessMemoryProfile,
    ProcessMemorySnapshot, ProductionEvidenceBinding, ProductionQualificationIdentity,
    SearchAccessControlContext, SearchIndex, SearchLexicalFeasibilityCoverage,
    SearchLexicalFeasibilityMetrics, SearchLexicalProductionQualificationReport, SearchMode,
    SearchOutOfCoreConfig, SearchOutOfCoreMetrics, SearchOutOfCoreOutput, SearchOutOfCoreReader,
    SearchProjectionDelta, SearchProjectionQualificationIdentity, SearchQueryOptions,
    SearchResultSet, SearchTopKScoreParity,
};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

pub const PRODUCTION_SEARCH_OUT_OF_CORE_QUALIFICATION_PROTOCOL: &str =
    "skein-production-search-out-of-core-qualification-v1";
const OUT_OF_CORE_MANIFEST_FILE: &str = "search_projection.out_of_core.manifest.skein";
const MINIMUM_LIFECYCLE_REPLICA_COUNT: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductionSearchCaseKind {
    SelectiveIdentifier,
    CjkText,
    CommonTerm,
    NoHit,
    MetadataFilter,
    AclFilter,
    Vector,
    Hybrid,
}

impl ProductionSearchCaseKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SelectiveIdentifier => "selective_identifier",
            Self::CjkText => "cjk_text",
            Self::CommonTerm => "common_term",
            Self::NoHit => "no_hit",
            Self::MetadataFilter => "metadata_filter",
            Self::AclFilter => "acl_filter",
            Self::Vector => "vector",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionSearchQueryCase {
    pub name: String,
    pub kind: ProductionSearchCaseKind,
    pub request: NowledgeMemSearchCandidateRequest,
    pub access_control: Option<SearchAccessControlContext>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionSearchLifecycleConfig {
    /// Each path must be a disposable, writable copy of search_projection_path.
    pub replica_paths: Vec<PathBuf>,
    /// This disposable copy is intentionally corrupted and cannot be reused.
    pub corruption_replica_path: PathBuf,
    pub delta: SearchProjectionDelta,
    pub expected_upsert_document_id: String,
    pub expected_deleted_document_id: String,
    pub upsert_verification: ProductionSearchQueryCase,
    pub delete_verification: ProductionSearchQueryCase,
    pub mixed_load_probe_runs: usize,
    pub reference_update_p95_micros: u64,
    pub reference_checkpoint_p95_micros: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionSearchOutOfCoreQualificationConfig {
    pub search_projection_path: PathBuf,
    pub out_of_core_config: SearchOutOfCoreConfig,
    pub search_memory_budget_bytes: u64,
    pub warmup_runs: usize,
    pub measurement_runs: usize,
    pub query_cases: Vec<ProductionSearchQueryCase>,
    pub lifecycle: ProductionSearchLifecycleConfig,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
}

impl ProductionSearchOutOfCoreQualificationConfig {
    fn validate(&self) -> Result<(), ProductionSearchQualificationError> {
        if !self.search_projection_path.is_dir() {
            return Err(ProductionSearchQualificationError::new(
                "production search qualification requires an existing projection directory",
            ));
        }
        if self.search_memory_budget_bytes == 0 {
            return Err(ProductionSearchQualificationError::new(
                "production search qualification requires a non-zero search memory budget",
            ));
        }
        if self.measurement_runs == 0 {
            return Err(ProductionSearchQualificationError::new(
                "production search qualification measurement_runs must be greater than zero",
            ));
        }
        validate_production_identity_for_current_target(
            &self.evidence_binding,
            &self.expected_identity,
        )
        .map_err(ProductionSearchQualificationError::from_error)?;
        validate_query_cases(&self.query_cases, &self.expected_identity)?;
        self.lifecycle
            .validate(&self.search_projection_path, &self.expected_identity)?;
        Ok(())
    }
}

impl ProductionSearchLifecycleConfig {
    fn validate(
        &self,
        source_path: &Path,
        expected_identity: &ProductionQualificationIdentity,
    ) -> Result<(), ProductionSearchQualificationError> {
        if self.replica_paths.len() < MINIMUM_LIFECYCLE_REPLICA_COUNT {
            return Err(ProductionSearchQualificationError::new(format!(
                "production search qualification requires at least {MINIMUM_LIFECYCLE_REPLICA_COUNT} disposable lifecycle replicas"
            )));
        }
        if self.mixed_load_probe_runs == 0 {
            return Err(ProductionSearchQualificationError::new(
                "production search qualification mixed_load_probe_runs must be greater than zero",
            ));
        }
        if self.reference_update_p95_micros == 0 || self.reference_checkpoint_p95_micros == 0 {
            return Err(ProductionSearchQualificationError::new(
                "production search qualification requires non-zero reference write latency evidence",
            ));
        }
        if self.delta.upserts.is_empty() || self.delta.deletes.is_empty() {
            return Err(ProductionSearchQualificationError::new(
                "production search lifecycle delta requires both upsert and delete operations",
            ));
        }
        if self.delta.source_graph_commit_epoch
            != Some(expected_identity.canonical_graph_commit_epoch)
        {
            return Err(ProductionSearchQualificationError::new(
                "production search lifecycle delta must retain the expected canonical graph epoch",
            ));
        }
        if self.expected_upsert_document_id.trim().is_empty()
            || self.expected_deleted_document_id.trim().is_empty()
            || self.expected_upsert_document_id == self.expected_deleted_document_id
        {
            return Err(ProductionSearchQualificationError::new(
                "production search lifecycle document identities are invalid",
            ));
        }
        let mut paths = BTreeSet::new();
        for path in self
            .replica_paths
            .iter()
            .chain(std::iter::once(&self.corruption_replica_path))
        {
            if path == source_path || !path.is_dir() || !paths.insert(path.clone()) {
                return Err(ProductionSearchQualificationError::new(
                    "production search qualification replicas must be distinct existing directories separate from the source projection",
                ));
            }
        }
        validate_query_case(&self.upsert_verification)?;
        validate_query_case(&self.delete_verification)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionSearchQualificationError {
    message: String,
}

impl ProductionSearchQualificationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_error(error: impl Display) -> Self {
        Self::new(error.to_string())
    }
}

impl Display for ProductionSearchQualificationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProductionSearchQualificationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionSearchQueryEvidence {
    pub name: String,
    pub kind: ProductionSearchCaseKind,
    pub mode: SearchMode,
    pub request_digest: String,
    pub reference_result_digest: String,
    pub out_of_core_result_digest: String,
    pub exact_topk_score_parity: bool,
    pub reference_latency: LatencyPercentiles,
    pub out_of_core_latency: LatencyPercentiles,
}

impl ProductionSearchQueryEvidence {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "kind": self.kind.as_str(),
            "mode": search_mode_name(self.mode),
            "request_digest": self.request_digest,
            "reference_result_digest": self.reference_result_digest,
            "out_of_core_result_digest": self.out_of_core_result_digest,
            "exact_topk_score_parity": self.exact_topk_score_parity,
            "reference_latency": self.reference_latency,
            "out_of_core_latency": self.out_of_core_latency,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionSearchLifecycleReport {
    pub incremental_upsert_delete: bool,
    pub checkpoint_reopen: bool,
    pub stale_generation: bool,
    pub corrupt_artifact_rejected: bool,
    pub mixed_foreground_background: bool,
    pub update_latency: LatencyPercentiles,
    pub checkpoint_latency: LatencyPercentiles,
    pub reopen_latency: LatencyPercentiles,
    pub checkpoint_write_amplification_per_million: u64,
}

impl ProductionSearchLifecycleReport {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "incremental_upsert_delete": self.incremental_upsert_delete,
            "checkpoint_reopen": self.checkpoint_reopen,
            "stale_generation": self.stale_generation,
            "corrupt_artifact_rejected": self.corrupt_artifact_rejected,
            "mixed_foreground_background": self.mixed_foreground_background,
            "update_latency": self.update_latency,
            "checkpoint_latency": self.checkpoint_latency,
            "reopen_latency": self.reopen_latency,
            "checkpoint_write_amplification_per_million": self.checkpoint_write_amplification_per_million,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionSearchOutOfCoreQualificationReport {
    pub qualification: SearchLexicalProductionQualificationReport,
    pub query_evidence: Vec<ProductionSearchQueryEvidence>,
    pub lifecycle: ProductionSearchLifecycleReport,
    pub process_memory: ProcessMemoryProfile,
    pub out_of_core_metrics: SearchOutOfCoreMetrics,
}

impl ProductionSearchOutOfCoreQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_SEARCH_OUT_OF_CORE_QUALIFICATION_PROTOCOL,
            "evidence_kind": "representative_production_search_replica",
            "production_eligible": true,
            "qualification": self.qualification.json(),
            "query_evidence": self.query_evidence.iter().map(ProductionSearchQueryEvidence::json).collect::<Vec<_>>(),
            "lifecycle": self.lifecycle.json(),
            "process_memory": process_memory_json(self.process_memory),
            "out_of_core_metrics": out_of_core_metrics_json(&self.out_of_core_metrics),
        })
    }
}

#[derive(Debug)]
struct QueryRun {
    result_digest: String,
    latency_micros: Vec<u64>,
    metrics: SearchOutOfCoreMetrics,
    selective_posting_bytes_read: u64,
    selective_candidate_postings_visited: u64,
    selective_matching_document_count: usize,
}

pub fn run_production_search_out_of_core_qualification(
    config: ProductionSearchOutOfCoreQualificationConfig,
) -> Result<ProductionSearchOutOfCoreQualificationReport, ProductionSearchQualificationError> {
    config.validate()?;

    // Candidate measurement runs first so loading the full-residency oracle cannot
    // inflate the candidate's process peak RSS.
    let candidate_reader = SearchOutOfCoreReader::open_with_config(
        &config.search_projection_path,
        config.out_of_core_config.clone(),
    )
    .map_err(ProductionSearchQualificationError::from_error)?;
    let projection_identity = candidate_reader.production_qualification_identity();
    if projection_identity.source_graph_commit_epoch
        != Some(config.expected_identity.canonical_graph_commit_epoch)
    {
        return Err(ProductionSearchQualificationError::new(
            "production search projection epoch does not match the expected release identity",
        ));
    }
    let projection_payload_bytes = candidate_reader.projection_payload_bytes();
    let process_start =
        ProcessMemorySnapshot::capture().map_err(ProductionSearchQualificationError::from_error)?;
    for _ in 0..config.warmup_runs {
        for query_case in &config.query_cases {
            execute_out_of_core(&candidate_reader, query_case)?;
        }
    }
    let candidate_runs = config
        .query_cases
        .iter()
        .map(|query_case| {
            run_out_of_core_case(&candidate_reader, query_case, config.measurement_runs)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let process_end =
        ProcessMemorySnapshot::capture().map_err(ProductionSearchQualificationError::from_error)?;
    let process_memory = ProcessMemoryProfile::between(process_start, process_end);
    drop(candidate_reader);

    let reference = SearchIndex::open(&config.search_projection_path)
        .map_err(ProductionSearchQualificationError::from_error)?;
    let reference_runs = config
        .query_cases
        .iter()
        .map(|query_case| run_reference_case(&reference, query_case, config.measurement_runs))
        .collect::<Result<Vec<_>, _>>()?;
    drop(reference);

    let lifecycle = run_lifecycle_probes(
        &config.lifecycle,
        &projection_identity,
        &config.out_of_core_config,
    )?;
    let query_evidence =
        build_query_evidence(&config.query_cases, &reference_runs, &candidate_runs);
    let parity = parity_by_mode(&query_evidence);
    let coverage = coverage_from_evidence(
        &config.query_cases,
        &lifecycle,
        projection_payload_bytes > config.search_memory_budget_bytes,
    );
    let aggregate_metrics =
        candidate_runs
            .iter()
            .fold(SearchOutOfCoreMetrics::default(), |mut aggregate, run| {
                add_out_of_core_metrics(&mut aggregate, &run.metrics);
                aggregate
            });
    let metrics = qualification_metrics(
        &config,
        projection_payload_bytes,
        process_memory,
        &reference_runs,
        &candidate_runs,
        &lifecycle,
        &aggregate_metrics,
    );
    let qualification = SearchLexicalProductionQualificationReport::evaluate_for_production(
        projection_identity,
        config.evidence_binding,
        config.expected_identity,
        parity,
        coverage,
        metrics,
    );

    Ok(ProductionSearchOutOfCoreQualificationReport {
        qualification,
        query_evidence,
        lifecycle,
        process_memory,
        out_of_core_metrics: aggregate_metrics,
    })
}

fn run_out_of_core_case(
    reader: &SearchOutOfCoreReader,
    query_case: &ProductionSearchQueryCase,
    measurement_runs: usize,
) -> Result<QueryRun, ProductionSearchQualificationError> {
    let mut latency_micros = Vec::with_capacity(measurement_runs);
    let mut metrics = SearchOutOfCoreMetrics::default();
    let mut first_digest = None;
    let mut selective_posting_bytes_read = 0;
    let mut selective_candidate_postings_visited = 0;
    let mut selective_matching_document_count = 0;
    for _ in 0..measurement_runs {
        let started = Instant::now();
        let output = execute_out_of_core(reader, query_case)?;
        latency_micros.push(elapsed_micros(started));
        let digest = result_digest(&output.result);
        if first_digest.as_ref().is_some_and(|first| first != &digest) {
            return Err(ProductionSearchQualificationError::new(format!(
                "out-of-core query case {} produced non-deterministic results",
                query_case.name
            )));
        }
        update_selective_metrics(
            query_case,
            &output.result,
            &mut selective_posting_bytes_read,
            &mut selective_candidate_postings_visited,
            &mut selective_matching_document_count,
        );
        add_out_of_core_metrics(&mut metrics, &output.metrics);
        first_digest.get_or_insert(digest);
    }
    Ok(QueryRun {
        result_digest: first_digest.expect("positive measurement_runs produces a digest"),
        latency_micros,
        metrics,
        selective_posting_bytes_read,
        selective_candidate_postings_visited,
        selective_matching_document_count,
    })
}

fn run_reference_case(
    index: &SearchIndex,
    query_case: &ProductionSearchQueryCase,
    measurement_runs: usize,
) -> Result<QueryRun, ProductionSearchQualificationError> {
    let mut latency_micros = Vec::with_capacity(measurement_runs);
    let mut first_digest = None;
    for _ in 0..measurement_runs {
        let started = Instant::now();
        let result = execute_reference(index, query_case)?;
        latency_micros.push(elapsed_micros(started));
        let digest = result_digest(&result);
        if first_digest.as_ref().is_some_and(|first| first != &digest) {
            return Err(ProductionSearchQualificationError::new(format!(
                "reference query case {} produced non-deterministic results",
                query_case.name
            )));
        }
        first_digest.get_or_insert(digest);
    }
    Ok(QueryRun {
        result_digest: first_digest.expect("positive measurement_runs produces a digest"),
        latency_micros,
        metrics: SearchOutOfCoreMetrics::default(),
        selective_posting_bytes_read: 0,
        selective_candidate_postings_visited: 0,
        selective_matching_document_count: 0,
    })
}

fn execute_out_of_core(
    reader: &SearchOutOfCoreReader,
    query_case: &ProductionSearchQueryCase,
) -> Result<SearchOutOfCoreOutput, ProductionSearchQualificationError> {
    let options = query_options(query_case);
    match &query_case.access_control {
        Some(access_control) => reader.search_with_options_access_control(
            &query_case.request.query_text,
            query_case.request.query_embedding.as_deref(),
            query_case.request.mode,
            options,
            access_control.clone(),
        ),
        None => reader.search_with_options(
            &query_case.request.query_text,
            query_case.request.query_embedding.as_deref(),
            query_case.request.mode,
            options,
        ),
    }
    .map_err(ProductionSearchQualificationError::from_error)
}

fn execute_reference(
    index: &SearchIndex,
    query_case: &ProductionSearchQueryCase,
) -> Result<SearchResultSet, ProductionSearchQualificationError> {
    let options = query_options(query_case);
    match &query_case.access_control {
        Some(access_control) => index.try_search_with_options_access_control(
            &query_case.request.query_text,
            query_case.request.query_embedding.as_deref(),
            query_case.request.mode,
            options,
            access_control.clone(),
        ),
        None => index.try_search_with_options(
            &query_case.request.query_text,
            query_case.request.query_embedding.as_deref(),
            query_case.request.mode,
            options,
        ),
    }
    .map_err(ProductionSearchQualificationError::from_error)
}

fn query_options(query_case: &ProductionSearchQueryCase) -> SearchQueryOptions {
    SearchQueryOptions {
        limit: query_case.request.limit,
        offset: query_case.request.offset,
        rank_window: query_case.request.rank_window,
        fusion_weights: query_case.request.fusion_weights,
        metadata_filters: query_case.request.metadata_filters.clone(),
        policy_epoch: query_case
            .access_control
            .as_ref()
            .map(|access_control| access_control.policy_epoch),
    }
}

fn run_lifecycle_probes(
    config: &ProductionSearchLifecycleConfig,
    expected_projection_identity: &SearchProjectionQualificationIdentity,
    out_of_core_config: &SearchOutOfCoreConfig,
) -> Result<ProductionSearchLifecycleReport, ProductionSearchQualificationError> {
    let mut update_micros = Vec::with_capacity(config.replica_paths.len());
    let mut checkpoint_micros = Vec::with_capacity(config.replica_paths.len());
    let mut reopen_micros = Vec::with_capacity(config.replica_paths.len());
    let mut incremental_upsert_delete = true;
    let mut checkpoint_reopen = true;
    let mut stale_generation = true;
    let mut mixed_foreground_background = true;
    let mut checkpoint_write_amplification_per_million = 0;
    let logical_delta_bytes = delta_logical_bytes(&config.delta);

    for path in &config.replica_paths {
        let old_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        require_projection_identity(&old_reader, expected_projection_identity)?;
        let old_generation = old_reader.generation();
        let upsert_before = execute_out_of_core(&old_reader, &config.upsert_verification)?;
        let delete_before = execute_out_of_core(&old_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            !contains_hit(&upsert_before.result, &config.expected_upsert_document_id)
                && contains_hit(&delete_before.result, &config.expected_deleted_document_id);

        let bytes_before = directory_regular_file_bytes(path)?;
        let mut index =
            SearchIndex::open(path).map_err(ProductionSearchQualificationError::from_error)?;
        let update_started = Instant::now();
        index
            .apply_projection_delta(config.delta.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        update_micros.push(elapsed_micros(update_started));

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker_case = config.upsert_verification.clone();
        let worker_runs = config.mixed_load_probe_runs;
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            let mut succeeded = true;
            for _ in 0..worker_runs {
                succeeded &= execute_out_of_core(&old_reader, &worker_case).is_ok();
            }
            succeeded
        });
        barrier.wait();
        let checkpoint_started = Instant::now();
        index
            .checkpoint()
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_micros.push(elapsed_micros(checkpoint_started));
        mixed_foreground_background &= worker.join().map_err(|_| {
            ProductionSearchQualificationError::new(
                "production search mixed-load probe thread panicked",
            )
        })?;
        drop(index);

        let bytes_after = directory_regular_file_bytes(path)?;
        checkpoint_write_amplification_per_million = checkpoint_write_amplification_per_million
            .max(ratio_per_million(
                bytes_after.saturating_sub(bytes_before),
                logical_delta_bytes,
            ));
        let reopen_started = Instant::now();
        let new_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        reopen_micros.push(elapsed_micros(reopen_started));
        stale_generation &= new_reader.generation() > old_generation;
        let upsert_after = execute_out_of_core(&new_reader, &config.upsert_verification)?;
        let delete_after = execute_out_of_core(&new_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            contains_hit(&upsert_after.result, &config.expected_upsert_document_id)
                && !contains_hit(&delete_after.result, &config.expected_deleted_document_id);
        let expected_upsert_digest = result_digest(&upsert_after.result);
        let expected_delete_digest = result_digest(&delete_after.result);
        drop(new_reader);
        let reopened = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_reopen &=
            result_digest(&execute_out_of_core(&reopened, &config.upsert_verification)?.result)
                == expected_upsert_digest
                && result_digest(
                    &execute_out_of_core(&reopened, &config.delete_verification)?.result,
                ) == expected_delete_digest;
    }

    let corrupt_reader = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .map_err(ProductionSearchQualificationError::from_error)?;
    require_projection_identity(&corrupt_reader, expected_projection_identity)?;
    drop(corrupt_reader);
    corrupt_out_of_core_manifest(&config.corruption_replica_path)?;
    let corrupt_artifact_rejected = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .is_err();

    Ok(ProductionSearchLifecycleReport {
        incremental_upsert_delete,
        checkpoint_reopen,
        stale_generation,
        corrupt_artifact_rejected,
        mixed_foreground_background,
        update_latency: latency_percentiles(&update_micros),
        checkpoint_latency: latency_percentiles(&checkpoint_micros),
        reopen_latency: latency_percentiles(&reopen_micros),
        checkpoint_write_amplification_per_million,
    })
}

fn require_projection_identity(
    reader: &SearchOutOfCoreReader,
    expected: &SearchProjectionQualificationIdentity,
) -> Result<(), ProductionSearchQualificationError> {
    if reader.production_qualification_identity() == *expected {
        Ok(())
    } else {
        Err(ProductionSearchQualificationError::new(
            "production search lifecycle replica identity does not match the source projection",
        ))
    }
}

fn corrupt_out_of_core_manifest(path: &Path) -> Result<(), ProductionSearchQualificationError> {
    let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest_path)
        .map_err(ProductionSearchQualificationError::from_error)?;
    let length = file
        .metadata()
        .map_err(ProductionSearchQualificationError::from_error)?
        .len();
    if length == 0 {
        return Err(ProductionSearchQualificationError::new(
            "production search corruption replica has an empty manifest",
        ));
    }
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)
        .map_err(ProductionSearchQualificationError::from_error)?;
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    byte[0] ^= 0xff;
    file.write_all(&byte)
        .and_then(|_| file.sync_all())
        .map_err(ProductionSearchQualificationError::from_error)
}

fn build_query_evidence(
    query_cases: &[ProductionSearchQueryCase],
    reference_runs: &[QueryRun],
    candidate_runs: &[QueryRun],
) -> Vec<ProductionSearchQueryEvidence> {
    query_cases
        .iter()
        .zip(reference_runs)
        .zip(candidate_runs)
        .map(
            |((query_case, reference), candidate)| ProductionSearchQueryEvidence {
                name: query_case.name.clone(),
                kind: query_case.kind,
                mode: query_case.request.mode,
                request_digest: request_digest(query_case),
                reference_result_digest: reference.result_digest.clone(),
                out_of_core_result_digest: candidate.result_digest.clone(),
                exact_topk_score_parity: reference.result_digest == candidate.result_digest,
                reference_latency: latency_percentiles(&reference.latency_micros),
                out_of_core_latency: latency_percentiles(&candidate.latency_micros),
            },
        )
        .collect()
}

fn parity_by_mode(query_evidence: &[ProductionSearchQueryEvidence]) -> SearchTopKScoreParity {
    SearchTopKScoreParity {
        text: mode_has_complete_parity(query_evidence, SearchMode::Text),
        vector: mode_has_complete_parity(query_evidence, SearchMode::Vector),
        hybrid: mode_has_complete_parity(query_evidence, SearchMode::Hybrid),
    }
}

fn mode_has_complete_parity(
    query_evidence: &[ProductionSearchQueryEvidence],
    mode: SearchMode,
) -> bool {
    let mut matching = query_evidence
        .iter()
        .filter(|evidence| evidence.mode == mode);
    matching.clone().next().is_some() && matching.all(|evidence| evidence.exact_topk_score_parity)
}

fn coverage_from_evidence(
    query_cases: &[ProductionSearchQueryCase],
    lifecycle: &ProductionSearchLifecycleReport,
    larger_than_memory: bool,
) -> SearchLexicalFeasibilityCoverage {
    let has = |kind| query_cases.iter().any(|query_case| query_case.kind == kind);
    SearchLexicalFeasibilityCoverage {
        selective_identifier: has(ProductionSearchCaseKind::SelectiveIdentifier),
        cjk_text: has(ProductionSearchCaseKind::CjkText),
        common_term: has(ProductionSearchCaseKind::CommonTerm),
        no_hit: has(ProductionSearchCaseKind::NoHit),
        metadata_filter: has(ProductionSearchCaseKind::MetadataFilter),
        acl_filter: has(ProductionSearchCaseKind::AclFilter),
        hybrid_rrf: has(ProductionSearchCaseKind::Hybrid),
        incremental_upsert_delete: lifecycle.incremental_upsert_delete,
        checkpoint_reopen: lifecycle.checkpoint_reopen,
        corrupt_artifact: lifecycle.corrupt_artifact_rejected,
        stale_manifest: lifecycle.stale_generation,
        mixed_foreground_background: lifecycle.mixed_foreground_background,
        larger_than_memory,
    }
}

#[allow(clippy::too_many_arguments)]
fn qualification_metrics(
    config: &ProductionSearchOutOfCoreQualificationConfig,
    projection_payload_bytes: u64,
    process_memory: ProcessMemoryProfile,
    reference_runs: &[QueryRun],
    candidate_runs: &[QueryRun],
    lifecycle: &ProductionSearchLifecycleReport,
    aggregate: &SearchOutOfCoreMetrics,
) -> SearchLexicalFeasibilityMetrics {
    let selective_reference = latency_for_kind(
        &config.query_cases,
        reference_runs,
        ProductionSearchCaseKind::SelectiveIdentifier,
    );
    let selective_candidate = latency_for_kind(
        &config.query_cases,
        candidate_runs,
        ProductionSearchCaseKind::SelectiveIdentifier,
    );
    let vector_candidate =
        latency_for_mode(&config.query_cases, candidate_runs, SearchMode::Vector);
    let hybrid_candidate =
        latency_for_mode(&config.query_cases, candidate_runs, SearchMode::Hybrid);
    let selective_run = config
        .query_cases
        .iter()
        .position(|query_case| query_case.kind == ProductionSearchCaseKind::SelectiveIdentifier)
        .and_then(|index| candidate_runs.get(index));
    SearchLexicalFeasibilityMetrics {
        canonical_dataset_bytes: projection_payload_bytes,
        storage_memory_budget_bytes: config.search_memory_budget_bytes,
        steady_resident_bytes: process_memory.steady_resident_bytes,
        peak_resident_bytes: process_memory.peak_resident_bytes,
        baseline_selective_text_p50_micros: selective_reference.p50_micros,
        baseline_selective_text_p95_micros: selective_reference.p95_micros,
        baseline_selective_text_p99_micros: selective_reference.p99_micros,
        segmented_selective_text_p50_micros: selective_candidate.p50_micros,
        segmented_selective_text_p95_micros: selective_candidate.p95_micros,
        segmented_selective_text_p99_micros: selective_candidate.p99_micros,
        baseline_throughput_per_second: throughput_per_second(reference_runs),
        segmented_throughput_per_second: throughput_per_second(candidate_runs),
        selective_posting_bytes_read: selective_run
            .map(|run| run.selective_posting_bytes_read)
            .unwrap_or_default(),
        selective_candidate_postings_visited: selective_run
            .map(|run| run.selective_candidate_postings_visited)
            .unwrap_or_default(),
        selective_matching_document_count: selective_run
            .map(|run| run.selective_matching_document_count)
            .unwrap_or_default(),
        process_memory_capabilities: process_memory.capabilities,
        total_page_faults: process_memory.total_page_faults,
        minor_page_faults: process_memory.minor_page_faults,
        major_page_faults: process_memory.major_page_faults,
        metadata_sidecar_bytes_read: aggregate.metadata_segment_bytes_read,
        vector_sidecar_bytes_read: aggregate.vector_segment_bytes_read,
        hydration_bytes: aggregate.hydrated_bytes,
        segmented_vector_p50_micros: vector_candidate.p50_micros,
        segmented_vector_p95_micros: vector_candidate.p95_micros,
        segmented_vector_p99_micros: vector_candidate.p99_micros,
        segmented_hybrid_p50_micros: hybrid_candidate.p50_micros,
        segmented_hybrid_p95_micros: hybrid_candidate.p95_micros,
        segmented_hybrid_p99_micros: hybrid_candidate.p99_micros,
        baseline_update_p95_micros: config.lifecycle.reference_update_p95_micros,
        segmented_update_p95_micros: lifecycle.update_latency.p95_micros,
        baseline_checkpoint_p95_micros: config.lifecycle.reference_checkpoint_p95_micros,
        segmented_checkpoint_p95_micros: lifecycle.checkpoint_latency.p95_micros,
        consolidation_write_amplification_per_million: lifecycle
            .checkpoint_write_amplification_per_million,
        recovery_p95_micros: lifecycle.reopen_latency.p95_micros,
    }
}

fn validate_query_cases(
    query_cases: &[ProductionSearchQueryCase],
    expected_identity: &ProductionQualificationIdentity,
) -> Result<(), ProductionSearchQualificationError> {
    let mut names = BTreeSet::new();
    for query_case in query_cases {
        validate_query_case(query_case)?;
        if !names.insert(query_case.name.clone()) {
            return Err(ProductionSearchQualificationError::new(
                "production search query case names must be unique",
            ));
        }
    }
    let required = [
        ProductionSearchCaseKind::SelectiveIdentifier,
        ProductionSearchCaseKind::CjkText,
        ProductionSearchCaseKind::CommonTerm,
        ProductionSearchCaseKind::NoHit,
        ProductionSearchCaseKind::MetadataFilter,
        ProductionSearchCaseKind::Vector,
        ProductionSearchCaseKind::Hybrid,
    ];
    if required.iter().any(|kind| {
        !query_cases
            .iter()
            .any(|query_case| query_case.kind == *kind)
    }) {
        return Err(ProductionSearchQualificationError::new(
            "production search qualification workload is missing a required query case",
        ));
    }
    let acl_enabled = expected_identity
        .enabled_features
        .iter()
        .any(|feature| feature == "acl");
    if acl_enabled
        && !query_cases
            .iter()
            .any(|query_case| query_case.kind == ProductionSearchCaseKind::AclFilter)
    {
        return Err(ProductionSearchQualificationError::new(
            "production search qualification requires an ACL case when acl is enabled",
        ));
    }
    Ok(())
}

fn validate_query_case(
    query_case: &ProductionSearchQueryCase,
) -> Result<(), ProductionSearchQualificationError> {
    if query_case.name.trim().is_empty() || query_case.request.limit == 0 {
        return Err(ProductionSearchQualificationError::new(
            "production search query cases require a name and non-zero limit",
        ));
    }
    match query_case.kind {
        ProductionSearchCaseKind::SelectiveIdentifier
        | ProductionSearchCaseKind::CjkText
        | ProductionSearchCaseKind::CommonTerm
        | ProductionSearchCaseKind::NoHit
            if query_case.request.mode != SearchMode::Text =>
        {
            return Err(ProductionSearchQualificationError::new(
                "production search lexical cases must use text mode",
            ));
        }
        ProductionSearchCaseKind::Vector if query_case.request.mode != SearchMode::Vector => {
            return Err(ProductionSearchQualificationError::new(
                "production search vector case must use vector mode",
            ));
        }
        ProductionSearchCaseKind::Hybrid if query_case.request.mode != SearchMode::Hybrid => {
            return Err(ProductionSearchQualificationError::new(
                "production search hybrid case must use hybrid mode",
            ));
        }
        ProductionSearchCaseKind::AclFilter if query_case.access_control.is_none() => {
            return Err(ProductionSearchQualificationError::new(
                "production search ACL case requires access-control context",
            ));
        }
        _ => {}
    }
    if matches!(
        query_case.request.mode,
        SearchMode::Vector | SearchMode::Hybrid
    ) && query_case.request.query_embedding.is_none()
    {
        return Err(ProductionSearchQualificationError::new(
            "production search vector and hybrid cases require a query embedding",
        ));
    }
    if query_case.request.compressed_vector_search_mode != CompressedVectorSearchMode::Disabled {
        return Err(ProductionSearchQualificationError::new(
            "production out-of-core parity cases must use canonical scalar vector scoring",
        ));
    }
    Ok(())
}

fn update_selective_metrics(
    query_case: &ProductionSearchQueryCase,
    result: &SearchResultSet,
    posting_bytes_read: &mut u64,
    candidate_postings_visited: &mut u64,
    matching_document_count: &mut usize,
) {
    if query_case.kind != ProductionSearchCaseKind::SelectiveIdentifier {
        return;
    }
    if let Some(text) = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
    {
        *posting_bytes_read = (*posting_bytes_read).max(text.posting_bytes_read);
        *candidate_postings_visited =
            (*candidate_postings_visited).max(text.candidate_postings_visited);
        *matching_document_count = (*matching_document_count).max(text.generated_candidate_count);
    }
}

fn latency_for_kind(
    query_cases: &[ProductionSearchQueryCase],
    runs: &[QueryRun],
    kind: ProductionSearchCaseKind,
) -> LatencyPercentiles {
    query_cases
        .iter()
        .position(|query_case| query_case.kind == kind)
        .and_then(|index| runs.get(index))
        .map(|run| latency_percentiles(&run.latency_micros))
        .unwrap_or_default()
}

fn latency_for_mode(
    query_cases: &[ProductionSearchQueryCase],
    runs: &[QueryRun],
    mode: SearchMode,
) -> LatencyPercentiles {
    let samples = query_cases
        .iter()
        .zip(runs)
        .filter(|(query_case, _)| query_case.request.mode == mode)
        .flat_map(|(_, run)| run.latency_micros.iter().copied())
        .collect::<Vec<_>>();
    latency_percentiles(&samples)
}

fn throughput_per_second(runs: &[QueryRun]) -> u64 {
    let query_count = runs
        .iter()
        .map(|run| run.latency_micros.len() as u64)
        .sum::<u64>();
    let elapsed_micros = runs
        .iter()
        .flat_map(|run| run.latency_micros.iter())
        .copied()
        .fold(0u64, u64::saturating_add)
        .max(1);
    query_count.saturating_mul(1_000_000) / elapsed_micros
}

fn contains_hit(result: &SearchResultSet, document_id: &str) -> bool {
    result.hits.iter().any(|hit| hit.id == document_id)
}

fn result_digest(result: &SearchResultSet) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"skein-production-search-result-v1");
    hash_usize(&mut hasher, result.total_hits);
    hash_usize(&mut hasher, result.limit);
    hash_usize(&mut hasher, result.offset);
    for hit in &result.hits {
        hash_field(&mut hasher, hit.id.as_bytes());
        for score in [
            hit.score,
            hit.vector_score,
            hit.text_score,
            hit.rrf_score,
            hit.vector_rrf_score,
            hit.text_rrf_score,
        ] {
            hasher.update(score.to_bits().to_le_bytes());
        }
        hash_optional_usize(&mut hasher, hit.vector_rank);
        hash_optional_usize(&mut hasher, hit.text_rank);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn request_digest(query_case: &ProductionSearchQueryCase) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"skein-production-search-request-v1");
    hash_field(&mut hasher, query_case.kind.as_str().as_bytes());
    hash_field(
        &mut hasher,
        search_mode_name(query_case.request.mode).as_bytes(),
    );
    hash_field(&mut hasher, query_case.request.query_text.as_bytes());
    if let Some(embedding) = &query_case.request.query_embedding {
        for value in embedding {
            hasher.update(value.to_bits().to_le_bytes());
        }
    }
    hash_usize(&mut hasher, query_case.request.limit);
    hash_usize(&mut hasher, query_case.request.offset);
    hash_optional_usize(&mut hasher, query_case.request.rank_window);
    hasher.update(
        query_case
            .request
            .fusion_weights
            .vector_weight
            .to_bits()
            .to_le_bytes(),
    );
    hasher.update(
        query_case
            .request
            .fusion_weights
            .text_weight
            .to_bits()
            .to_le_bytes(),
    );
    for (name, value) in &query_case.request.metadata_filters {
        hash_field(&mut hasher, name.as_bytes());
        hash_field(&mut hasher, value.as_bytes());
    }
    if let Some(access_control) = &query_case.access_control {
        hasher.update([1]);
        hasher.update(access_control.policy_epoch.to_le_bytes());
        hash_field(
            &mut hasher,
            access_control.visibility_metadata_field.as_bytes(),
        );
        for value in &access_control.allowed_visibility_values {
            hash_field(&mut hasher, value.as_bytes());
        }
    } else {
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hash_usize(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u64).to_le_bytes());
}

fn hash_optional_usize(hasher: &mut Sha256, value: Option<usize>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_usize(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn add_out_of_core_metrics(
    aggregate: &mut SearchOutOfCoreMetrics,
    metrics: &SearchOutOfCoreMetrics,
) {
    aggregate.segment_range_reads = aggregate
        .segment_range_reads
        .saturating_add(metrics.segment_range_reads);
    aggregate.segment_bytes_read = aggregate
        .segment_bytes_read
        .saturating_add(metrics.segment_bytes_read);
    aggregate.metadata_segment_bytes_read = aggregate
        .metadata_segment_bytes_read
        .saturating_add(metrics.metadata_segment_bytes_read);
    aggregate.vector_segment_bytes_read = aggregate
        .vector_segment_bytes_read
        .saturating_add(metrics.vector_segment_bytes_read);
    aggregate.hydration_segment_bytes_read = aggregate
        .hydration_segment_bytes_read
        .saturating_add(metrics.hydration_segment_bytes_read);
    aggregate.peak_segment_document_bytes = aggregate
        .peak_segment_document_bytes
        .max(metrics.peak_segment_document_bytes);
    aggregate.peak_metadata_segment_bytes = aggregate
        .peak_metadata_segment_bytes
        .max(metrics.peak_metadata_segment_bytes);
    aggregate.peak_vector_segment_bytes = aggregate
        .peak_vector_segment_bytes
        .max(metrics.peak_vector_segment_bytes);
    aggregate.candidate_spill_bytes = aggregate
        .candidate_spill_bytes
        .saturating_add(metrics.candidate_spill_bytes);
    aggregate.candidate_block_reads = aggregate
        .candidate_block_reads
        .saturating_add(metrics.candidate_block_reads);
    aggregate.candidate_bytes_read = aggregate
        .candidate_bytes_read
        .saturating_add(metrics.candidate_bytes_read);
    aggregate.vector_bytes_read = aggregate
        .vector_bytes_read
        .saturating_add(metrics.vector_bytes_read);
    aggregate.hydrated_documents = aggregate
        .hydrated_documents
        .saturating_add(metrics.hydrated_documents);
    aggregate.hydrated_bytes = aggregate
        .hydrated_bytes
        .saturating_add(metrics.hydrated_bytes);
}

fn delta_logical_bytes(delta: &SearchProjectionDelta) -> u64 {
    let upserts = delta.upserts.iter().fold(0u64, |bytes, row| {
        bytes
            .saturating_add(row.external_id.len() as u64)
            .saturating_add(row.title.len() as u64)
            .saturating_add(row.body.len() as u64)
            .saturating_add(
                row.embedding
                    .as_ref()
                    .map(|embedding| {
                        (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
                    })
                    .unwrap_or_default(),
            )
            .saturating_add(
                row.metadata
                    .iter()
                    .map(|(name, value)| (name.len() + value.len()) as u64)
                    .fold(0u64, u64::saturating_add),
            )
    });
    delta
        .deletes
        .iter()
        .map(|id| id.len() as u64)
        .fold(upserts, u64::saturating_add)
        .max(1)
}

fn directory_regular_file_bytes(path: &Path) -> Result<u64, ProductionSearchQualificationError> {
    let mut total = 0u64;
    let entries =
        std::fs::read_dir(path).map_err(ProductionSearchQualificationError::from_error)?;
    for entry in entries {
        let entry = entry.map_err(ProductionSearchQualificationError::from_error)?;
        let metadata = entry
            .metadata()
            .map_err(ProductionSearchQualificationError::from_error)?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn search_mode_name(mode: SearchMode) -> &'static str {
    match mode {
        SearchMode::Text => "text",
        SearchMode::Vector => "vector",
        SearchMode::Hybrid => "hybrid",
    }
}

fn process_memory_json(profile: ProcessMemoryProfile) -> serde_json::Value {
    serde_json::json!({
        "capabilities": {
            "resident_memory": profile.capabilities.resident_memory,
            "total_page_faults": profile.capabilities.total_page_faults,
            "split_page_faults": profile.capabilities.split_page_faults,
        },
        "start_resident_bytes": profile.start_resident_bytes,
        "start_peak_resident_bytes": profile.start_peak_resident_bytes,
        "steady_resident_bytes": profile.steady_resident_bytes,
        "peak_resident_bytes": profile.peak_resident_bytes,
        "steady_resident_growth_bytes": profile.steady_resident_growth_bytes,
        "lifetime_peak_resident_growth_bytes": profile.lifetime_peak_resident_growth_bytes,
        "total_page_faults": profile.total_page_faults,
        "minor_page_faults": profile.minor_page_faults,
        "major_page_faults": profile.major_page_faults,
    })
}

fn out_of_core_metrics_json(metrics: &SearchOutOfCoreMetrics) -> serde_json::Value {
    serde_json::json!({
        "segment_range_reads": metrics.segment_range_reads,
        "segment_bytes_read": metrics.segment_bytes_read,
        "metadata_segment_bytes_read": metrics.metadata_segment_bytes_read,
        "vector_segment_bytes_read": metrics.vector_segment_bytes_read,
        "hydration_segment_bytes_read": metrics.hydration_segment_bytes_read,
        "peak_segment_document_bytes": metrics.peak_segment_document_bytes,
        "peak_metadata_segment_bytes": metrics.peak_metadata_segment_bytes,
        "peak_vector_segment_bytes": metrics.peak_vector_segment_bytes,
        "candidate_spill_bytes": metrics.candidate_spill_bytes,
        "candidate_block_reads": metrics.candidate_block_reads,
        "candidate_bytes_read": metrics.candidate_bytes_read,
        "vector_bytes_read": metrics.vector_bytes_read,
        "hydrated_documents": metrics.hydrated_documents,
        "hydrated_bytes": metrics.hydrated_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        SearchEmbeddingManifest, SearchProjectionKind, SearchProjectionRow,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn representative_runner_collects_parity_and_lifecycle_evidence() {
        let root = test_root("runner");
        let source = root.join("source");
        let lifecycle_paths = (0..3)
            .map(|index| root.join(format!("lifecycle-{index}")))
            .collect::<Vec<_>>();
        let corruption = root.join("corruption");
        build_projection(&source);
        for path in lifecycle_paths.iter().chain(std::iter::once(&corruption)) {
            copy_projection(&source, path);
        }
        let identity = production_identity(42);
        let report = run_production_search_out_of_core_qualification(
            ProductionSearchOutOfCoreQualificationConfig {
                search_projection_path: source,
                out_of_core_config: SearchOutOfCoreConfig {
                    spill_directory: root.join("spill"),
                    ..SearchOutOfCoreConfig::default()
                },
                search_memory_budget_bytes: 1,
                warmup_runs: 1,
                measurement_runs: 2,
                query_cases: query_cases(),
                lifecycle: ProductionSearchLifecycleConfig {
                    replica_paths: lifecycle_paths,
                    corruption_replica_path: corruption,
                    delta: SearchProjectionDelta {
                        upserts: vec![projection_row(
                            "added",
                            "lifecycle-added",
                            "common lifecycle",
                            [1.0, 0.0],
                            "a",
                        )],
                        deletes: vec!["memory:delete".to_string()],
                        max_operations: Some(2),
                        source_graph_commit_epoch: Some(42),
                    },
                    expected_upsert_document_id: "memory:added".to_string(),
                    expected_deleted_document_id: "memory:delete".to_string(),
                    upsert_verification: text_case(
                        "upsert-verification",
                        ProductionSearchCaseKind::SelectiveIdentifier,
                        "lifecycle-added",
                    ),
                    delete_verification: text_case(
                        "delete-verification",
                        ProductionSearchCaseKind::SelectiveIdentifier,
                        "selective-alpha",
                    ),
                    mixed_load_probe_runs: 4,
                    reference_update_p95_micros: 10_000_000,
                    reference_checkpoint_p95_micros: 10_000_000,
                },
                evidence_binding: ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                expected_identity: identity,
            },
        )
        .expect("qualification runner should collect evidence");

        assert!(report
            .query_evidence
            .iter()
            .all(|evidence| evidence.exact_topk_score_parity));
        assert!(report.lifecycle.incremental_upsert_delete);
        assert!(report.lifecycle.checkpoint_reopen);
        assert!(report.lifecycle.stale_generation);
        assert!(report.lifecycle.corrupt_artifact_rejected);
        assert!(report.lifecycle.mixed_foreground_background);
        assert!(report
            .qualification
            .blocker_codes
            .contains(&"dataset_too_small".to_string()));
        assert!(report
            .qualification
            .blocker_codes
            .contains(&"resident_memory_budget_exceeded".to_string()));
        assert_eq!(
            report.json()["protocol"],
            PRODUCTION_SEARCH_OUT_OF_CORE_QUALIFICATION_PROTOCOL
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acl_case_is_required_only_for_acl_release_identity() {
        let cases = query_cases();
        assert!(validate_query_cases(&cases, &production_identity(42)).is_ok());
        let mut acl_identity = production_identity(42);
        acl_identity.enabled_features.push("acl".to_string());

        let error = validate_query_cases(&cases, &acl_identity).unwrap_err();

        assert!(error.to_string().contains("requires an ACL case"));
    }

    fn build_projection(path: &Path) {
        let mut index = SearchIndex::open(path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "test-embedding".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![
                    projection_row(
                        "delete",
                        "selective-alpha",
                        "common \u{4e2d}\u{6587} token",
                        [1.0, 0.0],
                        "a",
                    ),
                    projection_row("keep", "common beta", "common body", [0.0, 1.0], "b"),
                ],
                deletes: Vec::new(),
                max_operations: Some(2),
                source_graph_commit_epoch: Some(42),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }

    fn copy_projection(source: &Path, destination: &Path) {
        std::fs::create_dir_all(destination).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
            }
        }
    }

    fn query_cases() -> Vec<ProductionSearchQueryCase> {
        vec![
            text_case(
                "selective",
                ProductionSearchCaseKind::SelectiveIdentifier,
                "selective-alpha",
            ),
            text_case("cjk", ProductionSearchCaseKind::CjkText, "\u{4e2d}\u{6587}"),
            text_case("common", ProductionSearchCaseKind::CommonTerm, "common"),
            text_case("no-hit", ProductionSearchCaseKind::NoHit, "absent"),
            ProductionSearchQueryCase {
                name: "metadata".to_string(),
                kind: ProductionSearchCaseKind::MetadataFilter,
                request: NowledgeMemSearchCandidateRequest::text("common", 2)
                    .with_metadata_filters(BTreeMap::from([(
                        "group".to_string(),
                        "a".to_string(),
                    )])),
                access_control: None,
            },
            ProductionSearchQueryCase {
                name: "vector".to_string(),
                kind: ProductionSearchCaseKind::Vector,
                request: NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 2),
                access_control: None,
            },
            ProductionSearchQueryCase {
                name: "hybrid".to_string(),
                kind: ProductionSearchCaseKind::Hybrid,
                request: NowledgeMemSearchCandidateRequest::hybrid("common", vec![1.0, 0.0], 2)
                    .with_rank_window(Some(2)),
                access_control: None,
            },
        ]
    }

    fn text_case(
        name: &str,
        kind: ProductionSearchCaseKind,
        query: &str,
    ) -> ProductionSearchQueryCase {
        ProductionSearchQueryCase {
            name: name.to_string(),
            kind,
            request: NowledgeMemSearchCandidateRequest::text(query, 2),
            access_control: None,
        }
    }

    fn projection_row(
        external_id: &str,
        title: &str,
        body: &str,
        embedding: [f32; 2],
        group: &str,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: external_id.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            embedding: Some(embedding.to_vec()),
            source_id: None,
            metadata: BTreeMap::from([("group".to_string(), group.to_string())]),
        }
    }

    fn production_identity(epoch: u64) -> ProductionQualificationIdentity {
        ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: vec!["full-text-search".to_string(), "vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-configuration".to_string(),
            deployment_profile: "test".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn test_root(name: &str) -> PathBuf {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "skein-production-search-qualification-{name}-{}-{sequence}",
            std::process::id()
        ))
    }
}
