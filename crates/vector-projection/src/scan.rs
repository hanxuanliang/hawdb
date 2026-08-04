use crate::artifact::{FileProjection, SegmentParts};
use crate::codec::bytes_per_vector;
use crate::error::{ProjectionError, Result};
use crate::kernel::{score_codes, select_kernel, KernelPreference, ScanKernel};
use crate::model::{InMemoryProjection, ProjectionManifest};
use crate::quantizer::TurboQuantCodebook;
use crate::transform::normalize_and_transform;
use skein_core::RuntimeTaskContext;
use std::cmp::Ordering;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

const SCAN_BLOCK_ROWS: usize = 32;
const DEFAULT_SEARCH_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const WORKER_FIXED_BYTES: usize = 1_024;
const WORKER_STACK_BYTES: usize = 512 * 1024;
const SEARCH_FIXED_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy)]
pub struct ProjectionSearchOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: KernelPreference,
    pub allowed_ids: Option<&'a [u64]>,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl<'a> ProjectionSearchOptions<'a> {
    pub fn new() -> Self {
        Self {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_SEARCH_MEMORY_BYTES,
            kernel: KernelPreference::Auto,
            allowed_ids: None,
            task_context: None,
        }
    }

    pub fn with_max_parallelism(mut self, max_parallelism: NonZeroUsize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    pub fn with_max_working_bytes(mut self, max_working_bytes: usize) -> Self {
        self.max_working_bytes = max_working_bytes;
        self
    }

    pub fn with_kernel(mut self, kernel: KernelPreference) -> Self {
        self.kernel = kernel;
        self
    }

    pub fn with_allowed_ids(mut self, allowed_ids: &'a [u64]) -> Self {
        self.allowed_ids = Some(allowed_ids);
        self
    }

    pub fn with_task_context(mut self, task_context: &'a RuntimeTaskContext) -> Self {
        self.task_context = Some(task_context);
        self
    }
}

impl Default for ProjectionSearchOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionHit {
    pub id: u64,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSearchReport {
    pub kernel: ScanKernel,
    pub worker_count: usize,
    pub segment_count: usize,
    pub scanned_segment_count: usize,
    pub document_count: usize,
    pub scored_document_count: usize,
    pub filtered_document_count: usize,
    pub scanned_block_count: usize,
    pub skipped_block_count: usize,
    pub payload_bytes_read: u64,
    pub admitted_working_bytes: usize,
    pub candidate_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionSearchOutput {
    pub hits: Vec<ProjectionHit>,
    pub report: ProjectionSearchReport,
}

impl InMemoryProjection {
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> Result<ProjectionSearchOutput> {
        let max_rows = self
            .segments
            .iter()
            .map(|segment| segment.row_count())
            .max()
            .unwrap_or(0);
        search_projection(
            &self.manifest,
            query,
            top_k,
            options,
            max_rows,
            0,
            &self.codebook,
            |segment_index, transformed_query, kernel, codebook, allowed_ids, context| {
                let segment = &self.segments[segment_index];
                scan_segment(
                    segment.row_count(),
                    self.manifest.dimension,
                    |row| segment.ids[row],
                    |row| segment.renormalizations[row],
                    &segment.codes,
                    transformed_query,
                    top_k,
                    kernel,
                    codebook,
                    allowed_ids,
                    context,
                    0,
                )
            },
        )
    }
}

impl FileProjection {
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> Result<ProjectionSearchOutput> {
        let max_rows = self
            .manifest()
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .max()
            .unwrap_or(0);
        let max_payload_bytes = self
            .manifest()
            .segments
            .iter()
            .map(|segment| usize::try_from(segment.payload_bytes).unwrap_or(usize::MAX))
            .max()
            .unwrap_or(0);
        search_projection(
            self.manifest(),
            query,
            top_k,
            options,
            max_rows,
            max_payload_bytes,
            &self.codebook,
            |segment_index, transformed_query, kernel, codebook, allowed_ids, context| {
                let descriptor = &self.manifest().segments[segment_index];
                let buffer = self.read_segment(segment_index)?;
                let parts = buffer.parts(self.manifest().dimension, descriptor.row_count)?;
                scan_file_segment(
                    parts,
                    descriptor.row_count,
                    self.manifest().dimension,
                    transformed_query,
                    top_k,
                    kernel,
                    codebook,
                    allowed_ids,
                    context,
                    descriptor.payload_bytes,
                )
            },
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn search_projection<F>(
    manifest: &ProjectionManifest,
    query: &[f32],
    top_k: usize,
    options: ProjectionSearchOptions<'_>,
    max_segment_rows: usize,
    max_segment_payload_bytes: usize,
    codebook: &TurboQuantCodebook,
    operation: F,
) -> Result<ProjectionSearchOutput>
where
    F: Fn(
            usize,
            &[f32],
            ScanKernel,
            &TurboQuantCodebook,
            Option<&[u64]>,
            Option<&RuntimeTaskContext>,
        ) -> Result<SegmentSearchResult>
        + Sync,
{
    if query.len() != manifest.dimension {
        return Err(ProjectionError::InvalidVector(format!(
            "expected query dimension {}, got {}",
            manifest.dimension,
            query.len()
        )));
    }
    if options
        .allowed_ids
        .is_some_and(|ids| ids.windows(2).any(|pair| pair[0] >= pair[1]))
    {
        return Err(ProjectionError::InvalidConfiguration(
            "allowed ids must be sorted and unique".to_string(),
        ));
    }
    if let Some(context) = options.task_context {
        context.checkpoint()?;
    }
    let kernel = select_kernel(options.kernel)?;
    let mut transformed_query = vec![0.0; manifest.dimension];
    normalize_and_transform(query, manifest.transform_seed, &mut transformed_query)?;
    let segment_count = manifest.segments.len();
    if top_k == 0 || segment_count == 0 || options.allowed_ids.is_some_and(<[u64]>::is_empty) {
        return Ok(ProjectionSearchOutput {
            hits: Vec::new(),
            report: ProjectionSearchReport {
                kernel,
                worker_count: 0,
                segment_count,
                scanned_segment_count: 0,
                document_count: manifest.document_count,
                scored_document_count: 0,
                filtered_document_count: manifest.document_count,
                scanned_block_count: 0,
                skipped_block_count: manifest.document_count.div_ceil(SCAN_BLOCK_ROWS),
                payload_bytes_read: 0,
                admitted_working_bytes: transformed_query
                    .len()
                    .saturating_mul(std::mem::size_of::<f32>()),
                candidate_count: 0,
            },
        });
    }

    let query_bytes = transformed_query
        .len()
        .saturating_mul(std::mem::size_of::<f32>());
    let mask_bytes = options.allowed_ids.map_or(0, |_| {
        max_segment_rows.div_ceil(u64::BITS as usize) * std::mem::size_of::<u64>()
    });
    let top_k_bytes = top_k.saturating_mul(std::mem::size_of::<ProjectionHit>());
    let per_worker_bytes = max_segment_payload_bytes
        .saturating_add(mask_bytes)
        .saturating_add(top_k_bytes)
        .saturating_add(WORKER_STACK_BYTES)
        .saturating_add(WORKER_FIXED_BYTES);
    let global_top_k_bytes = top_k.saturating_mul(std::mem::size_of::<ProjectionHit>());
    let global_bytes = query_bytes
        .saturating_add(global_top_k_bytes)
        .saturating_add(SEARCH_FIXED_BYTES);
    let available_for_workers = options.max_working_bytes.saturating_sub(global_bytes);
    let admitted_by_memory = available_for_workers / per_worker_bytes.max(1);
    if admitted_by_memory == 0 {
        return Err(ProjectionError::ResourceBudgetExceeded {
            required: global_bytes.saturating_add(per_worker_bytes),
            available: options.max_working_bytes,
        });
    }
    let worker_count = options
        .max_parallelism
        .get()
        .min(segment_count)
        .min(admitted_by_memory)
        .max(1);
    let admitted_working_bytes = global_bytes.saturating_add(worker_count * per_worker_bytes);

    let top_k_state = Mutex::new(TopK::new(top_k));
    let report = Mutex::new(ReportAccumulator::default());
    let first_error = Mutex::new(None);
    let stopped = AtomicBool::new(false);
    let next_segment = AtomicUsize::new(0);

    let run_worker = || loop {
        if stopped.load(AtomicOrdering::Acquire) {
            break;
        }
        if let Some(context) = options.task_context
            && let Err(reason) = context.checkpoint()
        {
            store_error(&first_error, &stopped, ProjectionError::Cancelled(reason));
            break;
        }
        let segment_index = next_segment.fetch_add(1, AtomicOrdering::Relaxed);
        if segment_index >= segment_count {
            break;
        }
        match operation(
            segment_index,
            &transformed_query,
            kernel,
            codebook,
            options.allowed_ids,
            options.task_context,
        ) {
            Ok(segment_result) => {
                report
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .add(&segment_result);
                top_k_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .extend(segment_result.hits);
            }
            Err(error) => {
                store_error(&first_error, &stopped, error);
                break;
            }
        }
    };

    if worker_count == 1 {
        run_worker();
    } else {
        std::thread::scope(|scope| {
            for worker in 0..worker_count {
                if let Err(error) = std::thread::Builder::new()
                    .name(format!("skein-turboquant-scan-{worker}"))
                    .stack_size(WORKER_STACK_BYTES)
                    .spawn_scoped(scope, run_worker)
                {
                    store_error(&first_error, &stopped, ProjectionError::Io(error));
                    break;
                }
            }
        });
    }

    if let Some(error) = first_error
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return Err(error);
    }
    if let Some(context) = options.task_context {
        context.checkpoint()?;
    }
    let hits = top_k_state
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .finish();
    let accumulated = report
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(ProjectionSearchOutput {
        report: ProjectionSearchReport {
            kernel,
            worker_count,
            segment_count,
            scanned_segment_count: accumulated.scanned_segment_count,
            document_count: manifest.document_count,
            scored_document_count: accumulated.scored_document_count,
            filtered_document_count: accumulated.filtered_document_count,
            scanned_block_count: accumulated.scanned_block_count,
            skipped_block_count: accumulated.skipped_block_count,
            payload_bytes_read: accumulated.payload_bytes_read,
            admitted_working_bytes,
            candidate_count: hits.len(),
        },
        hits,
    })
}

#[allow(clippy::too_many_arguments)]
fn scan_file_segment(
    parts: SegmentParts<'_>,
    rows: usize,
    dimension: usize,
    query: &[f32],
    top_k: usize,
    kernel: ScanKernel,
    codebook: &TurboQuantCodebook,
    allowed_ids: Option<&[u64]>,
    context: Option<&RuntimeTaskContext>,
    payload_bytes_read: u64,
) -> Result<SegmentSearchResult> {
    scan_segment(
        rows,
        dimension,
        |row| parts.id(row),
        |row| parts.renormalization(row),
        parts.codes,
        query,
        top_k,
        kernel,
        codebook,
        allowed_ids,
        context,
        payload_bytes_read,
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_segment<Id, Scale>(
    rows: usize,
    dimension: usize,
    id_at: Id,
    scale_at: Scale,
    codes: &[u8],
    query: &[f32],
    top_k: usize,
    kernel: ScanKernel,
    codebook: &TurboQuantCodebook,
    allowed_ids: Option<&[u64]>,
    context: Option<&RuntimeTaskContext>,
    payload_bytes_read: u64,
) -> Result<SegmentSearchResult>
where
    Id: Fn(usize) -> u64,
    Scale: Fn(usize) -> f32,
{
    let bytes_per_vector = bytes_per_vector(dimension);
    if codes.len() != rows.saturating_mul(bytes_per_vector) {
        return Err(ProjectionError::CorruptArtifact(
            "segment code length does not match rows and dimension".to_string(),
        ));
    }
    let mask = allowed_ids.map(|allowed| {
        let mut words = vec![0u64; rows.div_ceil(u64::BITS as usize)];
        for row in 0..rows {
            if allowed.binary_search(&id_at(row)).is_ok() {
                words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            }
        }
        words
    });
    let mut top = TopK::new(top_k);
    let mut scored_document_count = 0usize;
    let mut scanned_block_count = 0usize;
    let mut skipped_block_count = 0usize;
    for block_start in (0..rows).step_by(SCAN_BLOCK_ROWS) {
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let block_end = (block_start + SCAN_BLOCK_ROWS).min(rows);
        if mask
            .as_deref()
            .is_some_and(|mask| !mask_has_any(mask, block_start, block_end))
        {
            skipped_block_count = skipped_block_count.saturating_add(1);
            continue;
        }
        scanned_block_count = scanned_block_count.saturating_add(1);
        for row in block_start..block_end {
            if mask
                .as_deref()
                .is_some_and(|mask| !mask_contains(mask, row))
            {
                continue;
            }
            let scale = scale_at(row);
            if !scale.is_finite() || scale < 0.0 {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "row {row} has an invalid renormalization"
                )));
            }
            let start = row * bytes_per_vector;
            let score = score_codes(
                kernel,
                &codes[start..start + bytes_per_vector],
                query,
                codebook.centroids(),
            ) * scale;
            if !score.is_finite() {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "row {row} produced a non-finite score"
                )));
            }
            top.push(ProjectionHit {
                id: id_at(row),
                score,
            });
            scored_document_count = scored_document_count.saturating_add(1);
        }
    }
    Ok(SegmentSearchResult {
        hits: top.finish(),
        scanned_segment_count: 1,
        scored_document_count,
        filtered_document_count: rows.saturating_sub(scored_document_count),
        scanned_block_count,
        skipped_block_count,
        payload_bytes_read,
    })
}

fn mask_has_any(mask: &[u64], start: usize, end: usize) -> bool {
    (start..end).any(|row| mask_contains(mask, row))
}

fn mask_contains(mask: &[u64], row: usize) -> bool {
    mask.get(row / u64::BITS as usize)
        .is_some_and(|word| word & (1u64 << (row % u64::BITS as usize)) != 0)
}

fn store_error(
    first_error: &Mutex<Option<ProjectionError>>,
    stopped: &AtomicBool,
    error: ProjectionError,
) {
    let mut slot = first_error
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if slot.is_none() {
        *slot = Some(error);
    }
    stopped.store(true, AtomicOrdering::Release);
}

#[derive(Debug)]
struct SegmentSearchResult {
    hits: Vec<ProjectionHit>,
    scanned_segment_count: usize,
    scored_document_count: usize,
    filtered_document_count: usize,
    scanned_block_count: usize,
    skipped_block_count: usize,
    payload_bytes_read: u64,
}

#[derive(Debug, Default)]
struct ReportAccumulator {
    scanned_segment_count: usize,
    scored_document_count: usize,
    filtered_document_count: usize,
    scanned_block_count: usize,
    skipped_block_count: usize,
    payload_bytes_read: u64,
}

impl ReportAccumulator {
    fn add(&mut self, result: &SegmentSearchResult) {
        self.scanned_segment_count = self
            .scanned_segment_count
            .saturating_add(result.scanned_segment_count);
        self.scored_document_count = self
            .scored_document_count
            .saturating_add(result.scored_document_count);
        self.filtered_document_count = self
            .filtered_document_count
            .saturating_add(result.filtered_document_count);
        self.scanned_block_count = self
            .scanned_block_count
            .saturating_add(result.scanned_block_count);
        self.skipped_block_count = self
            .skipped_block_count
            .saturating_add(result.skipped_block_count);
        self.payload_bytes_read = self
            .payload_bytes_read
            .saturating_add(result.payload_bytes_read);
    }
}

#[derive(Debug)]
struct TopK {
    limit: usize,
    hits: Vec<ProjectionHit>,
}

impl TopK {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            hits: Vec::with_capacity(limit),
        }
    }

    fn push(&mut self, hit: ProjectionHit) {
        if self.limit == 0 {
            return;
        }
        if self.hits.len() < self.limit {
            self.hits.push(hit);
            return;
        }
        let worst = self
            .hits
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| compare_best(left, right))
            .map(|(index, _)| index)
            .expect("non-empty bounded top-k");
        if compare_best(&hit, &self.hits[worst]) == Ordering::Greater {
            self.hits[worst] = hit;
        }
    }

    fn extend(&mut self, hits: Vec<ProjectionHit>) {
        for hit in hits {
            self.push(hit);
        }
    }

    fn finish(mut self) -> Vec<ProjectionHit> {
        self.hits.sort_by(|left, right| compare_best(right, left));
        self.hits
    }
}

fn compare_best(left: &ProjectionHit, right: &ProjectionHit) -> Ordering {
    left.score
        .total_cmp(&right.score)
        .then_with(|| right.id.cmp(&left.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FileProjection, ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity,
        ProjectionWriter,
    };
    use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scalar_projection_finds_nearest_vector_and_honors_filter() {
        let projection = sample_in_memory_projection(8, 2);
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let output = projection
            .search(
                &query,
                2,
                ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar),
            )
            .unwrap();
        assert_eq!(output.hits[0].id, 10);

        let allowed = [20];
        let filtered = projection
            .search(
                &query,
                2,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_allowed_ids(&allowed),
            )
            .unwrap();
        assert_eq!(filtered.hits.len(), 1);
        assert_eq!(filtered.hits[0].id, 20);
        assert_eq!(filtered.report.scored_document_count, 1);
    }

    #[test]
    fn allowlist_must_be_sorted_and_unique() {
        let projection = sample_in_memory_projection(8, 2);
        let allowed = [20, 10];
        let result = projection.search(
            &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            2,
            ProjectionSearchOptions::new().with_allowed_ids(&allowed),
        );

        assert!(matches!(
            result,
            Err(ProjectionError::InvalidConfiguration(message))
                if message == "allowed ids must be sorted and unique"
        ));
    }

    #[test]
    fn search_budget_accounts_for_global_top_k_and_worker_stack() {
        let projection = sample_in_memory_projection(8, 2);
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let query_bytes = query.len() * std::mem::size_of::<f32>();
        let top_k_bytes = 2 * std::mem::size_of::<ProjectionHit>();
        let required = query_bytes
            + top_k_bytes
            + SEARCH_FIXED_BYTES
            + top_k_bytes
            + WORKER_STACK_BYTES
            + WORKER_FIXED_BYTES;
        let result = projection.search(
            &query,
            2,
            ProjectionSearchOptions::new().with_max_working_bytes(required - 1),
        );

        assert!(matches!(
            result,
            Err(ProjectionError::ResourceBudgetExceeded {
                required: actual,
                available
            }) if actual == required && available == required - 1
        ));
    }

    #[test]
    fn parallel_file_scan_matches_sequential_scan() {
        let root = unique_test_dir("parallel");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_turboquant.1.skein");
        let config =
            ProjectionBuildConfig::new(64, ProjectionIdentity::new(1)).with_segment_rows(3);
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        for id in 0..30u64 {
            let vector = (0..64)
                .map(|dimension| ((id * 17 + dimension as u64 * 13) as f32).sin())
                .collect::<Vec<_>>();
            writer.push(id, &vector).unwrap();
        }
        let projection = writer.finish().unwrap();
        let query = (0..64)
            .map(|dimension| (dimension as f32 * 0.31).cos())
            .collect::<Vec<_>>();
        let sequential = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::MIN),
            )
            .unwrap();
        let parallel = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::new(4).unwrap()),
            )
            .unwrap();
        assert_eq!(parallel.hits, sequential.hits);
        assert_eq!(parallel.report.worker_count, 4);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_stops_between_scan_blocks() {
        let projection = sample_in_memory_projection(64, 1);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        let result = projection.search(
            &[1.0; 64],
            2,
            ProjectionSearchOptions::new().with_task_context(&context),
        );
        assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    }

    #[test]
    fn auto_kernel_matches_scalar_top_k() {
        let dimension = 96;
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        for id in 0..128u64 {
            let vector = (0..dimension)
                .map(|coordinate| ((id * 19 + coordinate as u64 * 7) as f32).cos())
                .collect::<Vec<_>>();
            builder.push(id, &vector).unwrap();
        }
        let projection = builder.finish().unwrap();
        let query = (0..dimension)
            .map(|coordinate| (coordinate as f32 * 0.23).sin())
            .collect::<Vec<_>>();
        let scalar = projection
            .search(
                &query,
                10,
                ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar),
            )
            .unwrap();
        let automatic = projection
            .search(&query, 10, ProjectionSearchOptions::new())
            .unwrap();
        assert_eq!(
            automatic.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            scalar.hits.iter().map(|hit| hit.id).collect::<Vec<_>>()
        );
        for (automatic, scalar) in automatic.hits.iter().zip(&scalar.hits) {
            assert!((automatic.score - scalar.score).abs() < 1e-4);
        }
    }

    fn sample_in_memory_projection(dimension: usize, segment_rows: usize) -> InMemoryProjection {
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
            .with_segment_rows(segment_rows);
        let mut builder = ProjectionBuilder::new(config).unwrap();
        let mut first = vec![0.0; dimension];
        first[0] = 1.0;
        let mut second = vec![0.0; dimension];
        second[1] = 1.0;
        let mut third = vec![0.0; dimension];
        third[2] = 1.0;
        builder.push(10, &first).unwrap();
        builder.push(20, &second).unwrap();
        builder.push(30, &third).unwrap();
        builder.finish().unwrap()
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_vector_scan_{name}_{}_{nanos}",
            std::process::id()
        ))
    }

    #[allow(dead_code)]
    fn assert_file_projection_is_send_sync(_: &FileProjection) {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileProjection>();
    }
}
