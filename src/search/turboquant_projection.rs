use super::SearchDocument;
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_vector_projection::{
    FileProjection, InMemoryProjection, KernelPreference, ProjectionBuildConfig,
    ProjectionBuildReport, ProjectionBuilder, ProjectionIdentity, ProjectionManifest,
    ProjectionSearchOptions, ProjectionSearchReport, ProjectionWriter, DEFAULT_BUILD_MEMORY_BYTES,
    DEFAULT_SEGMENT_ROWS, DEFAULT_TRANSFORM_SEED,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;

const DEFAULT_TURBOQUANT_SEARCH_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const NUMERIC_ID_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const NUMERIC_ID_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurboQuantCandidateProjectionBuildOptions {
    pub segment_rows: usize,
    pub max_working_bytes: usize,
    pub transform_seed: u64,
}

impl Default for TurboQuantCandidateProjectionBuildOptions {
    fn default() -> Self {
        Self {
            segment_rows: DEFAULT_SEGMENT_ROWS,
            max_working_bytes: DEFAULT_BUILD_MEMORY_BYTES,
            transform_seed: DEFAULT_TRANSFORM_SEED,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TurboQuantCandidateScanOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: KernelPreference,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl TurboQuantCandidateScanOptions<'_> {
    pub fn sequential() -> Self {
        Self {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_TURBOQUANT_SEARCH_MEMORY_BYTES,
            kernel: KernelPreference::Auto,
            task_context: None,
        }
    }
}

impl Default for TurboQuantCandidateScanOptions<'_> {
    fn default() -> Self {
        Self::sequential()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurboQuantCandidate {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurboQuantCandidateOutput {
    pub candidates: Vec<TurboQuantCandidate>,
    pub report: ProjectionSearchReport,
}

#[derive(Debug)]
pub struct TurboQuantCandidateProjection {
    storage: TurboQuantCandidateProjectionStorage,
    numeric_to_document_id: BTreeMap<u64, String>,
    document_to_numeric_id: BTreeMap<String, u64>,
    build_report: ProjectionBuildReport,
}

#[derive(Debug)]
enum TurboQuantCandidateProjectionStorage {
    InMemory(InMemoryProjection),
    File(FileProjection),
}

struct TurboQuantDocumentIdMap {
    dimension: usize,
    numeric_to_document_id: BTreeMap<u64, String>,
    document_to_numeric_id: BTreeMap<String, u64>,
}

impl TurboQuantCandidateProjection {
    pub fn build_from_documents(
        documents: &BTreeMap<String, SearchDocument>,
        identity: ProjectionIdentity,
        options: TurboQuantCandidateProjectionBuildOptions,
    ) -> Result<Option<Self>> {
        let Some(TurboQuantDocumentIdMap {
            dimension,
            numeric_to_document_id,
            document_to_numeric_id,
        }) = validate_and_map_documents(documents)?
        else {
            return Ok(None);
        };
        let config = build_config(dimension, identity, options);
        let mut builder = ProjectionBuilder::new(config).map_err(projection_error)?;
        for (numeric_id, document_id) in &numeric_to_document_id {
            let embedding = documents[document_id]
                .embedding
                .as_deref()
                .expect("mapped TurboQuant document has an embedding");
            builder
                .push(*numeric_id, embedding)
                .map_err(projection_error)?;
        }
        let projection = builder.finish().map_err(projection_error)?;
        let build_report = projection.build_report().clone();
        Ok(Some(Self {
            storage: TurboQuantCandidateProjectionStorage::InMemory(projection),
            numeric_to_document_id,
            document_to_numeric_id,
            build_report,
        }))
    }

    pub fn write_from_documents(
        artifact_path: impl AsRef<Path>,
        documents: &BTreeMap<String, SearchDocument>,
        identity: ProjectionIdentity,
        options: TurboQuantCandidateProjectionBuildOptions,
    ) -> Result<Option<Self>> {
        let Some(TurboQuantDocumentIdMap {
            dimension,
            numeric_to_document_id,
            document_to_numeric_id,
        }) = validate_and_map_documents(documents)?
        else {
            return Ok(None);
        };
        let config = build_config(dimension, identity, options);
        let mut writer =
            ProjectionWriter::create(artifact_path, config).map_err(projection_error)?;
        for (numeric_id, document_id) in &numeric_to_document_id {
            let embedding = documents[document_id]
                .embedding
                .as_deref()
                .expect("mapped TurboQuant document has an embedding");
            writer
                .push(*numeric_id, embedding)
                .map_err(projection_error)?;
        }
        let projection = writer.finish().map_err(projection_error)?;
        let build_report = projection.build_report();
        Ok(Some(Self {
            storage: TurboQuantCandidateProjectionStorage::File(projection),
            numeric_to_document_id,
            document_to_numeric_id,
            build_report,
        }))
    }

    pub fn load_from_path(
        artifact_path: impl AsRef<Path>,
        documents: &BTreeMap<String, SearchDocument>,
        expected_identity: &ProjectionIdentity,
    ) -> Result<Self> {
        let projection = FileProjection::open(artifact_path).map_err(projection_error)?;
        let Some(TurboQuantDocumentIdMap {
            dimension,
            numeric_to_document_id,
            document_to_numeric_id,
        }) = validate_and_map_documents(documents)?
        else {
            return Err(SkeinError::Storage(
                "Skein TurboQuant projection exists without vector documents".to_string(),
            ));
        };
        let manifest = projection.manifest();
        if manifest.dimension != dimension {
            return Err(SkeinError::Storage(format!(
                "Skein TurboQuant projection dimension {} does not match search dimension {dimension}",
                manifest.dimension
            )));
        }
        if &manifest.identity != expected_identity {
            return Err(SkeinError::Storage(format!(
                "Skein TurboQuant projection identity {:?} does not match expected {:?}",
                manifest.identity, expected_identity
            )));
        }
        let expected_digest = skein_vector_projection::source_digest(
            numeric_to_document_id.iter().map(|(numeric_id, document_id)| {
                (
                    *numeric_id,
                    documents[document_id]
                        .embedding
                        .as_deref()
                        .expect("mapped TurboQuant document has an embedding"),
                )
            }),
        );
        if manifest.source_digest != expected_digest {
            return Err(SkeinError::Storage(
                "Skein TurboQuant projection source digest does not match search documents"
                    .to_string(),
            ));
        }
        let build_report = projection.build_report();
        Ok(Self {
            storage: TurboQuantCandidateProjectionStorage::File(projection),
            numeric_to_document_id,
            document_to_numeric_id,
            build_report,
        })
    }

    pub fn search(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&str]>,
    ) -> Result<TurboQuantCandidateOutput> {
        self.search_with_options(
            query_embedding,
            limit,
            allowlist,
            TurboQuantCandidateScanOptions::default(),
        )
    }

    pub fn search_with_options(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&str]>,
        options: TurboQuantCandidateScanOptions<'_>,
    ) -> Result<TurboQuantCandidateOutput> {
        let allowed_numeric_ids = allowlist.map(|allowed| {
            let mut ids = allowed
                .iter()
                .filter_map(|id| self.document_to_numeric_id.get(*id).copied())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            ids
        });
        self.search_with_numeric_allowlist(query_embedding, limit, allowed_numeric_ids, options)
    }

    pub(super) fn search_candidates_for_documents_with_options(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&SearchDocument]>,
        options: TurboQuantCandidateScanOptions<'_>,
    ) -> Result<TurboQuantCandidateOutput> {
        let allowed_numeric_ids = allowlist.map(|allowed| {
            let mut ids = allowed
                .iter()
                .filter_map(|document| self.document_to_numeric_id.get(&document.id).copied())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            ids
        });
        self.search_with_numeric_allowlist(query_embedding, limit, allowed_numeric_ids, options)
    }

    fn search_with_numeric_allowlist(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowed_numeric_ids: Option<Vec<u64>>,
        options: TurboQuantCandidateScanOptions<'_>,
    ) -> Result<TurboQuantCandidateOutput> {
        let allowlist_bytes = allowed_numeric_ids
            .as_ref()
            .map_or(0, |ids| ids.len().saturating_mul(std::mem::size_of::<u64>()));
        let scan_working_bytes = options
            .max_working_bytes
            .checked_sub(allowlist_bytes)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "Skein TurboQuant projection allowlist requires {allowlist_bytes} bytes but the search budget is {} bytes",
                    options.max_working_bytes
                ))
            })?;
        let mut scan_options = ProjectionSearchOptions::new()
            .with_max_parallelism(options.max_parallelism)
            .with_max_working_bytes(scan_working_bytes)
            .with_kernel(options.kernel);
        if let Some(allowed) = &allowed_numeric_ids {
            scan_options = scan_options.with_allowed_ids(allowed);
        }
        if let Some(context) = options.task_context {
            scan_options = scan_options.with_task_context(context);
        }
        let mut output = match &self.storage {
            TurboQuantCandidateProjectionStorage::InMemory(projection) => {
                projection.search(query_embedding, limit, scan_options)
            }
            TurboQuantCandidateProjectionStorage::File(projection) => {
                projection.search(query_embedding, limit, scan_options)
            }
        }
        .map_err(projection_error)?;
        output.report.admitted_working_bytes = output
            .report
            .admitted_working_bytes
            .saturating_add(allowlist_bytes);
        let candidates = output
            .hits
            .into_iter()
            .map(|hit| {
                let id = self
                    .numeric_to_document_id
                    .get(&hit.id)
                    .cloned()
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "Skein TurboQuant projection returned unknown numeric id {}",
                            hit.id
                        ))
                    })?;
                Ok(TurboQuantCandidate {
                    id,
                    score: f64::from(hit.score),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(TurboQuantCandidateOutput {
            candidates,
            report: output.report,
        })
    }

    pub fn manifest(&self) -> &ProjectionManifest {
        match &self.storage {
            TurboQuantCandidateProjectionStorage::InMemory(projection) => projection.manifest(),
            TurboQuantCandidateProjectionStorage::File(projection) => projection.manifest(),
        }
    }

    pub fn build_report(&self) -> &ProjectionBuildReport {
        &self.build_report
    }

    pub fn is_file_backed(&self) -> bool {
        matches!(
            self.storage,
            TurboQuantCandidateProjectionStorage::File(_)
        )
    }

    pub fn contains_document_id(&self, document_id: &str) -> bool {
        self.document_to_numeric_id.contains_key(document_id)
    }

}

fn validate_and_map_documents(
    documents: &BTreeMap<String, SearchDocument>,
) -> Result<Option<TurboQuantDocumentIdMap>> {
    let mut dimension = None;
    let mut numeric_to_document_id = BTreeMap::new();
    let mut document_to_numeric_id = BTreeMap::new();
    for document in documents.values() {
        let Some(embedding) = document.embedding.as_deref() else {
            continue;
        };
        if embedding.is_empty() || !embedding.iter().all(|value| value.is_finite()) {
            return Err(SkeinError::Storage(format!(
                "Skein TurboQuant projection rejected empty or non-finite embedding for {}",
                document.id
            )));
        }
        match dimension {
            Some(existing) if existing != embedding.len() => {
                return Err(SkeinError::Storage(format!(
                    "Skein TurboQuant projection dimension mismatch: expected {existing}, got {}",
                    embedding.len()
                )));
            }
            Some(_) => {}
            None => dimension = Some(embedding.len()),
        }
        let numeric_id = stable_numeric_id(&document.id);
        if let Some(existing) = numeric_to_document_id.insert(numeric_id, document.id.clone()) {
            return Err(SkeinError::Storage(format!(
                "Skein TurboQuant projection id collision between {existing} and {}",
                document.id
            )));
        }
        document_to_numeric_id.insert(document.id.clone(), numeric_id);
    }
    Ok(dimension.map(|dimension| TurboQuantDocumentIdMap {
        dimension,
        numeric_to_document_id,
        document_to_numeric_id,
    }))
}

fn build_config(
    dimension: usize,
    identity: ProjectionIdentity,
    options: TurboQuantCandidateProjectionBuildOptions,
) -> ProjectionBuildConfig {
    ProjectionBuildConfig::new(dimension, identity)
        .with_segment_rows(options.segment_rows)
        .with_max_working_bytes(options.max_working_bytes)
        .with_transform_seed(options.transform_seed)
}

fn stable_numeric_id(document_id: &str) -> u64 {
    let mut hash = NUMERIC_ID_OFFSET;
    for byte in document_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(NUMERIC_ID_PRIME);
    }
    hash
}

fn projection_error(error: skein_vector_projection::ProjectionError) -> SkeinError {
    SkeinError::Storage(format!("Skein TurboQuant projection: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn turboquant_projection_round_trips_string_ids_and_raw_identity() {
        let documents = sample_documents();
        let root = unique_test_dir("roundtrip");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_turboquant.1.skein");
        let identity = ProjectionIdentity {
            generation: 1,
            source_epoch: Some(9),
            embedding_model: Some("test-model".to_string()),
            embedding_version: Some("v1".to_string()),
        };
        let written = TurboQuantCandidateProjection::write_from_documents(
            &artifact,
            &documents,
            identity.clone(),
            TurboQuantCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap();
        assert!(written.is_file_backed());

        let loaded =
            TurboQuantCandidateProjection::load_from_path(&artifact, &documents, &identity).unwrap();
        let output = loaded
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(output.candidates[0].id, "memory:a");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_allowlist_is_charged_to_the_search_budget() {
        let projection = TurboQuantCandidateProjection::build_from_documents(
            &sample_documents(),
            ProjectionIdentity::new(1),
            TurboQuantCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap();
        let allowed = ["memory:a", "memory:b"];
        let result = projection.search_with_options(
            &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            1,
            Some(&allowed),
            TurboQuantCandidateScanOptions {
                max_working_bytes: std::mem::size_of::<u64>(),
                ..TurboQuantCandidateScanOptions::default()
            },
        );

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("allowlist requires 16 bytes"));
    }

    #[test]
    fn unfiltered_candidate_scan_does_not_materialize_an_allowlist() {
        let projection = TurboQuantCandidateProjection::build_from_documents(
            &sample_documents(),
            ProjectionIdentity::new(1),
            TurboQuantCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap();
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let baseline = projection.search(&query, 1, None).unwrap();
        let exact_budget = baseline.report.admitted_working_bytes;

        projection
            .search_with_options(
                &query,
                1,
                None,
                TurboQuantCandidateScanOptions {
                    max_working_bytes: exact_budget,
                    ..TurboQuantCandidateScanOptions::default()
                },
            )
            .unwrap();
        let all_documents = ["memory:a", "memory:b"];
        let filtered = projection.search_with_options(
            &query,
            1,
            Some(&all_documents),
            TurboQuantCandidateScanOptions {
                max_working_bytes: exact_budget,
                ..TurboQuantCandidateScanOptions::default()
            },
        );
        assert!(filtered
            .unwrap_err()
            .to_string()
            .contains("resource budget exceeded"));
    }

    fn sample_documents() -> BTreeMap<String, SearchDocument> {
        [
            ("memory:a", vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ("memory:b", vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ]
        .into_iter()
        .map(|(id, embedding)| {
            (
                id.to_string(),
                SearchDocument {
                    id: id.to_string(),
                    title: id.to_string(),
                    content: String::new(),
                    embedding: Some(embedding),
                    metadata: BTreeMap::new(),
                },
            )
        })
        .collect()
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_turboquant_candidate_projection_{name}_{}_{nanos}",
            std::process::id()
        ))
    }
}
