//! Un-indexed writes held ahead of an immutable base projection.
//!
//! `InMemoryProjection`/`FileProjection` are built once (via `ProjectionBuilder`
//! / `ProjectionWriter`) and have no update path: every row is an immutable
//! TurboQuant-quantized artifact. `DeltaBuffer` holds recent upserts that
//! have not yet been folded into that base, scored by exact cosine
//! similarity at query time rather than quantized -- the same
//! immutable-base-plus-un-indexed-delta shape used by comparable systems
//! (an append-only base segment with a flat-scanned growing buffer on top).
//!
//! `DeltaBuffer` does not attempt to fold itself into the base: TurboQuant
//! encoding is lossy, so a base row's original vector cannot be recovered
//! from its quantized codes. Folding means the host re-running the existing
//! full-rebuild path (`ProjectionBuilder`/`ProjectionWriter`) over canonical
//! storage for base plus delta, then calling `clear()`. `should_optimize`
//! is the signal for when that is due.
//!
//! `benches/vector_projection_delta_fraction.rs` measured that signal's
//! cost: the delta's exact per-row scan (unquantized, no SIMD, no block
//! skipping) is markedly more expensive per row than the base's quantized
//! scan, so query latency grows fast well before the delta is large --
//! roughly double at just 1% of the base's document count, on an 8,192-row
//! base at dimension 384. `DEFAULT_OPTIMIZE_THRESHOLD` is set low (2%)
//! accordingly; callers whose delta scan is cheaper relative to their base
//! (smaller dimension, larger base) or who can tolerate more latency
//! between rebuilds can raise it via `with_optimize_threshold`.

use crate::error::{ProjectionError, Result};
use crate::scan::{compare_best, ProjectionHit, ProjectionSearchOptions, ProjectionSearchOutput};
use std::collections::HashSet;

const DEFAULT_OPTIMIZE_THRESHOLD: f64 = 0.02;

#[derive(Debug, Clone)]
struct DeltaEntry {
    id: u64,
    vector: Vec<f32>,
}

/// A small, unindexed set of upserted vectors awaiting a full rebuild.
#[derive(Debug, Clone)]
pub struct DeltaBuffer {
    dimension: usize,
    optimize_threshold: f64,
    entries: Vec<DeltaEntry>,
}

impl DeltaBuffer {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            optimize_threshold: DEFAULT_OPTIMIZE_THRESHOLD,
            entries: Vec::new(),
        }
    }

    /// Fraction of combined base+delta document count at which
    /// `should_optimize` starts returning true. Default 0.02 (2%); see the
    /// module docs for the benchmark this default was calibrated from.
    pub fn with_optimize_threshold(mut self, optimize_threshold: f64) -> Self {
        self.optimize_threshold = optimize_threshold;
        self
    }

    /// Insert `id`, or replace its vector if already present. Re-upserting
    /// an id already in the buffer keeps the buffer's size unchanged.
    pub fn upsert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected dimension {}, got {}",
                self.dimension,
                vector.len()
            )));
        }
        if !vector.iter().all(|value| value.is_finite()) {
            return Err(ProjectionError::InvalidVector(
                "coordinates must be finite".to_string(),
            ));
        }
        match self.entries.binary_search_by_key(&id, |entry| entry.id) {
            Ok(index) => vector.clone_into(&mut self.entries[index].vector),
            Err(index) => self.entries.insert(
                index,
                DeltaEntry {
                    id,
                    vector: vector.to_vec(),
                },
            ),
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Fraction of `base_document_count + len()` this buffer represents.
    pub fn fraction_of(&self, base_document_count: usize) -> f64 {
        let total = base_document_count.saturating_add(self.entries.len());
        if total == 0 {
            0.0
        } else {
            self.entries.len() as f64 / total as f64
        }
    }

    /// Whether the buffer has grown past its configured optimize threshold
    /// relative to `base_document_count` and the host should fold it into
    /// a fresh base rebuild.
    pub fn should_optimize(&self, base_document_count: usize) -> bool {
        self.fraction_of(base_document_count) >= self.optimize_threshold
    }

    /// Exact cosine-similarity scan, comparable to the base projection's
    /// approximate TurboQuant score (see module docs): both approximate
    /// the same cosine-similarity metric, since indexed and query vectors
    /// are unit-normalized before TurboQuant's orthogonal transform.
    fn scan(
        &self,
        query: &[f32],
        top_k: usize,
        allowed_ids: Option<&[u64]>,
    ) -> Result<Vec<ProjectionHit>> {
        if query.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected query dimension {}, got {}",
                self.dimension,
                query.len()
            )));
        }
        let query_norm = l2_norm(query);
        let mut hits: Vec<ProjectionHit> = self
            .entries
            .iter()
            .filter(|entry| {
                allowed_ids.is_none_or(|allowed| allowed.binary_search(&entry.id).is_ok())
            })
            .filter_map(|entry| {
                cosine_similarity(query, query_norm, &entry.vector).map(|score| ProjectionHit {
                    id: entry.id,
                    score,
                })
            })
            .collect();
        hits.sort_by(|left, right| compare_best(right, left));
        hits.truncate(top_k);
        Ok(hits)
    }
}

fn l2_norm(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt()
}

fn cosine_similarity(query: &[f32], query_norm: f64, vector: &[f32]) -> Option<f32> {
    let vector_norm = l2_norm(vector);
    if query_norm <= f64::EPSILON || vector_norm <= f64::EPSILON {
        return None;
    }
    let dot = query
        .iter()
        .zip(vector.iter())
        .map(|(query_value, vector_value)| f64::from(*query_value) * f64::from(*vector_value))
        .sum::<f64>();
    Some((dot / (query_norm * vector_norm)) as f32)
}

/// Result of merging a base projection search with a `DeltaBuffer` scan.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaMergedSearchOutput {
    pub hits: Vec<ProjectionHit>,
    pub base: ProjectionSearchOutput,
    pub delta_document_count: usize,
    /// `delta_document_count / (base.report.document_count + delta_document_count)`.
    pub delta_fraction: f64,
}

/// Search a base projection and a `DeltaBuffer` together, letting delta
/// entries shadow any base row sharing the same id (the delta value is
/// always the more recent write). `base_search` is expected to be
/// `|q, k, o| base.search(q, k, o)` for whichever `InMemoryProjection` or
/// `FileProjection` is being queried.
pub fn search_with_delta<F>(
    query: &[f32],
    top_k: usize,
    options: ProjectionSearchOptions<'_>,
    delta: &DeltaBuffer,
    base_search: F,
) -> Result<DeltaMergedSearchOutput>
where
    F: FnOnce(&[f32], usize, ProjectionSearchOptions<'_>) -> Result<ProjectionSearchOutput>,
{
    // Ask the base for extra headroom equal to the delta size, so that
    // filtering out ids the delta shadows still leaves up to top_k results
    // whenever the base has that many non-shadowed candidates.
    let base_top_k = top_k.saturating_add(delta.len());
    let base_output = base_search(query, base_top_k, options)?;
    let delta_hits = delta.scan(query, top_k, options.allowed_ids)?;

    let delta_ids: HashSet<u64> = delta.entries.iter().map(|entry| entry.id).collect();
    let mut hits: Vec<ProjectionHit> = base_output
        .hits
        .iter()
        .filter(|hit| !delta_ids.contains(&hit.id))
        .cloned()
        .collect();
    hits.extend(delta_hits);
    hits.sort_by(|left, right| compare_best(right, left));
    hits.truncate(top_k);

    let delta_document_count = delta.len();
    let total_document_count = base_output
        .report
        .document_count
        .saturating_add(delta_document_count);
    let delta_fraction = if total_document_count == 0 {
        0.0
    } else {
        delta_document_count as f64 / total_document_count as f64
    };
    Ok(DeltaMergedSearchOutput {
        hits,
        base: base_output,
        delta_document_count,
        delta_fraction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity};

    fn axis_vector(dimension: usize, axis: usize) -> Vec<f32> {
        let mut vector = vec![0.0; dimension];
        vector[axis] = 1.0;
        vector
    }

    fn sample_base() -> crate::InMemoryProjection {
        let config = ProjectionBuildConfig::new(4, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        builder.push(10, &axis_vector(4, 0)).unwrap();
        builder.push(20, &axis_vector(4, 1)).unwrap();
        builder.finish().unwrap()
    }

    #[test]
    fn delta_hit_shadows_stale_base_row_with_the_same_id() {
        let base = sample_base();
        let mut delta = DeltaBuffer::new(4);
        // id 10 is re-upserted in the delta pointing at a different axis;
        // the merged result must reflect the delta's value, not the base's.
        delta.upsert(10, &axis_vector(4, 3)).unwrap();

        let query = axis_vector(4, 3);
        let output = search_with_delta(
            &query,
            2,
            ProjectionSearchOptions::new().with_kernel(crate::KernelPreference::Scalar),
            &delta,
            |q, k, o| base.search(q, k, o),
        )
        .unwrap();

        assert_eq!(output.hits[0].id, 10);
        assert!(output.hits[0].score > 0.99);
        assert_eq!(output.delta_document_count, 1);
    }

    #[test]
    fn empty_delta_matches_plain_base_search() {
        let base = sample_base();
        let delta = DeltaBuffer::new(4);
        let query = axis_vector(4, 1);

        let output = search_with_delta(
            &query,
            2,
            ProjectionSearchOptions::new().with_kernel(crate::KernelPreference::Scalar),
            &delta,
            |q, k, o| base.search(q, k, o),
        )
        .unwrap();

        assert_eq!(output.hits[0].id, 20);
        assert_eq!(output.delta_document_count, 0);
        assert_eq!(output.delta_fraction, 0.0);
    }

    #[test]
    fn should_optimize_trips_at_the_configured_threshold() {
        let mut delta = DeltaBuffer::new(4).with_optimize_threshold(0.25);
        for id in 0..3 {
            delta.upsert(id, &axis_vector(4, 0)).unwrap();
        }
        // 3 / (9 + 3) = 0.25 -- exactly at threshold.
        assert!(delta.should_optimize(9));
        assert!(!delta.should_optimize(20));
    }

    #[test]
    fn upsert_rejects_dimension_mismatch() {
        let mut delta = DeltaBuffer::new(4);
        let result = delta.upsert(1, &[0.0, 1.0]);
        assert!(matches!(result, Err(ProjectionError::InvalidVector(_))));
    }
}
