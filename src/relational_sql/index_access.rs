use crate::error::{Result, SkeinError};
use crate::store::{
    GraphStore, RelationalIndexReadLimits, RelationalIndexReadViewBackendReport,
    RelationalIndexReadViewReport,
};
use skein_storage::{
    RelationalIndexShadowError, RelationalKey, RelationalRow, RelationalState,
    RELATIONAL_PRIMARY_INDEX_NAME,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationalIndexReadMode<'a> {
    Materialized,
    DemandPaged(&'a GraphStore),
    TransactionWorkspace,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RelationalIndexExecutionEvidence {
    pub table: String,
    pub index: String,
    pub lookups: usize,
    pub demand_paged_lookups: usize,
    pub canonical_fallback_lookups: usize,
    pub fallback_reasons: BTreeSet<&'static str>,
    pub base_generation: Option<u64>,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_set_digest: Option<String>,
    pub logical_pages: usize,
    pub logical_bytes: usize,
    pub file_pages: usize,
    pub file_bytes: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub delta_entries_visited: usize,
    pub live_batches_visited: usize,
    pub live_entries_visited: usize,
    pub live_entries_matched: usize,
    pub live_bytes_visited: usize,
    pub rows_visited: usize,
}

impl RelationalIndexExecutionEvidence {
    pub(crate) fn runtime_path(&self) -> &'static str {
        match (
            self.demand_paged_lookups != 0,
            self.canonical_fallback_lookups != 0,
        ) {
            (true, false) => "demand_paged",
            (false, true) => "canonical_fallback",
            (true, true) => "mixed",
            (false, false) => "not_executed",
        }
    }
}

pub(crate) struct RelationalIndexRuntime<'a> {
    mode: RelationalIndexReadMode<'a>,
    limits: RelationalIndexReadLimits,
    state: RefCell<RelationalIndexRuntimeState>,
}

#[derive(Default)]
struct RelationalIndexRuntimeState {
    logical_pages: usize,
    logical_bytes: usize,
    rows_visited: usize,
    evidence: BTreeMap<(String, String), RelationalIndexExecutionEvidence>,
}

struct RelationalIndexProbe<'input, 'state> {
    state: &'state RelationalState,
    table: &'input str,
    index: &'input str,
    prefix: &'input RelationalKey,
}

impl<'a> RelationalIndexRuntime<'a> {
    pub(crate) fn new(
        mode: RelationalIndexReadMode<'a>,
        limits: RelationalIndexReadLimits,
    ) -> Self {
        Self {
            mode,
            limits,
            state: RefCell::new(RelationalIndexRuntimeState::default()),
        }
    }

    pub(crate) fn evidence(&self) -> Vec<RelationalIndexExecutionEvidence> {
        self.state.borrow().evidence.values().cloned().collect()
    }

    pub(crate) fn visit_primary<'state>(
        &self,
        state: &'state RelationalState,
        table: &str,
        key: &RelationalKey,
        mut visit: impl FnMut(&'state RelationalKey, &'state RelationalRow) -> Result<bool>,
    ) -> Result<bool> {
        if matches!(self.mode, RelationalIndexReadMode::Materialized) {
            return match state.row_entry(table, key) {
                Some((key, row)) => visit(key, row),
                None => Ok(true),
            };
        }
        let fallback = |visit: &mut dyn FnMut(
            &'state RelationalKey,
            &'state RelationalRow,
        ) -> Result<bool>| {
            match state.row_entry(table, key) {
                Some((key, row)) => visit(key, row),
                None => Ok(true),
            }
        };
        self.visit_demand_or_fallback(
            RelationalIndexProbe {
                state,
                table,
                index: RELATIONAL_PRIMARY_INDEX_NAME,
                prefix: key,
            },
            &mut visit,
            fallback,
        )
    }

    pub(crate) fn visit_prefix<'state>(
        &self,
        state: &'state RelationalState,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'state RelationalKey, &'state RelationalRow) -> Result<bool>,
    ) -> Result<bool> {
        if matches!(self.mode, RelationalIndexReadMode::Materialized) {
            return visit_materialized_prefix(state, table, index, prefix, &mut visit);
        }
        let fallback = |visit: &mut dyn FnMut(
            &'state RelationalKey,
            &'state RelationalRow,
        ) -> Result<bool>| {
            visit_materialized_prefix(state, table, index, prefix, visit)
        };
        self.visit_demand_or_fallback(
            RelationalIndexProbe {
                state,
                table,
                index,
                prefix,
            },
            &mut visit,
            fallback,
        )
    }

    fn visit_demand_or_fallback<'input, 'state>(
        &self,
        probe: RelationalIndexProbe<'input, 'state>,
        visit: &mut dyn FnMut(&'state RelationalKey, &'state RelationalRow) -> Result<bool>,
        fallback: impl FnOnce(
            &mut dyn FnMut(&'state RelationalKey, &'state RelationalRow) -> Result<bool>,
        ) -> Result<bool>,
    ) -> Result<bool> {
        let RelationalIndexProbe {
            state,
            table,
            index,
            prefix,
        } = probe;
        let store = match self.mode {
            RelationalIndexReadMode::Materialized => unreachable!("handled by the caller"),
            RelationalIndexReadMode::TransactionWorkspace => {
                self.record_fallback(table, index, "transaction_workspace")?;
                return fallback(visit);
            }
            RelationalIndexReadMode::DemandPaged(store) => store,
        };
        let Some(remaining) = self.remaining_limits() else {
            self.record_fallback(table, index, "query_index_budget_exhausted")?;
            return fallback(visit);
        };
        let mut callback_error = None;
        let mut keep_going = true;
        let mut produced_provisional_rows = false;
        let attempt = store.visit_relational_index_read_view_prefix(
            table,
            index,
            prefix,
            remaining,
            |locator| {
                produced_provisional_rows = true;
                let Some((key, row)) = state.row_entry(table, locator) else {
                    callback_error = Some(SkeinError::StorageIntegrity(format!(
                        "relational index {index} on table {table} points to a missing row"
                    )));
                    return false;
                };
                match visit(key, row) {
                    Ok(continue_scan) => {
                        keep_going = continue_scan;
                        continue_scan
                    }
                    Err(error) => {
                        callback_error = Some(error);
                        false
                    }
                }
            },
        );
        if let Some(error) = callback_error {
            return Err(error);
        }
        match attempt {
            Some(Ok(report)) => {
                self.record_success(table, index, &report)?;
                Ok(keep_going)
            }
            Some(Err(RelationalIndexShadowError::Admission(_))) => {
                if produced_provisional_rows {
                    return Err(SkeinError::Execution(format!(
                        "relational index read for {table}.{index} exhausted admission after producing provisional row locators"
                    )));
                }
                self.record_fallback(table, index, "admission_rejected")?;
                fallback(visit)
            }
            Some(Err(RelationalIndexShadowError::MissingIndex { .. })) => {
                if produced_provisional_rows {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "relational index {table}.{index} disappeared after producing provisional row locators"
                    )));
                }
                self.record_fallback(table, index, "missing_index")?;
                fallback(visit)
            }
            Some(Err(error @ RelationalIndexShadowError::Corrupt(_))) => {
                Err(SkeinError::StorageIntegrity(format!(
                    "relational index read failed closed for {table}.{index}: {error}"
                )))
            }
            Some(Err(error @ RelationalIndexShadowError::Durability(_)))
            | Some(Err(error @ RelationalIndexShadowError::StaleGeneration { .. })) => {
                Err(SkeinError::StorageIntegrity(format!(
                    "relational index identity failed closed for {table}.{index}: {error}"
                )))
            }
            None => {
                self.record_fallback(table, index, "read_view_unavailable")?;
                fallback(visit)
            }
        }
    }

    fn remaining_limits(&self) -> Option<RelationalIndexReadLimits> {
        let state = self.state.borrow();
        Some(RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(
                self.limits
                    .max_pages
                    .get()
                    .checked_sub(state.logical_pages)?,
            )?,
            max_rows: NonZeroUsize::new(
                self.limits.max_rows.get().checked_sub(state.rows_visited)?,
            )?,
            max_bytes: NonZeroUsize::new(
                self.limits
                    .max_bytes
                    .get()
                    .checked_sub(state.logical_bytes)?,
            )?,
            max_tree_height: self.limits.max_tree_height,
        })
    }

    fn evidence_mut<'state>(
        state: &'state mut RelationalIndexRuntimeState,
        table: &str,
        index: &str,
    ) -> &'state mut RelationalIndexExecutionEvidence {
        state
            .evidence
            .entry((table.to_string(), index.to_string()))
            .or_insert_with(|| RelationalIndexExecutionEvidence {
                table: table.to_string(),
                index: index.to_string(),
                ..RelationalIndexExecutionEvidence::default()
            })
    }

    fn record_fallback(&self, table: &str, index: &str, reason: &'static str) -> Result<()> {
        let mut state = self.state.borrow_mut();
        let evidence = Self::evidence_mut(&mut state, table, index);
        evidence.lookups = checked_add(evidence.lookups, 1, "index lookup count")?;
        evidence.canonical_fallback_lookups = checked_add(
            evidence.canonical_fallback_lookups,
            1,
            "canonical fallback count",
        )?;
        evidence.fallback_reasons.insert(reason);
        Ok(())
    }

    fn record_success(
        &self,
        table: &str,
        index: &str,
        report: &RelationalIndexReadViewReport,
    ) -> Result<()> {
        let metrics = IndexReadMetrics::from_report(report)?;
        let mut state = self.state.borrow_mut();
        let logical_pages = checked_add(
            state.logical_pages,
            metrics.logical_pages,
            "query index page count",
        )?;
        let logical_bytes = checked_add(
            state.logical_bytes,
            metrics.logical_bytes,
            "query index byte count",
        )?;
        let rows_visited = checked_add(
            state.rows_visited,
            report.rows_visited,
            "query index row count",
        )?;
        if logical_pages > self.limits.max_pages.get()
            || logical_bytes > self.limits.max_bytes.get()
            || rows_visited > self.limits.max_rows.get()
        {
            return Err(SkeinError::Execution(format!(
                "relational index reads exceed the statement budget pages={}/{}, bytes={}/{}, rows={}/{}",
                logical_pages,
                self.limits.max_pages,
                logical_bytes,
                self.limits.max_bytes,
                rows_visited,
                self.limits.max_rows,
            )));
        }
        state.logical_pages = logical_pages;
        state.logical_bytes = logical_bytes;
        state.rows_visited = rows_visited;
        let evidence = Self::evidence_mut(&mut state, table, index);
        ensure_identity(evidence, report)?;
        evidence.lookups = checked_add(evidence.lookups, 1, "index lookup count")?;
        evidence.demand_paged_lookups = checked_add(
            evidence.demand_paged_lookups,
            1,
            "demand-paged lookup count",
        )?;
        metrics.accumulate(evidence)?;
        evidence.live_batches_visited = checked_add(
            evidence.live_batches_visited,
            report.live_batches_visited,
            "live batch count",
        )?;
        evidence.live_entries_visited = checked_add(
            evidence.live_entries_visited,
            report.live_entries_visited,
            "live entry count",
        )?;
        evidence.live_entries_matched = checked_add(
            evidence.live_entries_matched,
            report.live_entries_matched,
            "matched live entry count",
        )?;
        evidence.live_bytes_visited = checked_add(
            evidence.live_bytes_visited,
            report.live_bytes_visited,
            "live byte count",
        )?;
        evidence.rows_visited = checked_add(
            evidence.rows_visited,
            report.rows_visited,
            "index result row count",
        )?;
        Ok(())
    }
}

fn visit_materialized_prefix<'state>(
    state: &'state RelationalState,
    table: &str,
    index: &str,
    prefix: &RelationalKey,
    visit: &mut dyn FnMut(&'state RelationalKey, &'state RelationalRow) -> Result<bool>,
) -> Result<bool> {
    let mut error = None;
    let mut keep_going = true;
    state
        .visit_index_prefix_rows(table, index, prefix, |key, row| match visit(key, row) {
            Ok(continue_scan) => {
                keep_going = continue_scan;
                continue_scan
            }
            Err(candidate_error) => {
                error = Some(candidate_error);
                false
            }
        })
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "relational index {index} on table {table} is not materialized"
            ))
        })?;
    match error {
        Some(error) => Err(error),
        None => Ok(keep_going),
    }
}

fn ensure_identity(
    evidence: &mut RelationalIndexExecutionEvidence,
    report: &RelationalIndexReadViewReport,
) -> Result<()> {
    let observed = (
        Some(report.base_generation),
        report.delta_generation,
        Some(report.base_commit_epoch),
        Some(report.visible_commit_epoch),
        Some(report.root_set_digest.as_str()),
    );
    let expected = (
        evidence.base_generation,
        evidence.delta_generation,
        evidence.base_commit_epoch,
        evidence.visible_commit_epoch,
        evidence.root_set_digest.as_deref(),
    );
    if evidence.demand_paged_lookups != 0 && expected != observed {
        return Err(SkeinError::StorageIntegrity(
            "relational index view identity changed within one SQL statement".to_string(),
        ));
    }
    evidence.base_generation = observed.0;
    evidence.delta_generation = observed.1;
    evidence.base_commit_epoch = observed.2;
    evidence.visible_commit_epoch = observed.3;
    evidence.root_set_digest = observed.4.map(str::to_string);
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexReadMetrics {
    logical_pages: usize,
    logical_bytes: usize,
    file_pages: usize,
    file_bytes: usize,
    cache_hits: usize,
    cache_misses: usize,
    cache_admission_rejections: usize,
    delta_entries_visited: usize,
}

impl IndexReadMetrics {
    fn from_report(report: &RelationalIndexReadViewReport) -> Result<Self> {
        let mut metrics = match &report.backend {
            RelationalIndexReadViewBackendReport::Base(base) => Self {
                logical_pages: base.pages_read,
                logical_bytes: base.bytes_read,
                file_pages: base.file_pages_read,
                file_bytes: base.file_bytes_read,
                cache_hits: base.cache_hits,
                cache_misses: base.cache_misses,
                cache_admission_rejections: base.cache_admission_rejections,
                delta_entries_visited: 0,
            },
            RelationalIndexReadViewBackendReport::Recovered(recovered) => Self {
                logical_pages: checked_add(
                    recovered.base.pages_read,
                    recovered.delta_pages_read,
                    "recovered logical page count",
                )?,
                logical_bytes: checked_add(
                    recovered.base.bytes_read,
                    recovered.delta_bytes_read,
                    "recovered logical byte count",
                )?,
                file_pages: checked_add(
                    recovered.base.file_pages_read,
                    recovered.delta_file_pages_read,
                    "recovered file page count",
                )?,
                file_bytes: checked_add(
                    recovered.base.file_bytes_read,
                    recovered.delta_file_bytes_read,
                    "recovered file byte count",
                )?,
                cache_hits: checked_add(
                    recovered.base.cache_hits,
                    recovered.delta_cache_hits,
                    "recovered cache hit count",
                )?,
                cache_misses: checked_add(
                    recovered.base.cache_misses,
                    recovered.delta_cache_misses,
                    "recovered cache miss count",
                )?,
                cache_admission_rejections: checked_add(
                    recovered.base.cache_admission_rejections,
                    recovered.delta_cache_admission_rejections,
                    "recovered cache rejection count",
                )?,
                delta_entries_visited: recovered.delta_entries_visited,
            },
        };
        metrics.logical_bytes = checked_add(
            metrics.logical_bytes,
            report.live_bytes_visited,
            "logical bytes including live changes",
        )?;
        Ok(metrics)
    }

    fn accumulate(self, evidence: &mut RelationalIndexExecutionEvidence) -> Result<()> {
        evidence.logical_pages = checked_add(
            evidence.logical_pages,
            self.logical_pages,
            "logical page count",
        )?;
        evidence.logical_bytes = checked_add(
            evidence.logical_bytes,
            self.logical_bytes,
            "logical byte count",
        )?;
        evidence.file_pages =
            checked_add(evidence.file_pages, self.file_pages, "physical page count")?;
        evidence.file_bytes =
            checked_add(evidence.file_bytes, self.file_bytes, "physical byte count")?;
        evidence.cache_hits = checked_add(evidence.cache_hits, self.cache_hits, "cache hit count")?;
        evidence.cache_misses =
            checked_add(evidence.cache_misses, self.cache_misses, "cache miss count")?;
        evidence.cache_admission_rejections = checked_add(
            evidence.cache_admission_rejections,
            self.cache_admission_rejections,
            "cache admission rejection count",
        )?;
        evidence.delta_entries_visited = checked_add(
            evidence.delta_entries_visited,
            self.delta_entries_visited,
            "delta entry count",
        )?;
        Ok(())
    }
}

fn checked_add(left: usize, right: usize, counter: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| SkeinError::StorageIntegrity(format!("relational {counter} overflow")))
}
