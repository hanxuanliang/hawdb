//! Bounded demand reads over one immutable relational row-root generation.

use super::{
    validate_requested_fields, RelationalProjectedField, RelationalProjectedRow,
    RelationalRowPageError, RelationalRowPagePublicationError, RelationalRowPageRootDescriptor,
    RelationalRowPageRootReader, RelationalRowPageView,
};
use crate::relational::{
    ordered_key::{decode_ordered_relational_key, encode_ordered_relational_key},
    RelationalHydrationBudget, RelationalKey, RelationalOverflowPublicationError,
    RelationalOverflowRootReader, RelationalValue,
};
use crate::{SegmentCache, StoreId};
use skein_core::{RuntimeCancellationReason, RuntimeTaskContext};
use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES: usize = 16;
pub const DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS: usize = 4096;
pub const DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS: usize = 1;
pub const DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageDemandReadLimits {
    pub max_pages: NonZeroUsize,
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
    pub max_pins: NonZeroUsize,
    pub max_tree_height: NonZeroU32,
}

#[derive(Debug, Clone, Copy)]
pub struct RelationalRowPageProjectedRange<'a> {
    pub table: &'a str,
    pub lower: Bound<&'a RelationalKey>,
    pub upper: Bound<&'a RelationalKey>,
    pub requested_fields: &'a [usize],
}

pub(super) struct RelationalRowPageOverlayPoint<'a> {
    pub table: &'a str,
    pub primary_key: RelationalKey,
    pub value: RelationalRowPageProjectedOverlayValue,
    pub overflow_root: Option<&'a RelationalOverflowRootReader>,
}

pub(super) struct RelationalRowPageOverlayRange<'a> {
    pub rows: BTreeMap<RelationalKey, RelationalRowPageProjectedOverlayValue>,
    pub overflow_root: Option<&'a RelationalOverflowRootReader>,
}

pub(super) struct RelationalRowPageOverlayRead<'a> {
    pub range: RelationalRowPageProjectedRange<'a>,
    pub limits: RelationalRowPageDemandReadLimits,
    pub overlay: RelationalRowPageOverlayRange<'a>,
}

type ProjectedRowResolver<'a> = dyn FnMut(
        &mut RelationalProjectedRow,
        &mut RelationalHydrationBudget,
        &RuntimeTaskContext,
    ) -> Result<(), RelationalRowPageDemandReadError>
    + 'a;

#[derive(Debug)]
pub(super) enum RelationalRowPageProjectedOverlayValue {
    Present(Box<[RelationalProjectedField]>),
    Deleted,
}

impl Default for RelationalRowPageDemandReadLimits {
    fn default() -> Self {
        Self {
            max_pages: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES)
                .expect("default relational row read page limit is non-zero"),
            max_rows: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS)
                .expect("default relational row read row limit is non-zero"),
            max_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES)
                .expect("default relational row read byte limit is non-zero"),
            max_pins: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS)
                .expect("default relational row read pin limit is non-zero"),
            max_tree_height: NonZeroU32::new(DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT)
                .expect("default relational row read tree-height limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalRowPageDemandReadReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub descriptor_reads: usize,
    pub pages_read: usize,
    pub bytes_read: usize,
    pub file_pages_read: usize,
    pub file_bytes_read: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub rows_decoded: usize,
    pub rows_emitted: usize,
    pub hydrated_values: usize,
    pub compressed_hydration_bytes: usize,
    pub decompressed_hydration_bytes: usize,
    pub peak_pins: usize,
    pub stopped_early: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageDemandReadError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingTable(String),
    Stopped(RuntimeCancellationReason),
}

impl fmt::Display for RelationalRowPageDemandReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational row demand-read admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row demand reader: {message}")
            }
            Self::Durability(message) => {
                write!(
                    formatter,
                    "relational row demand-read durability failed: {message}"
                )
            }
            Self::MissingTable(table) => {
                write!(
                    formatter,
                    "relational row demand reader has no table {table}"
                )
            }
            Self::Stopped(reason) => {
                write!(formatter, "relational row demand read stopped: {reason}")
            }
        }
    }
}

impl std::error::Error for RelationalRowPageDemandReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            _ => None,
        }
    }
}

pub struct RelationalRowPageDemandReader {
    root: Arc<RelationalRowPageRootReader>,
    overflow: Arc<RelationalOverflowRootReader>,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    poisoned: AtomicBool,
}

impl fmt::Debug for RelationalRowPageDemandReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalRowPageDemandReader")
            .field("generation", &self.root.manifest().generation)
            .field(
                "source_commit_epoch",
                &self.root.manifest().source_commit_epoch,
            )
            .field("poisoned", &self.is_poisoned())
            .finish_non_exhaustive()
    }
}

impl RelationalRowPageDemandReader {
    /// Opens a demand reader pinned to one exact row/overflow root pair.
    ///
    /// The caller owns snapshot selection. This reader never follows an
    /// independent latest selector and therefore cannot cross generations
    /// during a point or range read.
    pub fn new(
        root: Arc<RelationalRowPageRootReader>,
        overflow: Arc<RelationalOverflowRootReader>,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalRowPageDemandReadError> {
        root.validate_overflow_root(&overflow)
            .map_err(map_row_publication_error)?;
        Ok(Self {
            root,
            overflow,
            cache,
            store_id,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn generation(&self) -> u64 {
        self.root.manifest().generation
    }

    pub fn source_commit_epoch(&self) -> u64 {
        self.root.manifest().source_commit_epoch
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Reads one primary-key row and decodes only the requested field ordinals.
    ///
    /// Overflow values are hydrated only when their field ordinal is present
    /// in `requested_fields`.
    pub fn point_projected(
        &self,
        table: &str,
        primary_key: &RelationalKey,
        requested_fields: &[usize],
        limits: RelationalRowPageDemandReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageDemandReadReport,
        ),
        RelationalRowPageDemandReadError,
    > {
        let mut context = DemandReadContext::new(self, limits, hydration, task)?;
        let table_root = context.table_root(table)?;
        context
            .validate_requested_fields(requested_fields, table_root.column_count.get() as usize)?;
        context.admit_descriptor_search(table_root.page_count)?;
        let (descriptor, descriptor_reads) = self
            .root
            .find_table_page_descriptor_accounted(table, primary_key)
            .map_err(|error| context.map_row_publication_error(error))?;
        context.add_descriptor_reads(descriptor_reads)?;
        let Some((_ordinal, descriptor)) = descriptor else {
            return Ok((None, context.finish()));
        };
        let encoded_key = encode_ordered_relational_key(primary_key)
            .map_err(|error| RelationalRowPageDemandReadError::Admission(error.to_string()))?;
        if encoded_key.as_slice() < descriptor.lower_bound.as_slice()
            || encoded_key.as_slice() > descriptor.upper_bound.as_slice()
        {
            return Ok((None, context.finish()));
        }
        let bytes = context.read_page(&descriptor)?;
        let view =
            RelationalRowPageView::open_slot(&bytes, self.root.publication_config().page_limits)
                .map_err(|error| context.map_page_error(error))?;
        context.validate_column_count(&table_root, &view)?;
        let Some(mut row) = view
            .find_projected_row(primary_key, requested_fields)
            .map_err(|error| context.map_page_error(error))?
        else {
            context.checkpoint()?;
            return Ok((None, context.finish()));
        };
        context.checkpoint()?;
        context.admit_row()?;
        context.hydrate_projected_row(&mut row)?;
        context.report.rows_decoded += 1;
        context.report.rows_emitted += 1;
        Ok((Some(row), context.finish()))
    }

    pub(super) fn point_projected_overlay(
        &self,
        point: RelationalRowPageOverlayPoint<'_>,
        limits: RelationalRowPageDemandReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageDemandReadReport,
        ),
        RelationalRowPageDemandReadError,
    > {
        let RelationalRowPageOverlayPoint {
            table,
            primary_key,
            value,
            overflow_root,
        } = point;
        let mut context = DemandReadContext::new(self, limits, hydration, task)?;
        context.table_root(table)?;
        let mut output = None;
        let mut resolve = |_: &mut RelationalProjectedRow,
                           _: &mut RelationalHydrationBudget,
                           _: &RuntimeTaskContext| { Ok(()) };
        emit_overlay_row(
            &mut context,
            primary_key,
            value,
            overflow_root,
            &mut resolve,
            &mut |row| {
                output = Some(row);
                true
            },
        )?;
        Ok((output, context.finish()))
    }

    /// Visits an ordered primary-key range without collecting its rows.
    ///
    /// Rows observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard their effects if a later page fails. The
    /// callback may return `false` to stop before loading the remaining pages.
    pub fn visit_projected_range(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageDemandReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        visit: impl FnMut(RelationalProjectedRow) -> bool,
    ) -> Result<RelationalRowPageDemandReadReport, RelationalRowPageDemandReadError> {
        let mut resolve = |_: &mut RelationalProjectedRow,
                           _: &mut RelationalHydrationBudget,
                           _: &RuntimeTaskContext| { Ok(()) };
        self.visit_projected_range_with_overlay(
            RelationalRowPageOverlayRead {
                range,
                limits,
                overlay: RelationalRowPageOverlayRange {
                    rows: BTreeMap::new(),
                    overflow_root: None,
                },
            },
            hydration,
            task,
            &mut resolve,
            visit,
        )
    }

    pub(super) fn visit_projected_range_with_overlay(
        &self,
        read: RelationalRowPageOverlayRead<'_>,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        resolve: &mut ProjectedRowResolver<'_>,
        mut visit: impl FnMut(RelationalProjectedRow) -> bool,
    ) -> Result<RelationalRowPageDemandReadReport, RelationalRowPageDemandReadError> {
        let RelationalRowPageOverlayRead {
            range,
            limits,
            overlay,
        } = read;
        let RelationalRowPageProjectedRange {
            table,
            lower,
            upper,
            requested_fields,
        } = range;
        let range = EncodedRange::new(lower, upper)?;
        let mut context = DemandReadContext::new(self, limits, hydration, task)?;
        if range.empty {
            return Ok(context.finish());
        }
        let table_root = context.table_root(table)?;
        let column_count = table_root.column_count.get() as usize;
        context.validate_requested_fields(requested_fields, column_count)?;
        let has_overlay = !overlay.rows.is_empty();
        let overlay_overflow = overlay.overflow_root;
        let mut overlay = overlay.rows.into_iter().peekable();
        if table_root.page_count == 0 {
            emit_remaining_overlay(
                &mut context,
                &mut overlay,
                overlay_overflow,
                resolve,
                &mut visit,
            )?;
            return Ok(context.finish());
        }

        let (mut ordinal, mut first_descriptor) = match lower {
            Bound::Unbounded => (0, None),
            Bound::Included(primary_key) | Bound::Excluded(primary_key) => {
                context.admit_descriptor_search(table_root.page_count)?;
                let (descriptor, descriptor_reads) = self
                    .root
                    .find_table_page_descriptor_accounted(table, primary_key)
                    .map_err(|error| context.map_row_publication_error(error))?;
                context.add_descriptor_reads(descriptor_reads)?;
                let Some((ordinal, descriptor)) = descriptor else {
                    if !emit_remaining_overlay(
                        &mut context,
                        &mut overlay,
                        overlay_overflow,
                        resolve,
                        &mut visit,
                    )? {
                        context.report.stopped_early = true;
                    }
                    return Ok(context.finish());
                };
                (ordinal, Some(descriptor))
            }
        };
        if first_descriptor
            .as_ref()
            .is_some_and(|descriptor| range.page_is_before_lower(descriptor))
        {
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                RelationalRowPageDemandReadError::Corrupt(
                    "relational row range page ordinal overflow".to_string(),
                )
            })?;
            first_descriptor = None;
        }

        let mut apply_lower_bound = !matches!(lower, Bound::Unbounded);
        while ordinal < table_root.page_count {
            context.checkpoint()?;
            let descriptor = match first_descriptor.take() {
                Some(descriptor) => descriptor,
                None => {
                    let descriptor = self
                        .root
                        .read_table_page_descriptor(table, ordinal)
                        .map_err(|error| context.map_row_publication_error(error))?;
                    context.add_descriptor_reads(1)?;
                    descriptor
                }
            };
            if range.page_is_past_upper(&descriptor) {
                break;
            }
            let bytes = context.read_page(&descriptor)?;
            let view = RelationalRowPageView::open_slot(
                &bytes,
                self.root.publication_config().page_limits,
            )
            .map_err(|error| context.map_page_error(error))?;
            context.validate_column_count(&table_root, &view)?;
            let row_start = if apply_lower_bound {
                apply_lower_bound = false;
                match lower {
                    Bound::Included(primary_key) => view
                        .lower_bound_row(primary_key, true)
                        .map_err(|error| context.map_page_error(error))?,
                    Bound::Excluded(primary_key) => view
                        .lower_bound_row(primary_key, false)
                        .map_err(|error| context.map_page_error(error))?,
                    Bound::Unbounded => 0,
                }
            } else {
                0
            };
            for row_ordinal in row_start..view.row_count() {
                context.checkpoint()?;
                let encoded_key = view
                    .key(row_ordinal)
                    .map_err(|error| context.map_page_error(error))?;
                if range.key_is_past_upper(encoded_key) {
                    if !emit_remaining_overlay(
                        &mut context,
                        &mut overlay,
                        overlay_overflow,
                        resolve,
                        &mut visit,
                    )? {
                        context.report.stopped_early = true;
                    }
                    return Ok(context.finish());
                }
                if !has_overlay {
                    context.admit_row()?;
                    let mut row = view
                        .decode_projected_row(row_ordinal, requested_fields)
                        .map_err(|error| context.map_page_error(error))?;
                    context.hydrate_projected_row(&mut row)?;
                    resolve(&mut row, context.hydration, context.task)?;
                    context.report.rows_decoded += 1;
                    context.report.rows_emitted += 1;
                    if !visit(row) {
                        context.report.stopped_early = true;
                        return Ok(context.finish());
                    }
                    continue;
                }
                let primary_key = decode_ordered_relational_key(encoded_key).map_err(|error| {
                    context.corrupt(format!(
                        "row {row_ordinal} primary key cannot be decoded: {error}"
                    ))
                })?;
                if !emit_overlay_before(
                    &mut context,
                    &mut overlay,
                    &primary_key,
                    overlay_overflow,
                    resolve,
                    &mut visit,
                )? {
                    context.report.stopped_early = true;
                    return Ok(context.finish());
                }
                if overlay
                    .peek()
                    .is_some_and(|(overlay_key, _)| overlay_key == &primary_key)
                {
                    let (overlay_key, overlay_value) =
                        overlay.next().expect("peeked overlay entry exists");
                    if !emit_overlay_row(
                        &mut context,
                        overlay_key,
                        overlay_value,
                        overlay_overflow,
                        resolve,
                        &mut visit,
                    )? {
                        context.report.stopped_early = true;
                        return Ok(context.finish());
                    }
                    continue;
                }
                context.admit_row()?;
                let mut row = view
                    .decode_projected_row(row_ordinal, requested_fields)
                    .map_err(|error| context.map_page_error(error))?;
                context.hydrate_projected_row(&mut row)?;
                resolve(&mut row, context.hydration, context.task)?;
                context.report.rows_decoded += 1;
                context.report.rows_emitted += 1;
                if !visit(row) {
                    context.report.stopped_early = true;
                    return Ok(context.finish());
                }
            }
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                RelationalRowPageDemandReadError::Corrupt(
                    "relational row range page ordinal overflow".to_string(),
                )
            })?;
        }
        if !emit_remaining_overlay(
            &mut context,
            &mut overlay,
            overlay_overflow,
            resolve,
            &mut visit,
        )? {
            context.report.stopped_early = true;
        }
        Ok(context.finish())
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }
}

struct DemandReadContext<'a> {
    reader: &'a RelationalRowPageDemandReader,
    limits: RelationalRowPageDemandReadLimits,
    hydration: &'a mut RelationalHydrationBudget,
    hydration_start_compressed: usize,
    hydration_start_decompressed: usize,
    task: &'a RuntimeTaskContext,
    report: RelationalRowPageDemandReadReport,
}

impl<'a> DemandReadContext<'a> {
    fn new(
        reader: &'a RelationalRowPageDemandReader,
        limits: RelationalRowPageDemandReadLimits,
        hydration: &'a mut RelationalHydrationBudget,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self, RelationalRowPageDemandReadError> {
        if reader.is_poisoned() {
            return Err(RelationalRowPageDemandReadError::Corrupt(
                "relational row demand reader is poisoned".to_string(),
            ));
        }
        task.checkpoint()
            .map_err(RelationalRowPageDemandReadError::Stopped)?;
        Ok(Self {
            reader,
            limits,
            hydration_start_compressed: hydration.compressed_bytes,
            hydration_start_decompressed: hydration.decompressed_bytes,
            hydration,
            task,
            report: RelationalRowPageDemandReadReport {
                generation: reader.generation(),
                source_commit_epoch: reader.source_commit_epoch(),
                ..RelationalRowPageDemandReadReport::default()
            },
        })
    }

    fn table_root(
        &self,
        table: &str,
    ) -> Result<super::RelationalRowPageTableRoot, RelationalRowPageDemandReadError> {
        self.reader
            .root
            .table_root(table)
            .cloned()
            .map_err(|error| self.map_row_publication_error(error))
    }

    fn checkpoint(&self) -> Result<(), RelationalRowPageDemandReadError> {
        if self.reader.is_poisoned() {
            return Err(RelationalRowPageDemandReadError::Corrupt(
                "relational row demand reader is poisoned".to_string(),
            ));
        }
        self.task
            .checkpoint()
            .map_err(RelationalRowPageDemandReadError::Stopped)
    }

    fn validate_requested_fields(
        &self,
        requested_fields: &[usize],
        column_count: usize,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        validate_requested_fields(
            requested_fields,
            column_count,
            self.reader.root.publication_config().page_limits,
        )
        .map_err(|error| self.map_page_error(error))
    }

    fn corrupt(&self, message: String) -> RelationalRowPageDemandReadError {
        self.reader.poison();
        RelationalRowPageDemandReadError::Corrupt(message)
    }

    fn validate_column_count(
        &self,
        table: &super::RelationalRowPageTableRoot,
        page: &RelationalRowPageView<'_>,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        validate_table_column_count(table, page)
    }

    fn admit_descriptor_search(
        &self,
        page_count: u64,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        let height = descriptor_search_read_bound(page_count);
        if height > self.limits.max_tree_height.get() {
            return Err(RelationalRowPageDemandReadError::Admission(format!(
                "row-root descriptor search height {height} exceeds limit {}",
                self.limits.max_tree_height
            )));
        }
        Ok(())
    }

    fn add_descriptor_reads(
        &mut self,
        reads: usize,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        self.report.descriptor_reads =
            self.report
                .descriptor_reads
                .checked_add(reads)
                .ok_or_else(|| {
                    RelationalRowPageDemandReadError::Admission(
                        "row-root descriptor read counter overflow".to_string(),
                    )
                })?;
        Ok(())
    }

    fn read_page(
        &mut self,
        descriptor: &RelationalRowPageRootDescriptor,
    ) -> Result<Arc<[u8]>, RelationalRowPageDemandReadError> {
        self.checkpoint()?;
        if self.report.pages_read >= self.limits.max_pages.get() {
            return Err(RelationalRowPageDemandReadError::Admission(format!(
                "relational row demand read exceeds page limit {}",
                self.limits.max_pages
            )));
        }
        let page_bytes = self
            .reader
            .root
            .publication_config()
            .page_limits
            .max_page_bytes
            .get();
        let next_bytes = self
            .report
            .bytes_read
            .checked_add(page_bytes)
            .ok_or_else(|| {
                RelationalRowPageDemandReadError::Admission(
                    "relational row demand-read byte counter overflow".to_string(),
                )
            })?;
        if next_bytes > self.limits.max_bytes.get() {
            return Err(RelationalRowPageDemandReadError::Admission(format!(
                "relational row demand read needs {next_bytes} bytes, exceeding byte limit {}",
                self.limits.max_bytes
            )));
        }
        let read = self
            .reader
            .root
            .read_page_slot_accounted(descriptor, &self.reader.cache, self.reader.store_id)
            .map_err(|error| self.map_row_publication_error(error))?;
        self.report.pages_read += 1;
        self.report.bytes_read = next_bytes;
        self.report.cache_hits += usize::from(read.cache_hit);
        self.report.cache_misses += usize::from(read.cache_miss);
        self.report.cache_admission_rejections += usize::from(read.cache_admission_rejected);
        self.report.peak_pins = self.report.peak_pins.max(1);
        if !read.cache_hit {
            self.report.file_pages_read += 1;
            self.report.file_bytes_read = self
                .report
                .file_bytes_read
                .checked_add(page_bytes)
                .ok_or_else(|| {
                    RelationalRowPageDemandReadError::Admission(
                        "relational row file-byte counter overflow".to_string(),
                    )
                })?;
        }
        Ok(read.bytes)
    }

    fn admit_row(&self) -> Result<(), RelationalRowPageDemandReadError> {
        if self.report.rows_decoded >= self.limits.max_rows.get() {
            return Err(RelationalRowPageDemandReadError::Admission(format!(
                "relational row demand read exceeds row limit {}",
                self.limits.max_rows
            )));
        }
        Ok(())
    }

    fn hydrate_projected_row(
        &mut self,
        row: &mut RelationalProjectedRow,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        let overflow = Arc::clone(&self.reader.overflow);
        self.hydrate_projected_row_from(row, &overflow)
    }

    fn hydrate_projected_row_from(
        &mut self,
        row: &mut RelationalProjectedRow,
        overflow: &RelationalOverflowRootReader,
    ) -> Result<(), RelationalRowPageDemandReadError> {
        let hydration_count = row
            .fields
            .iter()
            .filter(|field| matches!(field.value, RelationalValue::Overflow(_)))
            .count();
        let next_hydrated_values = self
            .report
            .hydrated_values
            .checked_add(hydration_count)
            .ok_or_else(|| {
                RelationalRowPageDemandReadError::Admission(
                    "relational row hydration counter overflow".to_string(),
                )
            })?;
        let mut staged_budget = *self.hydration;
        if hydration_count != 0 {
            if staged_budget.hydrated_rows >= staged_budget.max_rows {
                return Err(RelationalRowPageDemandReadError::Admission(format!(
                    "relational hydration exceeds max_rows {}",
                    staged_budget.max_rows
                )));
            }
            staged_budget.hydrated_rows += 1;
        }
        for field in &mut row.fields {
            let RelationalValue::Overflow(reference) = &field.value else {
                continue;
            };
            self.checkpoint()?;
            let hydrated = match overflow.hydrate(reference, &mut staged_budget, Some(self.task)) {
                Ok(value) => value,
                Err(error) => {
                    self.checkpoint()?;
                    return Err(self.map_overflow_error(error));
                }
            };
            self.checkpoint()?;
            field.value = hydrated;
        }
        *self.hydration = staged_budget;
        self.report.hydrated_values = next_hydrated_values;
        Ok(())
    }

    fn finish(mut self) -> RelationalRowPageDemandReadReport {
        self.report.compressed_hydration_bytes = self
            .hydration
            .compressed_bytes
            .saturating_sub(self.hydration_start_compressed);
        self.report.decompressed_hydration_bytes = self
            .hydration
            .decompressed_bytes
            .saturating_sub(self.hydration_start_decompressed);
        self.report
    }

    fn map_page_error(&self, error: RelationalRowPageError) -> RelationalRowPageDemandReadError {
        let mapped = match error {
            RelationalRowPageError::Admission(message) => {
                RelationalRowPageDemandReadError::Admission(message)
            }
            RelationalRowPageError::Corrupt(message) => {
                RelationalRowPageDemandReadError::Corrupt(message)
            }
        };
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_row_publication_error(
        &self,
        error: RelationalRowPagePublicationError,
    ) -> RelationalRowPageDemandReadError {
        let mapped = map_row_publication_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_overflow_error(
        &self,
        error: RelationalOverflowPublicationError,
    ) -> RelationalRowPageDemandReadError {
        let mapped = match error {
            RelationalOverflowPublicationError::Admission(message) => {
                RelationalRowPageDemandReadError::Admission(message)
            }
            RelationalOverflowPublicationError::Corrupt(message) => {
                RelationalRowPageDemandReadError::Corrupt(message)
            }
            RelationalOverflowPublicationError::Durability(message) => {
                RelationalRowPageDemandReadError::Durability(message)
            }
            RelationalOverflowPublicationError::MissingExtent(digest) => {
                RelationalRowPageDemandReadError::Corrupt(format!(
                    "overflow root has no extent {digest}"
                ))
            }
            stale @ RelationalOverflowPublicationError::StaleGeneration { .. } => {
                RelationalRowPageDemandReadError::Corrupt(stale.to_string())
            }
        };
        self.poison_if_needed(&mapped);
        mapped
    }

    fn poison_if_needed(&self, error: &RelationalRowPageDemandReadError) {
        if matches!(
            error,
            RelationalRowPageDemandReadError::Corrupt(_)
                | RelationalRowPageDemandReadError::Durability(_)
        ) {
            self.reader.poison();
        }
    }
}

fn validate_table_column_count(
    table: &super::RelationalRowPageTableRoot,
    page: &RelationalRowPageView<'_>,
) -> Result<(), RelationalRowPageDemandReadError> {
    if page.column_count() != table.column_count.get() as usize {
        return Err(RelationalRowPageDemandReadError::Corrupt(format!(
            "table {} binds {} columns but page {} contains {}",
            table.table,
            table.column_count,
            page.page_id().get(),
            page.column_count()
        )));
    }
    Ok(())
}

type OverlayIterator =
    std::iter::Peekable<btree_map::IntoIter<RelationalKey, RelationalRowPageProjectedOverlayValue>>;

fn emit_overlay_before(
    context: &mut DemandReadContext<'_>,
    overlay: &mut OverlayIterator,
    base_key: &RelationalKey,
    overflow_root: Option<&RelationalOverflowRootReader>,
    resolve: &mut ProjectedRowResolver<'_>,
    visit: &mut impl FnMut(RelationalProjectedRow) -> bool,
) -> Result<bool, RelationalRowPageDemandReadError> {
    while overlay
        .peek()
        .is_some_and(|(overlay_key, _)| overlay_key < base_key)
    {
        let (key, value) = overlay.next().expect("peeked overlay entry exists");
        if !emit_overlay_row(context, key, value, overflow_root, resolve, visit)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn emit_remaining_overlay(
    context: &mut DemandReadContext<'_>,
    overlay: &mut OverlayIterator,
    overflow_root: Option<&RelationalOverflowRootReader>,
    resolve: &mut ProjectedRowResolver<'_>,
    visit: &mut impl FnMut(RelationalProjectedRow) -> bool,
) -> Result<bool, RelationalRowPageDemandReadError> {
    for (key, value) in overlay.by_ref() {
        if !emit_overlay_row(context, key, value, overflow_root, resolve, visit)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn emit_overlay_row(
    context: &mut DemandReadContext<'_>,
    primary_key: RelationalKey,
    value: RelationalRowPageProjectedOverlayValue,
    overflow_root: Option<&RelationalOverflowRootReader>,
    resolve: &mut ProjectedRowResolver<'_>,
    visit: &mut impl FnMut(RelationalProjectedRow) -> bool,
) -> Result<bool, RelationalRowPageDemandReadError> {
    let RelationalRowPageProjectedOverlayValue::Present(fields) = value else {
        return Ok(true);
    };
    context.checkpoint()?;
    context.admit_row()?;
    let mut row = RelationalProjectedRow {
        primary_key,
        fields: fields.into_vec(),
    };
    if let Some(overflow_root) = overflow_root {
        context.hydrate_projected_row_from(&mut row, overflow_root)?;
    }
    resolve(&mut row, context.hydration, context.task)?;
    context.report.rows_decoded += 1;
    context.report.rows_emitted += 1;
    Ok(visit(row))
}

struct EncodedRange {
    lower: Option<(Vec<u8>, bool)>,
    upper: Option<(Vec<u8>, bool)>,
    empty: bool,
}

impl EncodedRange {
    fn new(
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
    ) -> Result<Self, RelationalRowPageDemandReadError> {
        let lower = encode_bound(lower)?;
        let upper = encode_bound(upper)?;
        let empty = match (&lower, &upper) {
            (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
                lower > upper || (lower == upper && !(*lower_inclusive && *upper_inclusive))
            }
            _ => false,
        };
        Ok(Self {
            lower,
            upper,
            empty,
        })
    }

    fn page_is_before_lower(&self, descriptor: &RelationalRowPageRootDescriptor) -> bool {
        self.lower.as_ref().is_some_and(|(lower, inclusive)| {
            descriptor.upper_bound.as_slice() < lower.as_slice()
                || (!inclusive && descriptor.upper_bound.as_slice() == lower.as_slice())
        })
    }

    fn page_is_past_upper(&self, descriptor: &RelationalRowPageRootDescriptor) -> bool {
        self.upper.as_ref().is_some_and(|(upper, inclusive)| {
            descriptor.lower_bound.as_slice() > upper.as_slice()
                || (!inclusive && descriptor.lower_bound.as_slice() == upper.as_slice())
        })
    }

    fn key_is_past_upper(&self, encoded_key: &[u8]) -> bool {
        self.upper.as_ref().is_some_and(|(upper, inclusive)| {
            encoded_key > upper.as_slice() || (!inclusive && encoded_key == upper.as_slice())
        })
    }
}

fn encode_bound(
    bound: Bound<&RelationalKey>,
) -> Result<Option<(Vec<u8>, bool)>, RelationalRowPageDemandReadError> {
    match bound {
        Bound::Included(key) => encode_ordered_relational_key(key)
            .map(|encoded| Some((encoded, true)))
            .map_err(|error| RelationalRowPageDemandReadError::Admission(error.to_string())),
        Bound::Excluded(key) => encode_ordered_relational_key(key)
            .map(|encoded| Some((encoded, false)))
            .map_err(|error| RelationalRowPageDemandReadError::Admission(error.to_string())),
        Bound::Unbounded => Ok(None),
    }
}

fn descriptor_search_read_bound(page_count: u64) -> u32 {
    if page_count == 0 {
        0
    } else {
        (u64::BITS - page_count.leading_zeros()).saturating_add(1)
    }
}

fn map_row_publication_error(
    error: RelationalRowPagePublicationError,
) -> RelationalRowPageDemandReadError {
    match error {
        RelationalRowPagePublicationError::Admission(message) => {
            RelationalRowPageDemandReadError::Admission(message)
        }
        RelationalRowPagePublicationError::Corrupt(message) => {
            RelationalRowPageDemandReadError::Corrupt(message)
        }
        RelationalRowPagePublicationError::Durability(message) => {
            RelationalRowPageDemandReadError::Durability(message)
        }
        RelationalRowPagePublicationError::MissingTable(table) => {
            RelationalRowPageDemandReadError::MissingTable(table)
        }
        stale @ RelationalRowPagePublicationError::StaleGeneration { .. } => {
            RelationalRowPageDemandReadError::Corrupt(stale.to_string())
        }
    }
}

#[cfg(test)]
#[path = "demand/tests.rs"]
mod tests;
