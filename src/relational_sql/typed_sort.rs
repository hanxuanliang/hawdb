use crate::error::{Result, SkeinError};
use crate::sql::{SqlNullOrder, SqlOrderDirection};
use skein_core::RuntimeTaskContext;
use skein_executor::blocking::spill_backed_report;
use skein_executor::kernel::{
    ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker,
};
use skein_executor::pipeline::runtime_checkpoint;
use skein_executor::spill::{SpillReader, SpillRun, SpillWriter};
use skein_executor::{BlockingOperatorMemoryReport, ExecutionMemoryConfig};
use skein_integrity::{Sha256Digest, SHA256_BYTES};
use skein_storage::{RelationalKey, RelationalOverflowRef, RelationalScalarType, RelationalValue};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{Cursor, Read};

const RELATIONAL_SORT_RECORD_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct RelationalSortKey {
    value: RelationalValue,
    direction: SqlOrderDirection,
    nulls: SqlNullOrder,
}

impl RelationalSortKey {
    pub(super) fn new(
        value: RelationalValue,
        direction: SqlOrderDirection,
        nulls: SqlNullOrder,
    ) -> Result<Self> {
        if matches!(value, RelationalValue::Overflow(_)) {
            return Err(SkeinError::Execution(
                "ORDER BY requires overflow hydration before qualification".to_string(),
            ));
        }
        Ok(Self {
            value,
            direction,
            nulls,
        })
    }

    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.value.estimated_payload_bytes())
    }

    fn cmp_value(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.direction, other.direction);
        debug_assert_eq!(self.nulls, other.nulls);
        let left_null = matches!(self.value, RelationalValue::Null);
        let right_null = matches!(other.value, RelationalValue::Null);
        if left_null || right_null {
            let nulls_first = match self.nulls {
                SqlNullOrder::First => true,
                SqlNullOrder::Last => false,
                SqlNullOrder::DialectDefault => self.direction == SqlOrderDirection::Desc,
            };
            return match (left_null, right_null, nulls_first) {
                (true, true, _) => Ordering::Equal,
                (true, false, true) | (false, true, false) => Ordering::Less,
                (true, false, false) | (false, true, true) => Ordering::Greater,
                (false, false, _) => unreachable!("null branch requires at least one null"),
            };
        }
        let ordering = self.value.cmp(&other.value);
        match self.direction {
            SqlOrderDirection::Asc => ordering,
            SqlOrderDirection::Desc => ordering.reverse(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalRowLocator {
    primary_keys: Box<[Option<RelationalKey>]>,
}

impl RelationalRowLocator {
    pub(super) fn new(primary_keys: Vec<Option<RelationalKey>>) -> Self {
        Self {
            primary_keys: primary_keys.into_boxed_slice(),
        }
    }

    pub(super) fn primary_keys(&self) -> &[Option<RelationalKey>] {
        &self.primary_keys
    }

    fn memory_bytes(&self) -> usize {
        self.primary_keys.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.primary_keys.len() * std::mem::size_of::<Option<RelationalKey>>(),
            ),
            |total, key| {
                key.as_ref().map_or(total, |key| {
                    total
                        .saturating_add(key.0.len() * std::mem::size_of::<RelationalValue>())
                        .saturating_add(key.0.iter().fold(0usize, |bytes, value| {
                            bytes.saturating_add(value.estimated_payload_bytes())
                        }))
                })
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RelationalSortRow {
    sort_keys: Vec<RelationalSortKey>,
    locator: RelationalRowLocator,
    ordinal: u64,
}

impl RelationalSortRow {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.sort_keys.iter().fold(0usize, |bytes, key| {
                bytes.saturating_add(key.memory_bytes())
            }))
            .saturating_add(self.locator.memory_bytes())
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        for (left, right) in self.sort_keys.iter().zip(&other.sort_keys) {
            let ordering = left.cmp_value(right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        self.ordinal.cmp(&other.ordinal)
    }
}

impl Eq for RelationalSortRow {}

impl Ord for RelationalSortRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_key(other)
    }
}

impl PartialOrd for RelationalSortRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RelationalSortMergeEntry {
    row: RelationalSortRow,
    run_index: usize,
}

impl PartialEq for RelationalSortMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for RelationalSortMergeEntry {}

impl Ord for RelationalSortMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for RelationalSortMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct RelationalExternalOrder<'a> {
    operator: &'static str,
    file_operator: &'static str,
    offset: usize,
    limit: usize,
    retained: usize,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    runs: Vec<SpillRun>,
    heap: BinaryHeap<RelationalSortRow>,
    input_rows: u64,
    spilled_rows: usize,
}

impl<'a> RelationalExternalOrder<'a> {
    pub(super) fn new(
        operator: &'static str,
        file_operator: &'static str,
        offset: usize,
        limit: usize,
        memory: &'a ExecutionMemoryConfig,
        task_context: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        Self {
            operator,
            file_operator,
            offset,
            limit,
            retained: offset.saturating_add(limit),
            memory,
            task_context,
            tracker: OperatorMemoryTracker::new(memory.blocking_operator_bytes),
            spill_budget: SpillBudgetTracker::new(operator, memory),
            runs: Vec::new(),
            heap: BinaryHeap::new(),
            input_rows: 0,
            spilled_rows: 0,
        }
    }

    pub(super) fn push(
        &mut self,
        sort_keys: Vec<RelationalSortKey>,
        locator: RelationalRowLocator,
    ) -> Result<()> {
        if self.retained == 0 {
            return Ok(());
        }
        let candidate = RelationalSortRow {
            sort_keys,
            locator,
            ordinal: self.input_rows,
        };
        self.input_rows = self.input_rows.saturating_add(1);
        let bytes = candidate.memory_bytes();
        ensure_operator_item_fits(self.operator, bytes, &self.tracker)?;
        if self.heap.len() < self.retained {
            if self.tracker.would_exceed(bytes) {
                self.spill_heap()?;
            }
            self.tracker.charge(bytes);
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            let worst_bytes = self
                .heap
                .peek()
                .map(RelationalSortRow::memory_bytes)
                .unwrap_or(0);
            if self
                .tracker
                .used_bytes
                .saturating_sub(worst_bytes)
                .saturating_add(bytes)
                > self.tracker.budget_bytes
            {
                self.spill_heap()?;
            } else {
                self.heap.pop();
                self.tracker.release(worst_bytes);
            }
            self.tracker.charge(bytes);
            self.heap.push(candidate);
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        mut visit: impl FnMut(RelationalRowLocator) -> Result<bool>,
    ) -> Result<BlockingOperatorMemoryReport> {
        runtime_checkpoint(self.task_context)?;
        if self.runs.is_empty() {
            let mut selected = self.heap.into_vec();
            selected.sort_by(RelationalSortRow::cmp_key);
            for row in selected.into_iter().skip(self.offset).take(self.limit) {
                runtime_checkpoint(self.task_context)?;
                if !visit(row.locator)? {
                    break;
                }
            }
        } else {
            if !self.heap.is_empty() {
                self.spill_heap()?;
            }
            self.compact_runs()?;
            self.merge_runs(&mut visit)?;
        }
        Ok(spill_backed_report(
            self.operator,
            &self.tracker,
            self.tracker.peak_bytes,
            self.input_rows as usize,
            &self.spill_budget,
            self.spilled_rows,
        ))
    }

    fn spill_heap(&mut self) -> Result<()> {
        runtime_checkpoint(self.task_context)?;
        let mut rows = std::mem::take(&mut self.heap).into_vec();
        rows.sort_by(RelationalSortRow::cmp_key);
        self.spilled_rows = self.spilled_rows.saturating_add(rows.len());
        let (run, mut writer) = self.spill_budget.create_run(self.file_operator)?;
        for row in rows {
            runtime_checkpoint(self.task_context)?;
            write_sort_row(&mut writer, &row, &mut self.spill_budget)?;
        }
        writer.finish()?;
        self.runs.push(run);
        self.tracker.reset();
        Ok(())
    }

    fn compact_runs(&mut self) -> Result<()> {
        while self.runs.len() > 2 {
            runtime_checkpoint(self.task_context)?;
            let mut compacted = Vec::with_capacity(self.runs.len().div_ceil(2));
            let mut pending = std::mem::take(&mut self.runs).into_iter();
            while let Some(left) = pending.next() {
                let Some(right) = pending.next() else {
                    compacted.push(left);
                    break;
                };
                compacted.push(merge_run_pair(
                    &left,
                    &right,
                    self.retained,
                    self.file_operator,
                    self.memory,
                    &mut self.spill_budget,
                    self.task_context,
                )?);
            }
            self.runs = compacted;
        }
        Ok(())
    }

    fn merge_runs(
        &self,
        visit: &mut impl FnMut(RelationalRowLocator) -> Result<bool>,
    ) -> Result<()> {
        let mut readers = self
            .runs
            .iter()
            .map(SpillRun::reader)
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        let run_count = readers.len();
        for (run_index, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = read_sort_row(reader, self.memory.blocking_operator_bytes.get())? {
                ensure_merge_row_fits(&row, self.memory, run_count)?;
                heap.push(RelationalSortMergeEntry { row, run_index });
            }
        }
        let mut ordinal = 0usize;
        while let Some(entry) = heap.pop() {
            runtime_checkpoint(self.task_context)?;
            let run_index = entry.run_index;
            if ordinal >= self.offset && ordinal < self.retained && !visit(entry.row.locator)? {
                break;
            }
            ordinal = ordinal.saturating_add(1);
            if ordinal >= self.retained {
                break;
            }
            if let Some(row) = read_sort_row(
                &mut readers[run_index],
                self.memory.blocking_operator_bytes.get(),
            )? {
                ensure_merge_row_fits(&row, self.memory, readers.len())?;
                heap.push(RelationalSortMergeEntry { row, run_index });
            }
        }
        Ok(())
    }
}

fn merge_run_pair(
    left: &SpillRun,
    right: &SpillRun,
    retained: usize,
    file_operator: &str,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<SpillRun> {
    let mut readers = [left.reader()?, right.reader()?];
    let mut heap = BinaryHeap::new();
    let run_count = readers.len();
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_sort_row(reader, memory.blocking_operator_bytes.get())? {
            ensure_merge_row_fits(&row, memory, run_count)?;
            heap.push(RelationalSortMergeEntry { row, run_index });
        }
    }
    let (run, mut writer) = spill_budget.create_run(file_operator)?;
    let mut written = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let run_index = entry.run_index;
        write_sort_row(&mut writer, &entry.row, spill_budget)?;
        written = written.saturating_add(1);
        if written >= retained {
            break;
        }
        if let Some(row) = read_sort_row(
            &mut readers[run_index],
            memory.blocking_operator_bytes.get(),
        )? {
            ensure_merge_row_fits(&row, memory, readers.len())?;
            heap.push(RelationalSortMergeEntry { row, run_index });
        }
    }
    writer.finish()?;
    Ok(run)
}

fn ensure_merge_row_fits(
    row: &RelationalSortRow,
    memory: &ExecutionMemoryConfig,
    run_count: usize,
) -> Result<()> {
    let per_row_budget = memory.blocking_operator_bytes.get() / run_count.max(1);
    let bytes = row.memory_bytes();
    if bytes > per_row_budget {
        return Err(SkeinError::Execution(format!(
            "typed relational sort merge row uses {bytes} bytes, exceeding its {per_row_budget}-byte merge share"
        )));
    }
    Ok(())
}

fn write_sort_row(
    writer: &mut SpillWriter,
    row: &RelationalSortRow,
    spill_budget: &mut SpillBudgetTracker,
) -> Result<()> {
    let mut payload = Vec::with_capacity(row.memory_bytes());
    payload.push(RELATIONAL_SORT_RECORD_VERSION);
    payload.extend_from_slice(&row.ordinal.to_le_bytes());
    write_len(&mut payload, row.sort_keys.len())?;
    for key in &row.sort_keys {
        payload.push(match key.direction {
            SqlOrderDirection::Asc => 0,
            SqlOrderDirection::Desc => 1,
        });
        payload.push(match key.nulls {
            SqlNullOrder::DialectDefault => 0,
            SqlNullOrder::First => 1,
            SqlNullOrder::Last => 2,
        });
        write_relational_value(&mut payload, &key.value)?;
    }
    write_len(&mut payload, row.locator.primary_keys.len())?;
    for primary_key in &row.locator.primary_keys {
        match primary_key {
            None => payload.push(0),
            Some(primary_key) => {
                payload.push(1);
                write_len(&mut payload, primary_key.0.len())?;
                for value in &primary_key.0 {
                    write_relational_value(&mut payload, value)?;
                }
            }
        }
    }
    writer.write_record_payload(&payload, spill_budget)?;
    Ok(())
}

fn read_sort_row(
    reader: &mut SpillReader,
    max_record_bytes: usize,
) -> Result<Option<RelationalSortRow>> {
    let Some(payload) = reader.read_record_payload(max_record_bytes)? else {
        return Ok(None);
    };
    let mut cursor = Cursor::new(payload.as_slice());
    if read_u8(&mut cursor)? != RELATIONAL_SORT_RECORD_VERSION {
        return Err(SkeinError::Execution(
            "typed relational sort spill record has an unsupported version".to_string(),
        ));
    }
    let ordinal = read_u64(&mut cursor)?;
    let sort_key_len = read_len(&mut cursor, payload.len())?;
    let mut sort_keys = Vec::with_capacity(sort_key_len);
    for _ in 0..sort_key_len {
        let direction = match read_u8(&mut cursor)? {
            0 => SqlOrderDirection::Asc,
            1 => SqlOrderDirection::Desc,
            _ => return Err(invalid_spill("invalid sort direction")),
        };
        let nulls = match read_u8(&mut cursor)? {
            0 => SqlNullOrder::DialectDefault,
            1 => SqlNullOrder::First,
            2 => SqlNullOrder::Last,
            _ => return Err(invalid_spill("invalid null ordering")),
        };
        sort_keys.push(RelationalSortKey {
            value: read_relational_value(&mut cursor, payload.len())?,
            direction,
            nulls,
        });
    }
    let locator_len = read_len(&mut cursor, payload.len())?;
    let mut primary_keys = Vec::with_capacity(locator_len);
    for _ in 0..locator_len {
        match read_u8(&mut cursor)? {
            0 => primary_keys.push(None),
            1 => {
                let value_len = read_len(&mut cursor, payload.len())?;
                let mut values = Vec::with_capacity(value_len);
                for _ in 0..value_len {
                    values.push(read_relational_value(&mut cursor, payload.len())?);
                }
                primary_keys.push(Some(RelationalKey(values)));
            }
            _ => return Err(invalid_spill("invalid locator presence tag")),
        }
    }
    if cursor.position() != payload.len() as u64 {
        return Err(invalid_spill("trailing bytes"));
    }
    Ok(Some(RelationalSortRow {
        sort_keys,
        locator: RelationalRowLocator::new(primary_keys),
        ordinal,
    }))
}

fn write_relational_value(output: &mut Vec<u8>, value: &RelationalValue) -> Result<()> {
    match value {
        RelationalValue::Null => output.push(0),
        RelationalValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        RelationalValue::BigInt(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        RelationalValue::Text(value) => {
            output.push(4);
            write_bytes(output, value.as_bytes())?;
        }
        RelationalValue::Bytea(value) => {
            output.push(5);
            write_bytes(output, value)?;
        }
        RelationalValue::Overflow(reference) => {
            output.push(6);
            output.extend_from_slice(reference.digest.as_bytes());
            output.push(match reference.scalar_type {
                RelationalScalarType::Boolean => 0,
                RelationalScalarType::BigInt => 1,
                RelationalScalarType::DoublePrecision => 2,
                RelationalScalarType::Text => 3,
                RelationalScalarType::Bytea => 4,
            });
            output.extend_from_slice(&reference.compressed_bytes.to_le_bytes());
            output.extend_from_slice(&reference.uncompressed_bytes.to_le_bytes());
        }
    }
    Ok(())
}

fn read_relational_value(
    input: &mut Cursor<&[u8]>,
    record_bytes: usize,
) -> Result<RelationalValue> {
    Ok(match read_u8(input)? {
        0 => RelationalValue::Null,
        1 => match read_u8(input)? {
            0 => RelationalValue::Boolean(false),
            1 => RelationalValue::Boolean(true),
            _ => return Err(invalid_spill("invalid boolean value")),
        },
        2 => RelationalValue::BigInt(i64::from_le_bytes(read_array(input)?)),
        3 => {
            RelationalValue::DoublePrecision(f64::from_bits(u64::from_le_bytes(read_array(input)?)))
        }
        4 => RelationalValue::Text(
            String::from_utf8(read_bytes(input, record_bytes)?)
                .map_err(|_| invalid_spill("invalid UTF-8 text"))?,
        ),
        5 => RelationalValue::Bytea(read_bytes(input, record_bytes)?),
        6 => {
            let digest = Sha256Digest::from_bytes(read_array::<SHA256_BYTES>(input)?);
            let scalar_type = match read_u8(input)? {
                0 => RelationalScalarType::Boolean,
                1 => RelationalScalarType::BigInt,
                2 => RelationalScalarType::DoublePrecision,
                3 => RelationalScalarType::Text,
                4 => RelationalScalarType::Bytea,
                _ => return Err(invalid_spill("invalid overflow scalar type")),
            };
            RelationalValue::Overflow(RelationalOverflowRef {
                digest,
                scalar_type,
                compressed_bytes: read_u64(input)?,
                uncompressed_bytes: read_u64(input)?,
            })
        }
        _ => return Err(invalid_spill("invalid relational value tag")),
    })
}

fn write_len(output: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        SkeinError::Execution("typed relational sort length exceeds u32".to_string())
    })?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    write_len(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

fn read_len(input: &mut Cursor<&[u8]>, record_bytes: usize) -> Result<usize> {
    let value = read_u32(input)? as usize;
    if value > record_bytes {
        return Err(invalid_spill("declared length exceeds record size"));
    }
    Ok(value)
}

fn read_bytes(input: &mut Cursor<&[u8]>, record_bytes: usize) -> Result<Vec<u8>> {
    let len = read_len(input, record_bytes)?;
    let remaining = record_bytes.saturating_sub(input.position() as usize);
    if len > remaining {
        return Err(invalid_spill("truncated byte string"));
    }
    let mut value = vec![0; len];
    input
        .read_exact(&mut value)
        .map_err(|_| invalid_spill("truncated byte string"))?;
    Ok(value)
}

fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    Ok(read_array::<1>(input)?[0])
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(input)?))
}

fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    Ok(u64::from_le_bytes(read_array(input)?))
}

fn read_array<const N: usize>(input: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut value = [0; N];
    input
        .read_exact(&mut value)
        .map_err(|_| invalid_spill("truncated fixed-width value"))?;
    Ok(value)
}

fn invalid_spill(reason: &str) -> SkeinError {
    SkeinError::Execution(format!(
        "typed relational sort spill record is invalid: {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_memory(name: &str, blocking_bytes: usize) -> ExecutionMemoryConfig {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(64).unwrap(),
            max_total_spill_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            max_total_spill_runs: NonZeroUsize::new(128).unwrap(),
            min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
            spill_directory: std::env::temp_dir().join(format!(
                "skein-relational-typed-sort-{}-{name}-{nonce}",
                std::process::id()
            )),
            ..ExecutionMemoryConfig::default()
        }
    }

    #[test]
    fn compact_spill_codec_round_trips_sort_keys_and_locator() {
        let memory = test_memory("codec", 4096);
        let mut budget = SpillBudgetTracker::new("TopNExec", &memory);
        let (run, mut writer) = budget.create_run("relational-topn").unwrap();
        let row = RelationalSortRow {
            sort_keys: vec![
                RelationalSortKey::new(
                    RelationalValue::Text("key".to_string()),
                    SqlOrderDirection::Asc,
                    SqlNullOrder::Last,
                )
                .unwrap(),
                RelationalSortKey::new(
                    RelationalValue::Null,
                    SqlOrderDirection::Desc,
                    SqlNullOrder::DialectDefault,
                )
                .unwrap(),
            ],
            locator: RelationalRowLocator::new(vec![
                Some(RelationalKey(vec![RelationalValue::BigInt(7)])),
                None,
            ]),
            ordinal: 42,
        };
        write_sort_row(&mut writer, &row, &mut budget).unwrap();
        writer.finish().unwrap();

        let mut reader = run.reader().unwrap();
        assert_eq!(read_sort_row(&mut reader, 4096).unwrap(), Some(row));
        assert_eq!(read_sort_row(&mut reader, 4096).unwrap(), None);
        drop(reader);
        drop(run);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn typed_top_n_spills_and_preserves_offset_order() {
        let memory = test_memory("topn", 500);
        let mut order =
            RelationalExternalOrder::new("TopNExec", "relational-topn", 1, 2, &memory, None);
        for value in [5, 1, 4, 2, 3] {
            order
                .push(
                    vec![RelationalSortKey::new(
                        RelationalValue::BigInt(value),
                        SqlOrderDirection::Asc,
                        SqlNullOrder::DialectDefault,
                    )
                    .unwrap()],
                    RelationalRowLocator::new(vec![Some(RelationalKey(vec![
                        RelationalValue::BigInt(value),
                    ]))]),
                )
                .unwrap();
        }
        let mut output = Vec::new();
        let report = order
            .finish(|locator| {
                output.push(locator.primary_keys()[0].as_ref().unwrap().0[0].clone());
                Ok(true)
            })
            .unwrap();

        assert_eq!(
            output,
            [RelationalValue::BigInt(2), RelationalValue::BigInt(3)]
        );
        assert!(report.spill_run_count > 0);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }
}
