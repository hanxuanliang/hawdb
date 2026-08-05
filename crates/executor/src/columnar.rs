//! Typed columnar batches used by vectorized executor fragments.

use skein_core::{Result, SkeinError, Value};
use skein_plan::ComparisonOp;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    Bool,
    Int64,
    Float64,
    Utf8,
    NodeId,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDescriptor {
    pub id: SlotId,
    pub name: String,
    pub logical_type: LogicalType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingSchema {
    slots: Arc<[SlotDescriptor]>,
}

impl BindingSchema {
    pub fn try_new(slots: Vec<SlotDescriptor>) -> Result<Self> {
        for (index, slot) in slots.iter().enumerate() {
            if slot.id.0 as usize != index {
                return Err(SkeinError::Execution(format!(
                    "columnar schema slot ids must be dense: expected {index}, got {}",
                    slot.id.0
                )));
            }
        }
        Ok(Self {
            slots: slots.into(),
        })
    }

    pub fn slots(&self) -> &[SlotDescriptor] {
        &self.slots
    }

    pub fn slot(&self, id: SlotId) -> Option<&SlotDescriptor> {
        self.slots.get(id.0 as usize)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validity {
    All { len: usize },
    Bitmap { len: usize, words: Arc<[u64]> },
}

impl Validity {
    pub fn all(len: usize) -> Self {
        Self::All { len }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::All { len } | Self::Bitmap { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_valid(&self, row: usize) -> bool {
        if row >= self.len() {
            return false;
        }
        match self {
            Self::All { .. } => true,
            Self::Bitmap { words, .. } => words
                .get(row / u64::BITS as usize)
                .is_some_and(|word| word & (1u64 << (row % u64::BITS as usize)) != 0),
        }
    }

    pub fn valid_count(&self) -> usize {
        match self {
            Self::All { len } => *len,
            Self::Bitmap { len, words } => count_bitmap_rows(words, *len),
        }
    }
}

#[derive(Debug, Default)]
pub struct ValidityBuilder {
    len: usize,
    words: Vec<u64>,
    word_capacity: usize,
}

impl ValidityBuilder {
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            len: 0,
            words: Vec::new(),
            word_capacity: rows.div_ceil(u64::BITS as usize),
        }
    }

    pub fn push(&mut self, valid: bool) {
        let word_index = self.len / u64::BITS as usize;
        let bit_index = self.len % u64::BITS as usize;
        if self.words.is_empty() {
            if !valid {
                self.words = Vec::with_capacity(self.word_capacity);
                self.words.resize(word_index + 1, u64::MAX);
                self.words[word_index] = if bit_index == 0 {
                    0
                } else {
                    (1u64 << bit_index) - 1
                };
            }
        } else {
            if word_index == self.words.len() {
                self.words.push(0);
            }
            if valid {
                self.words[word_index] |= 1u64 << bit_index;
            }
        }
        self.len = self.len.saturating_add(1);
    }

    pub fn finish(self) -> Validity {
        if self.words.is_empty() {
            Validity::All { len: self.len }
        } else {
            Validity::Bitmap {
                len: self.len,
                words: self.words.into(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVector {
    Bool {
        values: Arc<[u8]>,
        validity: Validity,
    },
    Int64 {
        values: Arc<[i64]>,
        validity: Validity,
    },
    Float64 {
        values: Arc<[f64]>,
        validity: Validity,
    },
    Utf8 {
        values: Arc<[String]>,
        validity: Validity,
    },
    NodeId(Arc<[u64]>),
    Dynamic(Arc<[Value]>),
}

impl ColumnVector {
    pub fn int64(values: Vec<i64>, validity: Validity) -> Result<Self> {
        ensure_column_len("Int64", values.len(), validity.len())?;
        Ok(Self::Int64 {
            values: values.into(),
            validity,
        })
    }

    pub fn float64(values: Vec<f64>, validity: Validity) -> Result<Self> {
        ensure_column_len("Float64", values.len(), validity.len())?;
        Ok(Self::Float64 {
            values: values.into(),
            validity,
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::Utf8 { values, .. } => values.len(),
            Self::NodeId(values) => values.len(),
            Self::Dynamic(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn logical_type(&self) -> LogicalType {
        match self {
            Self::Bool { .. } => LogicalType::Bool,
            Self::Int64 { .. } => LogicalType::Int64,
            Self::Float64 { .. } => LogicalType::Float64,
            Self::Utf8 { .. } => LogicalType::Utf8,
            Self::NodeId(_) => LogicalType::NodeId,
            Self::Dynamic(_) => LogicalType::Dynamic,
        }
    }

    pub fn value(&self, row: usize) -> Option<Value> {
        match self {
            Self::Bool { values, validity } if validity.is_valid(row) => {
                values.get(row).map(|value| Value::Bool(*value != 0))
            }
            Self::Int64 { values, validity } if validity.is_valid(row) => {
                values.get(row).copied().map(Value::Int)
            }
            Self::Float64 { values, validity } if validity.is_valid(row) => {
                values.get(row).copied().map(Value::Float)
            }
            Self::Utf8 { values, validity } if validity.is_valid(row) => {
                values.get(row).cloned().map(Value::String)
            }
            Self::Bool { .. } | Self::Int64 { .. } | Self::Float64 { .. } | Self::Utf8 { .. } => {
                None
            }
            Self::NodeId(values) => values.get(row).map(|value| Value::Int(*value as i64)),
            Self::Dynamic(values) => values.get(row).cloned(),
        }
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        let validity_bytes = match self {
            Self::Bool { validity, .. }
            | Self::Int64 { validity, .. }
            | Self::Float64 { validity, .. }
            | Self::Utf8 { validity, .. } => match validity {
                Validity::All { .. } => 0,
                Validity::Bitmap { words, .. } => words.len() * std::mem::size_of::<u64>(),
            },
            Self::NodeId(_) | Self::Dynamic(_) => 0,
        };
        validity_bytes.saturating_add(match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len() * std::mem::size_of::<i64>(),
            Self::Float64 { values, .. } => values.len() * std::mem::size_of::<f64>(),
            Self::Utf8 { values, .. } => values.iter().fold(
                values.len() * std::mem::size_of::<String>(),
                |total, value| total.saturating_add(value.len()),
            ),
            Self::NodeId(values) => values.len() * std::mem::size_of::<u64>(),
            Self::Dynamic(values) => values.len() * std::mem::size_of::<Value>(),
        })
    }
}

fn ensure_column_len(kind: &str, values: usize, validity: usize) -> Result<()> {
    if values != validity {
        return Err(SkeinError::Execution(format!(
            "{kind} column has {values} values but {validity} validity entries"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    All {
        len: usize,
    },
    Bitmap {
        len: usize,
        words: Arc<[u64]>,
        selected: usize,
    },
    Indices {
        len: usize,
        rows: Arc<[u32]>,
    },
}

impl Selection {
    pub fn all(len: usize) -> Self {
        Self::All { len }
    }

    pub fn none(len: usize) -> Self {
        Self::Indices {
            len,
            rows: Arc::from([]),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::All { len } | Self::Bitmap { len, .. } | Self::Indices { len, .. } => *len,
        }
    }

    pub fn selected_count(&self) -> usize {
        match self {
            Self::All { len } => *len,
            Self::Bitmap { selected, .. } => *selected,
            Self::Indices { rows, .. } => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.selected_count() == 0
    }

    pub fn iter(&self) -> SelectionIter<'_> {
        SelectionIter {
            selection: self,
            cursor: 0,
        }
    }
}

struct SelectionBuilder {
    len: usize,
    selected: usize,
    sparse_rows: Vec<u32>,
    bitmap_words: Option<Vec<u64>>,
}

impl SelectionBuilder {
    fn new(len: usize) -> Self {
        Self {
            len,
            selected: 0,
            sparse_rows: Vec::new(),
            bitmap_words: (len > u32::MAX as usize)
                .then(|| vec![0; len.div_ceil(u64::BITS as usize)]),
        }
    }

    fn select(&mut self, row: usize) {
        self.selected = self.selected.saturating_add(1);
        if let Some(words) = &mut self.bitmap_words {
            words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            return;
        }

        self.sparse_rows.push(row as u32);
        if self.selected.saturating_mul(8) > self.len {
            let rows = std::mem::take(&mut self.sparse_rows);
            let mut words = vec![0; self.len.div_ceil(u64::BITS as usize)];
            for row in rows {
                let row = row as usize;
                words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            }
            self.bitmap_words = Some(words);
        }
    }

    fn finish(self) -> Selection {
        if self.selected == self.len {
            return Selection::all(self.len);
        }
        if self.selected == 0 {
            return Selection::none(self.len);
        }
        match self.bitmap_words {
            Some(words) => Selection::Bitmap {
                len: self.len,
                words: words.into(),
                selected: self.selected,
            },
            None => Selection::Indices {
                len: self.len,
                rows: self.sparse_rows.into(),
            },
        }
    }
}

pub struct SelectionIter<'a> {
    selection: &'a Selection,
    cursor: usize,
}

impl Iterator for SelectionIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.selection {
            Selection::All { len } => {
                let row = self.cursor;
                if row == *len {
                    return None;
                }
                self.cursor = self.cursor.saturating_add(1);
                Some(row)
            }
            Selection::Indices { rows, .. } => {
                let row = rows.get(self.cursor).copied()? as usize;
                self.cursor = self.cursor.saturating_add(1);
                Some(row)
            }
            Selection::Bitmap { len, words, .. } => {
                while self.cursor < *len {
                    let row = self.cursor;
                    self.cursor = self.cursor.saturating_add(1);
                    if words[row / u64::BITS as usize] & (1u64 << (row % u64::BITS as usize)) != 0 {
                        return Some(row);
                    }
                }
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnarBatch {
    schema: Arc<BindingSchema>,
    columns: Vec<Arc<ColumnVector>>,
    selection: Selection,
    row_count: usize,
}

impl ColumnarBatch {
    pub fn try_new(schema: Arc<BindingSchema>, columns: Vec<Arc<ColumnVector>>) -> Result<Self> {
        if schema.len() != columns.len() {
            return Err(SkeinError::Execution(format!(
                "columnar batch has {} slots but {} columns",
                schema.len(),
                columns.len()
            )));
        }
        let row_count = columns.first().map_or(0, |column| column.len());
        for (slot, column) in schema.slots().iter().zip(&columns) {
            if column.len() != row_count {
                return Err(SkeinError::Execution(format!(
                    "columnar slot '{}' has {} rows, expected {row_count}",
                    slot.name,
                    column.len()
                )));
            }
            if slot.logical_type != column.logical_type() {
                return Err(SkeinError::Execution(format!(
                    "columnar slot '{}' expects {:?}, got {:?}",
                    slot.name,
                    slot.logical_type,
                    column.logical_type()
                )));
            }
        }
        Ok(Self {
            schema,
            columns,
            selection: Selection::all(row_count),
            row_count,
        })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn selected_count(&self) -> usize {
        self.selection.selected_count()
    }

    pub fn schema(&self) -> &BindingSchema {
        &self.schema
    }

    pub fn column(&self, slot: SlotId) -> Option<&Arc<ColumnVector>> {
        self.columns.get(slot.0 as usize)
    }

    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    pub fn with_selection(mut self, selection: Selection) -> Result<Self> {
        if selection.len() != self.row_count {
            return Err(SkeinError::Execution(format!(
                "columnar selection has {} rows, expected {}",
                selection.len(),
                self.row_count
            )));
        }
        self.selection = selection;
        Ok(self)
    }

    pub fn project(&self, slots: &[SlotId]) -> Result<Self> {
        let mut descriptors = Vec::with_capacity(slots.len());
        let mut columns = Vec::with_capacity(slots.len());
        for (output_index, slot) in slots.iter().copied().enumerate() {
            let descriptor = self.schema.slot(slot).ok_or_else(|| {
                SkeinError::Execution(format!("unknown columnar slot {}", slot.0))
            })?;
            descriptors.push(SlotDescriptor {
                id: SlotId(output_index as u32),
                name: descriptor.name.clone(),
                logical_type: descriptor.logical_type,
            });
            columns.push(Arc::clone(
                self.column(slot).expect("schema and columns align"),
            ));
        }
        Ok(Self {
            schema: Arc::new(BindingSchema::try_new(descriptors)?),
            columns,
            selection: self.selection.clone(),
            row_count: self.row_count,
        })
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        self.columns.iter().fold(0usize, |total, column| {
            total.saturating_add(column.estimated_memory_bytes())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumericLiteral {
    Int(i64),
    Float(f64),
}

impl NumericLiteral {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int(value) => Some(Self::Int(*value)),
            Value::Float(value) => Some(Self::Float(*value)),
            _ => None,
        }
    }
}

pub fn filter_numeric_column(
    column: &ColumnVector,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    if column.len() != input.len() {
        return Err(SkeinError::Execution(format!(
            "numeric filter column has {} rows but input selection has {}",
            column.len(),
            input.len()
        )));
    }
    match column {
        ColumnVector::Int64 { values, validity } => {
            filter_int64_values(values, validity, input, op, expected)
        }
        ColumnVector::Float64 { values, validity } => {
            filter_float64_values(values, validity, input, op, expected)
        }
        other => Err(SkeinError::Execution(format!(
            "numeric filter requires Int64 or Float64, got {:?}",
            other.logical_type()
        ))),
    }
}

pub fn filter_int64_values(
    values: &[i64],
    validity: &Validity,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_numeric_values(values, validity, input, |actual| {
        int64_value_matches(actual, op, expected)
    })
}

pub fn filter_float64_values(
    values: &[f64],
    validity: &Validity,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_numeric_values(values, validity, input, |actual| {
        float64_value_matches(actual, op, expected)
    })
}

fn filter_numeric_values<T: Copy>(
    values: &[T],
    validity: &Validity,
    input: &Selection,
    mut matches: impl FnMut(T) -> bool,
) -> Result<Selection> {
    if values.len() != validity.len() || values.len() != input.len() {
        return Err(SkeinError::Execution(format!(
            "numeric filter has {} values, {} validity entries, and {} selected input rows",
            values.len(),
            validity.len(),
            input.len()
        )));
    }
    let mut output = SelectionBuilder::new(input.len());
    for row in input.iter() {
        if validity.is_valid(row) && matches(values[row]) {
            output.select(row);
        }
    }
    Ok(output.finish())
}

#[inline]
pub fn int64_value_matches(actual: i64, op: ComparisonOp, expected: NumericLiteral) -> bool {
    match expected {
        NumericLiteral::Int(expected) => compare_ordering(actual.cmp(&expected), op),
        NumericLiteral::Float(expected) => {
            compare_ordering((actual as f64).total_cmp(&expected), op)
        }
    }
}

#[inline]
pub fn float64_value_matches(actual: f64, op: ComparisonOp, expected: NumericLiteral) -> bool {
    let expected = match expected {
        NumericLiteral::Int(expected) => expected as f64,
        NumericLiteral::Float(expected) => expected,
    };
    compare_ordering(actual.total_cmp(&expected), op)
}

fn compare_ordering(ordering: std::cmp::Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn count_bitmap_rows(words: &[u64], len: usize) -> usize {
    words
        .iter()
        .enumerate()
        .fold(0usize, |total, (index, word)| {
            let bits = if index + 1 == words.len() && !len.is_multiple_of(u64::BITS as usize) {
                word & ((1u64 << (len % u64::BITS as usize)) - 1)
            } else {
                *word
            };
            total.saturating_add(bits.count_ones() as usize)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_uses_no_bitmap_for_dense_columns() {
        let mut builder = ValidityBuilder::with_capacity(3);
        builder.push(true);
        builder.push(true);
        builder.push(true);

        assert!(builder.words.is_empty());
        assert_eq!(builder.finish(), Validity::All { len: 3 });
    }

    #[test]
    fn validity_materializes_prior_rows_on_first_null() {
        let mut builder = ValidityBuilder::with_capacity(130);
        for _ in 0..65 {
            builder.push(true);
        }
        builder.push(false);
        builder.push(true);

        let validity = builder.finish();
        assert_eq!(validity.valid_count(), 66);
        assert!(validity.is_valid(64));
        assert!(!validity.is_valid(65));
        assert!(validity.is_valid(66));
    }

    #[test]
    fn numeric_filter_preserves_null_and_nan_semantics() {
        let mut validity = ValidityBuilder::with_capacity(4);
        validity.push(true);
        validity.push(false);
        validity.push(true);
        validity.push(true);
        let column =
            ColumnVector::float64(vec![1.0, 0.0, f64::NAN, 3.0], validity.finish()).unwrap();

        let selection = filter_numeric_column(
            &column,
            &Selection::all(4),
            ComparisonOp::Gte,
            NumericLiteral::Float(2.0),
        )
        .unwrap();

        assert_eq!(selection.iter().collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn sparse_filter_uses_index_selection() {
        let column = ColumnVector::int64((0..64).collect(), Validity::all(64)).unwrap();
        let selection = filter_numeric_column(
            &column,
            &Selection::all(64),
            ComparisonOp::Gte,
            NumericLiteral::Int(63),
        )
        .unwrap();

        assert!(matches!(selection, Selection::Indices { .. }));
        assert_eq!(selection.iter().collect::<Vec<_>>(), vec![63]);
    }

    #[test]
    fn dense_filter_uses_bitmap_selection() {
        let column = ColumnVector::int64((0..64).collect(), Validity::all(64)).unwrap();
        let selection = filter_numeric_column(
            &column,
            &Selection::all(64),
            ComparisonOp::Gte,
            NumericLiteral::Int(32),
        )
        .unwrap();

        assert!(matches!(selection, Selection::Bitmap { .. }));
        assert_eq!(selection.selected_count(), 32);
    }

    #[test]
    fn projection_reuses_column_storage() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "score".to_string(),
                logical_type: LogicalType::Int64,
            }])
            .unwrap(),
        );
        let column = Arc::new(ColumnVector::int64(vec![1, 2], Validity::all(2)).unwrap());
        let batch = ColumnarBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&column)]).unwrap();
        let projected = batch.project(&[SlotId(0)]).unwrap();

        assert!(Arc::ptr_eq(projected.column(SlotId(0)).unwrap(), &column));
    }
}
