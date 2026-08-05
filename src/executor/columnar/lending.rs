use super::{schema_value_mismatch, NumericFragment};
use crate::error::Result;
use skein_core::Value;
use skein_executor::columnar::{float64_value_matches, int64_value_matches};
use skein_storage::NodeRecord;

pub(super) fn admitted_numeric_batch_rows(
    configured_rows: usize,
    memory_budget_bytes: usize,
    needs_node_ids: bool,
) -> Option<usize> {
    let value_bytes = std::mem::size_of::<f64>();
    let node_id_bytes = usize::from(needs_node_ids) * std::mem::size_of::<u64>();
    let bytes_per_row = value_bytes.saturating_add(node_id_bytes);
    let mut lower = 0usize;
    let mut upper = configured_rows;
    while lower < upper {
        let rows = lower + (upper - lower).div_ceil(2);
        let required_bytes = rows.saturating_mul(bytes_per_row);
        if required_bytes <= memory_budget_bytes {
            lower = rows;
        } else {
            upper = rows.saturating_sub(1);
        }
    }
    (lower > 0).then_some(lower)
}

/// A cursor whose batch view is valid only until the next mutable cursor step.
///
/// Keeping this internal preserves the object-safe row executor facade while
/// allowing statically dispatched producers to reuse their backing buffers.
pub(super) trait LendingBatchCursor {
    type Batch<'batch>
    where
        Self: 'batch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch<'_>>>;
}

#[derive(Debug, Clone, Copy)]
pub(super) enum NumericBatchValues<'batch> {
    Int(&'batch [i64]),
    Float(&'batch [f64]),
}

impl NumericBatchValues<'_> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Int(values) => values.len(),
            Self::Float(values) => values.len(),
        }
    }

    pub(super) fn value(self, row: usize) -> Value {
        match self {
            Self::Int(values) => Value::Int(values[row]),
            Self::Float(values) => Value::Float(values[row]),
        }
    }
}

enum NumericValueBuffer {
    Int(Vec<i64>),
    Float(Vec<f64>),
}

impl NumericValueBuffer {
    fn with_capacity(property_type: crate::schema::PropertyType, rows: usize) -> Self {
        match property_type {
            crate::schema::PropertyType::Int => Self::Int(Vec::with_capacity(rows)),
            crate::schema::PropertyType::Float => Self::Float(Vec::with_capacity(rows)),
            _ => unreachable!("numeric fragment eligibility checks the property type"),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Int(values) => values.clear(),
            Self::Float(values) => values.clear(),
        }
    }

    fn view(&self) -> NumericBatchValues<'_> {
        match self {
            Self::Int(values) => NumericBatchValues::Int(values),
            Self::Float(values) => NumericBatchValues::Float(values),
        }
    }
}

pub(super) struct NumericNodeBatch<'batch> {
    pub(super) input_rows: usize,
    pub(super) node_ids: Option<&'batch [u64]>,
    pub(super) values: NumericBatchValues<'batch>,
}

pub(super) struct NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    nodes: I,
    fragment: NumericFragment<'plan>,
    batch_rows: usize,
    node_ids: Option<Vec<u64>>,
    values: NumericValueBuffer,
}

impl<'store, 'plan, I> NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    pub(super) fn new(
        nodes: I,
        fragment: NumericFragment<'plan>,
        batch_rows: usize,
        needs_node_ids: bool,
    ) -> Self {
        Self {
            nodes,
            fragment,
            batch_rows,
            node_ids: needs_node_ids.then(|| Vec::with_capacity(batch_rows)),
            values: NumericValueBuffer::with_capacity(fragment.property_type, batch_rows),
        }
    }

    fn push_node_if_selected(&mut self, node: &NodeRecord) -> Result<()> {
        match (
            &mut self.values,
            node.properties.get(self.fragment.property),
        ) {
            (NumericValueBuffer::Int(values), Some(Value::Int(value))) => {
                if int64_value_matches(*value, self.fragment.op, self.fragment.expected) {
                    if let Some(node_ids) = &mut self.node_ids {
                        node_ids.push(node.id.0);
                    }
                    values.push(*value);
                }
            }
            (NumericValueBuffer::Float(values), Some(Value::Float(value))) => {
                if float64_value_matches(*value, self.fragment.op, self.fragment.expected) {
                    if let Some(node_ids) = &mut self.node_ids {
                        node_ids.push(node.id.0);
                    }
                    values.push(*value);
                }
            }
            (_, Some(Value::Null) | None) => {}
            (_, Some(value)) => return Err(schema_value_mismatch(self.fragment, value)),
        }
        Ok(())
    }
}

impl<'store, 'plan, I> LendingBatchCursor for NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    type Batch<'batch>
        = NumericNodeBatch<'batch>
    where
        Self: 'batch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch<'_>>> {
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.clear();
        }
        self.values.clear();
        let mut input_rows = 0usize;
        for _ in 0..self.batch_rows {
            let Some(node) = self.nodes.next() else {
                break;
            };
            input_rows = input_rows.saturating_add(1);
            self.push_node_if_selected(node)?;
        }
        if input_rows == 0 {
            return Ok(None);
        }
        Ok(Some(NumericNodeBatch {
            input_rows,
            node_ids: self.node_ids.as_deref(),
            values: self.values.view(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::NodeId;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn numeric_batch_admission_accounts_for_required_slots() {
        assert_eq!(admitted_numeric_batch_rows(128, 80, false), Some(10));
        assert_eq!(admitted_numeric_batch_rows(128, 80, true), Some(5));
        assert_eq!(admitted_numeric_batch_rows(128, 7, false), None);
    }

    #[test]
    fn lending_numeric_cursor_reuses_value_storage() {
        let nodes = (0..5)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("score".to_string(), Value::Int(id as i64))]),
            })
            .collect::<Vec<_>>();
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: crate::schema::PropertyType::Int,
            op: skein_plan::ComparisonOp::Gte,
            expected: skein_executor::columnar::NumericLiteral::Int(0),
        };
        let mut cursor = NumericNodeBatchCursor::new(nodes.iter(), fragment, 2, true);

        let first_values_ptr = {
            let first = cursor.next_batch().unwrap().unwrap();
            let first_values = match first.values {
                NumericBatchValues::Int(values) => values,
                NumericBatchValues::Float(_) => panic!("expected integer batch"),
            };
            assert_eq!(first.node_ids, Some(&[0, 1][..]));
            assert_eq!(first_values, &[0, 1]);
            first_values.as_ptr()
        };
        {
            let second = cursor.next_batch().unwrap().unwrap();
            let second_values = match second.values {
                NumericBatchValues::Int(values) => values,
                NumericBatchValues::Float(_) => panic!("expected integer batch"),
            };
            assert_eq!(second.node_ids, Some(&[2, 3][..]));
            assert_eq!(second_values, &[2, 3]);
            assert_eq!(second_values.as_ptr(), first_values_ptr);
        }
        {
            let final_batch = cursor.next_batch().unwrap().unwrap();
            assert_eq!(final_batch.node_ids, Some(&[4][..]));
        }
        assert!(cursor.next_batch().unwrap().is_none());
    }

    #[test]
    fn lending_numeric_cursor_tracks_nulls_without_retaining_records() {
        let nodes = [
            NodeRecord {
                id: NodeId(1),
                labels: BTreeSet::new(),
                properties: BTreeMap::new(),
            },
            NodeRecord {
                id: NodeId(2),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("score".to_string(), Value::Float(2.5))]),
            },
        ];
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: crate::schema::PropertyType::Float,
            op: skein_plan::ComparisonOp::Gt,
            expected: skein_executor::columnar::NumericLiteral::Float(1.0),
        };
        let mut cursor = NumericNodeBatchCursor::new(nodes.iter(), fragment, 8, false);

        let batch = cursor.next_batch().unwrap().unwrap();
        assert_eq!(batch.node_ids, None);
        assert_eq!(batch.input_rows, 2);
        assert_eq!(batch.values.len(), 1);
        assert_eq!(batch.values.value(0), Value::Float(2.5));
    }
}
