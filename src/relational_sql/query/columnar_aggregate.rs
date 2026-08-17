use super::{expression_name, resolve_column, BoundRow};
use crate::error::{Result, SkeinError};
use crate::sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument,
};
use crate::value::Value;
use skein_executor::{
    BindingSchema, ColumnVector, ColumnarBatch, LogicalType, QueryMemoryClass, QueryMemoryLease,
    QueryMemoryLedger, SlotDescriptor, SlotId, ValidityBuilder,
};
use skein_storage::{RelationalScalarType, RelationalTableSchema, RelationalValue};
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(super) struct ColumnarAggregateExecutor {
    projections: Vec<ColumnarAggregateProjection>,
    schema: Arc<BindingSchema>,
    buffers: Vec<ColumnBuffer>,
    batch_rows: usize,
    row_count: usize,
    _batch_lease: QueryMemoryLease,
}

struct ColumnarAggregateProjection {
    output_name: String,
    kind: ColumnarAggregateKind,
    accumulator: ColumnarAggregateAccumulator,
}

enum ColumnarAggregateKind {
    CountAll,
    CountColumn(SqlColumnRef),
    SumInt64(SqlColumnRef),
}

enum ColumnarAggregateAccumulator {
    Count(usize),
    SumInt64(Option<i64>),
}

enum ColumnBuffer {
    Bool {
        values: Vec<u8>,
        validity: ValidityBuilder,
    },
    Int64 {
        values: Vec<i64>,
        validity: ValidityBuilder,
    },
}

impl ColumnarAggregateExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_new(
        select: &SelectStatement,
        base_schema: &RelationalTableSchema,
        base_table: &str,
        base_qualifier: &str,
        has_no_joins: bool,
        requested_batch_rows: usize,
        batch_payload_bytes: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
    ) -> Result<Option<Self>> {
        if !has_no_joins || select.projection.is_empty() {
            return Ok(None);
        }
        let Some(projections) = select
            .projection
            .iter()
            .map(|projection| {
                prepare_projection(projection, base_schema, base_table, base_qualifier)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let schema = Arc::new(BindingSchema::try_new(
            projections
                .iter()
                .enumerate()
                .map(|(index, projection)| SlotDescriptor {
                    id: SlotId(index as u32),
                    name: format!("aggregate_{index}"),
                    logical_type: match projection.kind {
                        ColumnarAggregateKind::CountAll | ColumnarAggregateKind::CountColumn(_) => {
                            LogicalType::Bool
                        }
                        ColumnarAggregateKind::SumInt64(_) => LogicalType::Int64,
                    },
                })
                .collect(),
        )?);
        let batch_rows = admitted_batch_rows(
            requested_batch_rows.max(1),
            &projections,
            &schema,
            batch_payload_bytes.get(),
        )
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "relational columnar aggregate cannot fit one row within batch_payload_bytes {}",
                batch_payload_bytes
            ))
        })?;
        let reserved_bytes = estimated_batch_bytes(batch_rows, &projections, &schema);
        let account = memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "RelationalColumnarAggregate input batch",
            batch_payload_bytes,
        );
        let batch_lease = account.reserve(reserved_bytes)?;
        let buffers = create_buffers(&projections, batch_rows);
        Ok(Some(Self {
            projections,
            schema,
            buffers,
            batch_rows,
            row_count: 0,
            _batch_lease: batch_lease,
        }))
    }

    pub(super) fn blocking_state_bytes(&self) -> usize {
        self.projections.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.projections
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ColumnarAggregateProjection>()),
            ),
            |total, projection| {
                total
                    .saturating_add(projection.output_name.capacity())
                    .saturating_add(match &projection.kind {
                        ColumnarAggregateKind::CountAll => 0,
                        ColumnarAggregateKind::CountColumn(column)
                        | ColumnarAggregateKind::SumInt64(column) => {
                            column.name.capacity()
                                + column
                                    .qualifier
                                    .as_ref()
                                    .map_or(0, |qualifier| qualifier.capacity())
                        }
                    })
            },
        )
    }

    pub(super) fn push(&mut self, row: &BoundRow<'_>) -> Result<()> {
        for (projection, buffer) in self.projections.iter().zip(&mut self.buffers) {
            match (&projection.kind, buffer) {
                (ColumnarAggregateKind::CountAll, ColumnBuffer::Bool { values, validity }) => {
                    values.push(1);
                    validity.push(true);
                }
                (
                    ColumnarAggregateKind::CountColumn(column),
                    ColumnBuffer::Bool { values, validity },
                ) => {
                    values.push(1);
                    validity.push(!matches!(
                        resolve_column(row, column)?,
                        RelationalValue::Null
                    ));
                }
                (
                    ColumnarAggregateKind::SumInt64(column),
                    ColumnBuffer::Int64 { values, validity },
                ) => match resolve_column(row, column)? {
                    RelationalValue::Null => {
                        values.push(0);
                        validity.push(false);
                    }
                    RelationalValue::BigInt(value) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    _ => {
                        return Err(SkeinError::Semantic(
                            "SUM requires BIGINT or DOUBLE PRECISION input".to_string(),
                        ));
                    }
                },
                _ => unreachable!("columnar aggregate schema and buffers are aligned"),
            }
        }
        self.row_count = self.row_count.saturating_add(1);
        if self.row_count == self.batch_rows {
            self.consume_batch()?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Vec<(String, Value)>> {
        self.consume_batch()?;
        self.projections
            .into_iter()
            .map(|projection| {
                let value = match projection.accumulator {
                    ColumnarAggregateAccumulator::Count(count) => {
                        Value::Int(i64::try_from(count).unwrap_or(i64::MAX))
                    }
                    ColumnarAggregateAccumulator::SumInt64(sum) => {
                        sum.map_or(Value::Null, Value::Int)
                    }
                };
                Ok((projection.output_name, value))
            })
            .collect()
    }

    fn consume_batch(&mut self) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }
        let buffers = std::mem::take(&mut self.buffers);
        let columns = buffers
            .into_iter()
            .map(|buffer| match buffer {
                ColumnBuffer::Bool { values, validity } => {
                    ColumnVector::boolean_bytes(values, validity.finish())
                }
                ColumnBuffer::Int64 { values, validity } => {
                    ColumnVector::int64(values, validity.finish())
                }
            })
            .map(|column| column.map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let batch = ColumnarBatch::try_new(Arc::clone(&self.schema), columns)?;
        for (index, projection) in self.projections.iter_mut().enumerate() {
            let slot = SlotId(index as u32);
            match (&projection.kind, &mut projection.accumulator) {
                (ColumnarAggregateKind::CountAll, ColumnarAggregateAccumulator::Count(count)) => {
                    *count = count.saturating_add(batch.count_selected());
                }
                (
                    ColumnarAggregateKind::CountColumn(_),
                    ColumnarAggregateAccumulator::Count(count),
                ) => {
                    *count = count.saturating_add(batch.count_valid(slot)?);
                }
                (
                    ColumnarAggregateKind::SumInt64(_),
                    ColumnarAggregateAccumulator::SumInt64(sum),
                ) => {
                    let partial = batch.sum_int64(slot).map_err(|error| {
                        if error.to_string().contains("SUM overflow") {
                            SkeinError::Execution("BIGINT SUM overflow".to_string())
                        } else {
                            error
                        }
                    })?;
                    if let Some(partial) = partial {
                        *sum = Some(sum.unwrap_or(0).checked_add(partial).ok_or_else(|| {
                            SkeinError::Execution("BIGINT SUM overflow".to_string())
                        })?);
                    }
                }
                _ => unreachable!("columnar aggregate kind and accumulator are aligned"),
            }
        }
        drop(batch);
        self.buffers = create_buffers(&self.projections, self.batch_rows);
        self.row_count = 0;
        Ok(())
    }
}

fn prepare_projection(
    projection: &SelectProjection,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
) -> Option<ColumnarAggregateProjection> {
    let SelectProjection::Expression { expression, alias } = projection else {
        return None;
    };
    let SqlExpression::Function {
        name,
        arguments,
        distinct: false,
    } = expression
    else {
        return None;
    };
    let output_name = alias.clone().unwrap_or_else(|| expression_name(expression));
    let (kind, accumulator) = match (name.as_str(), arguments.as_slice()) {
        ("count", [SqlFunctionArgument::Wildcard]) => (
            ColumnarAggregateKind::CountAll,
            ColumnarAggregateAccumulator::Count(0),
        ),
        ("count", [SqlFunctionArgument::Expression(SqlExpression::Column(column))])
            if base_column_position(column, base_schema, base_table, base_qualifier).is_some() =>
        {
            (
                ColumnarAggregateKind::CountColumn(column.clone()),
                ColumnarAggregateAccumulator::Count(0),
            )
        }
        ("sum", [SqlFunctionArgument::Expression(SqlExpression::Column(column))])
            if base_column_position(column, base_schema, base_table, base_qualifier)
                .is_some_and(|position| {
                    base_schema.columns[position].scalar_type == RelationalScalarType::BigInt
                }) =>
        {
            (
                ColumnarAggregateKind::SumInt64(column.clone()),
                ColumnarAggregateAccumulator::SumInt64(None),
            )
        }
        _ => return None,
    };
    Some(ColumnarAggregateProjection {
        output_name,
        kind,
        accumulator,
    })
}

fn base_column_position(
    column: &SqlColumnRef,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
) -> Option<usize> {
    if column
        .qualifier
        .as_deref()
        .is_some_and(|qualifier| qualifier != base_table && qualifier != base_qualifier)
    {
        return None;
    }
    base_schema.column_position(&column.name)
}

fn admitted_batch_rows(
    requested: usize,
    projections: &[ColumnarAggregateProjection],
    schema: &BindingSchema,
    byte_limit: usize,
) -> Option<usize> {
    let mut low = 1usize;
    let mut high = requested;
    let mut admitted = None;
    while low <= high {
        let candidate = low + (high - low) / 2;
        if estimated_batch_bytes(candidate, projections, schema) <= byte_limit {
            admitted = Some(candidate);
            low = candidate.saturating_add(1);
        } else {
            high = candidate.saturating_sub(1);
        }
    }
    admitted
}

fn estimated_batch_bytes(
    rows: usize,
    projections: &[ColumnarAggregateProjection],
    schema: &BindingSchema,
) -> usize {
    let schema_bytes = std::mem::size_of::<BindingSchema>()
        .saturating_add(
            schema
                .slots()
                .len()
                .saturating_mul(std::mem::size_of::<SlotDescriptor>()),
        )
        .saturating_add(
            schema
                .slots()
                .iter()
                .map(|slot| slot.name.len())
                .sum::<usize>(),
        );
    projections.iter().fold(
        schema_bytes
            .saturating_add(std::mem::size_of::<ColumnarBatch>())
            .saturating_add(
                projections
                    .len()
                    .saturating_mul(std::mem::size_of::<Arc<ColumnVector>>()),
            ),
        |total, projection| {
            let values = match projection.kind {
                ColumnarAggregateKind::CountAll | ColumnarAggregateKind::CountColumn(_) => rows,
                ColumnarAggregateKind::SumInt64(_) => {
                    rows.saturating_mul(std::mem::size_of::<i64>())
                }
            };
            let validity = match projection.kind {
                ColumnarAggregateKind::CountAll => 0,
                ColumnarAggregateKind::CountColumn(_) | ColumnarAggregateKind::SumInt64(_) => rows
                    .div_ceil(u64::BITS as usize)
                    .saturating_mul(std::mem::size_of::<u64>()),
            };
            total
                .saturating_add(std::mem::size_of::<ColumnVector>())
                .saturating_add(values)
                .saturating_add(validity)
        },
    )
}

fn create_buffers(projections: &[ColumnarAggregateProjection], rows: usize) -> Vec<ColumnBuffer> {
    projections
        .iter()
        .map(|projection| match projection.kind {
            ColumnarAggregateKind::CountAll | ColumnarAggregateKind::CountColumn(_) => {
                ColumnBuffer::Bool {
                    values: Vec::with_capacity(rows),
                    validity: ValidityBuilder::with_capacity(rows),
                }
            }
            ColumnarAggregateKind::SumInt64(_) => ColumnBuffer::Int64 {
                values: Vec::with_capacity(rows),
                validity: ValidityBuilder::with_capacity(rows),
            },
        })
        .collect()
}
