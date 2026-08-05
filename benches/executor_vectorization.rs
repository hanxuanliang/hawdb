use serde_json::json;
use skein::executor::execute_with_row_limit_profile;
use skein::optimizer::PhysicalPlan;
use skein::planner::{ComparisonOp, Predicate, Projection, ProjectionExpression};
use skein::schema::{Catalog, PropertyType, TableKind};
use skein::store::GraphStore;
use skein::Value;
use skein_executor::{filter_numeric_column, ColumnVector, NumericLiteral, Selection, Validity};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

const MICRO_ROWS: usize = 131_072;
const MICRO_ITERATIONS: usize = 64;
const END_TO_END_ROWS: usize = 16_384;
const END_TO_END_ITERATIONS: usize = 64;
const SAMPLES: usize = 11;

fn main() {
    let micro = micro_benchmark();
    let end_to_end = end_to_end_benchmark();
    assert_eq!(micro.row_checksum, micro.columnar_checksum);
    assert_eq!(end_to_end.row_checksum, end_to_end.columnar_checksum);

    println!(
        "executor_vectorization {}",
        json!({
            "micro": micro.json(),
            "end_to_end": end_to_end.json(),
        })
    );
}

fn micro_benchmark() -> ComparisonReport {
    let rows = (0..MICRO_ROWS)
        .map(|row| {
            BTreeMap::from([
                ("score".to_string(), Value::Int(row as i64)),
                ("payload".to_string(), Value::Int((row % 17) as i64)),
            ])
        })
        .collect::<Vec<_>>();
    let column = ColumnVector::int64(
        (0..MICRO_ROWS).map(|row| row as i64).collect(),
        Validity::all(MICRO_ROWS),
    )
    .expect("micro benchmark column must be valid");
    let threshold = (MICRO_ROWS * 7 / 8) as i64;
    let expected = Value::Int(threshold);

    let (row_ns, row_checksum, columnar_ns, columnar_checksum) =
        paired_median_sample(MICRO_ITERATIONS, |path| match path {
            ExecutionPath::Row => {
                let mut checksum = 0u64;
                for row in black_box(&rows) {
                    let value = row.get("score").expect("score must exist");
                    if skein_executor::predicate::compare_property_values(
                        value,
                        ComparisonOp::Gte,
                        &expected,
                    ) {
                        checksum = checksum.wrapping_add(match value {
                            Value::Int(value) => *value as u64,
                            _ => unreachable!("score is an integer"),
                        });
                    }
                }
                black_box(checksum)
            }
            ExecutionPath::Columnar => {
                let selection = filter_numeric_column(
                    black_box(&column),
                    &Selection::all(MICRO_ROWS),
                    ComparisonOp::Gte,
                    NumericLiteral::Int(threshold),
                )
                .expect("columnar filter must succeed");
                black_box(
                    selection
                        .iter()
                        .fold(0u64, |total, row| total.wrapping_add(row as u64)),
                )
            }
        });

    ComparisonReport {
        rows: MICRO_ROWS,
        iterations: MICRO_ITERATIONS,
        row_ns,
        columnar_ns,
        row_checksum,
        columnar_checksum,
    }
}

fn end_to_end_benchmark() -> ComparisonReport {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", PropertyType::Int, false);
    catalog.get_or_create_property(table, "payload", PropertyType::Int, false);
    let mut store = GraphStore::in_memory();
    for row in 0..END_TO_END_ROWS {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    ("score".to_string(), Value::Int(row as i64)),
                    ("payload".to_string(), Value::Int((row % 17) as i64)),
                ]),
            )
            .expect("benchmark node creation must succeed");
    }
    let threshold = (END_TO_END_ROWS * 7 / 8) as i64;
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: ComparisonOp::Gte,
        value: Value::Int(threshold),
    };
    let columnar_plan = projection_plan(compare.clone());
    let row_plan = projection_plan(Predicate::And(vec![compare]));

    let columnar_probe =
        execute_with_row_limit_profile(&columnar_plan, &mut catalog, &mut store, None)
            .expect("columnar probe must succeed");
    assert!(
        columnar_probe
            .profile
            .pipeline_memory_report
            .columnar_batches
            > 0
    );
    let row_probe = execute_with_row_limit_profile(&row_plan, &mut catalog, &mut store, None)
        .expect("row probe must succeed");
    assert_eq!(row_probe.profile.pipeline_memory_report.columnar_batches, 0);
    assert_eq!(columnar_probe.rows, row_probe.rows);

    let (row_ns, row_checksum, columnar_ns, columnar_checksum) =
        paired_median_sample(END_TO_END_ITERATIONS, |path| {
            let plan = match path {
                ExecutionPath::Row => &row_plan,
                ExecutionPath::Columnar => &columnar_plan,
            };
            let output =
                execute_with_row_limit_profile(black_box(plan), &mut catalog, &mut store, None)
                    .expect("benchmark execution must succeed");
            black_box(output_checksum(&output.rows))
        });

    ComparisonReport {
        rows: END_TO_END_ROWS,
        iterations: END_TO_END_ITERATIONS,
        row_ns,
        columnar_ns,
        row_checksum,
        columnar_checksum,
    }
}

fn projection_plan(predicate: Predicate) -> PhysicalPlan {
    PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        }],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate,
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    }
}

fn output_checksum(rows: &[BTreeMap<String, Value>]) -> u64 {
    rows.iter().fold(0u64, |total, row| {
        total.wrapping_add(match row.get("score") {
            Some(Value::Int(value)) => *value as u64,
            _ => panic!("benchmark output score must be an integer"),
        })
    })
}

fn paired_median_sample(
    iterations: usize,
    mut operation: impl FnMut(ExecutionPath) -> u64,
) -> (u128, u64, u128, u64) {
    black_box(operation(ExecutionPath::Row));
    black_box(operation(ExecutionPath::Columnar));
    let mut row_samples = Vec::with_capacity(SAMPLES);
    let mut columnar_samples = Vec::with_capacity(SAMPLES);
    let mut row_checksum = 0u64;
    let mut columnar_checksum = 0u64;
    for sample in 0..SAMPLES {
        let order = if sample % 2 == 0 {
            [ExecutionPath::Row, ExecutionPath::Columnar]
        } else {
            [ExecutionPath::Columnar, ExecutionPath::Row]
        };
        for path in order {
            let started = Instant::now();
            for _ in 0..iterations {
                let checksum = operation(path);
                match path {
                    ExecutionPath::Row => row_checksum = checksum,
                    ExecutionPath::Columnar => columnar_checksum = checksum,
                }
            }
            match path {
                ExecutionPath::Row => row_samples.push(started.elapsed().as_nanos()),
                ExecutionPath::Columnar => columnar_samples.push(started.elapsed().as_nanos()),
            }
        }
    }
    row_samples.sort_unstable();
    columnar_samples.sort_unstable();
    (
        row_samples[row_samples.len() / 2],
        row_checksum,
        columnar_samples[columnar_samples.len() / 2],
        columnar_checksum,
    )
}

#[derive(Debug, Clone, Copy)]
enum ExecutionPath {
    Row,
    Columnar,
}

#[derive(Debug, Clone, Copy)]
struct ComparisonReport {
    rows: usize,
    iterations: usize,
    row_ns: u128,
    columnar_ns: u128,
    row_checksum: u64,
    columnar_checksum: u64,
}

impl ComparisonReport {
    fn json(self) -> serde_json::Value {
        let row_ns_per_iteration = self.row_ns as f64 / self.iterations as f64;
        let columnar_ns_per_iteration = self.columnar_ns as f64 / self.iterations as f64;
        json!({
            "rows": self.rows,
            "iterations_per_sample": self.iterations,
            "samples": SAMPLES,
            "row_median_ns": self.row_ns,
            "columnar_median_ns": self.columnar_ns,
            "row_ns_per_iteration": row_ns_per_iteration,
            "columnar_ns_per_iteration": columnar_ns_per_iteration,
            "row_rows_per_second": self.rows as f64 * 1_000_000_000.0 / row_ns_per_iteration,
            "columnar_rows_per_second": self.rows as f64 * 1_000_000_000.0 / columnar_ns_per_iteration,
            "speedup": self.row_ns as f64 / self.columnar_ns as f64,
            "improved": self.columnar_ns < self.row_ns,
            "row_checksum": self.row_checksum,
            "columnar_checksum": self.columnar_checksum,
        })
    }
}
