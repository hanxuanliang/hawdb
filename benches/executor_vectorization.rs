use serde_json::json;
use skein::executor::{execute_with_row_limit_and_context, execute_with_row_limit_profile};
use skein::optimizer::PhysicalPlan;
use skein::planner::{ComparisonOp, Predicate, Projection, ProjectionExpression};
use skein::schema::{Catalog, PropertyType, TableKind};
use skein::store::GraphStore;
use skein::Value;
use skein_core::RuntimeTaskContext;
use skein_executor::{
    execute_morsels_ordered, filter_numeric_column, ColumnVector, MorselAdmission,
    MorselAdmissionRequest, NumericLiteral, PipelineId, Selection, SharedExecutorPool,
    SharedPoolMorselScheduler, Validity,
};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const MICRO_ROWS: usize = 131_072;
const MICRO_ITERATIONS: usize = 64;
const END_TO_END_ROWS: usize = 65_536;
const END_TO_END_ITERATIONS: usize = 16;
const END_TO_END_PAYLOAD_BYTES: usize = 256;
const SAMPLES: usize = 11;
const MORSEL_ROWS: usize = 1_048_576;
const MORSEL_TARGET_ROWS: usize = 16_384;
const MORSEL_ITERATIONS: usize = 8;

fn main() {
    let micro = micro_benchmark();
    let (end_to_end, production_morsel) = end_to_end_benchmark();
    let morsel = morsel_benchmark();
    assert_eq!(micro.row_checksum, micro.columnar_checksum);
    assert_eq!(end_to_end.row_checksum, end_to_end.columnar_checksum);

    println!(
        "executor_vectorization {}",
        json!({
            "micro": micro.json(),
            "end_to_end": end_to_end.json(),
            "morsel": morsel,
            "production_morsel": production_morsel,
            "end_to_end_payload_bytes_per_row": END_TO_END_PAYLOAD_BYTES,
        })
    );
}

fn morsel_benchmark() -> serde_json::Value {
    let requested_workers = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .min(NonZeroUsize::new(4).unwrap());
    let bytes_per_worker = NonZeroUsize::new(64 * 1024).unwrap();
    let admission = MorselAdmission::try_new(MorselAdmissionRequest {
        pipeline_id: PipelineId(1),
        input_rows: MORSEL_ROWS,
        target_rows: NonZeroUsize::new(MORSEL_TARGET_ROWS).unwrap(),
        requested_parallelism: requested_workers,
        bytes_per_worker,
        memory_budget_bytes: NonZeroUsize::new(
            bytes_per_worker
                .get()
                .saturating_mul(requested_workers.get()),
        )
        .unwrap(),
    })
    .unwrap();
    let input = (0..MORSEL_ROWS)
        .map(|row| (row as u64).wrapping_mul(0x9e37_79b9))
        .collect::<Vec<_>>();
    let pool = SharedExecutorPool::new(requested_workers).unwrap();
    let scheduler = SharedPoolMorselScheduler::new(pool);
    let mut sequential_checksum = 0u64;
    let mut parallel_checksum = 0u64;
    let mut sequential_samples = Vec::with_capacity(SAMPLES);
    let mut parallel_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let parallel_first = sample % 2 == 1;
        for parallel in [parallel_first, !parallel_first] {
            let started = Instant::now();
            for _ in 0..MORSEL_ITERATIONS {
                let outputs = if parallel {
                    scheduler
                        .execute(&admission, |morsel| {
                            Ok(morsel_checksum(
                                &input[morsel.start_row..morsel.start_row + morsel.row_count],
                            ))
                        })
                        .unwrap()
                } else {
                    execute_morsels_ordered(&admission, |morsel| {
                        Ok(morsel_checksum(
                            &input[morsel.start_row..morsel.start_row + morsel.row_count],
                        ))
                    })
                    .unwrap()
                };
                let checksum = outputs
                    .into_iter()
                    .fold(0u64, |total, value| total.wrapping_add(value));
                if parallel {
                    parallel_checksum = black_box(checksum);
                } else {
                    sequential_checksum = black_box(checksum);
                }
            }
            if parallel {
                parallel_samples.push(started.elapsed().as_nanos());
            } else {
                sequential_samples.push(started.elapsed().as_nanos());
            }
        }
    }
    assert_eq!(parallel_checksum, sequential_checksum);
    sequential_samples.sort_unstable();
    parallel_samples.sort_unstable();
    let sequential_ns = sequential_samples[SAMPLES / 2];
    let parallel_ns = parallel_samples[SAMPLES / 2];
    json!({
        "rows": MORSEL_ROWS,
        "target_rows": MORSEL_TARGET_ROWS,
        "morsel_count": admission.morsel_count(),
        "admitted_workers": admission.max_workers(),
        "iterations_per_sample": MORSEL_ITERATIONS,
        "samples": SAMPLES,
        "sequential_median_ns": sequential_ns,
        "shared_pool_median_ns": parallel_ns,
        "speedup": sequential_ns as f64 / parallel_ns.max(1) as f64,
        "checksum": parallel_checksum,
    })
}

fn morsel_checksum(input: &[u64]) -> u64 {
    input.iter().fold(0u64, |total, value| {
        total.wrapping_add(value.rotate_left(17).wrapping_mul(0xbf58_476d_1ce4_e5b9))
    })
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

fn end_to_end_benchmark() -> (ComparisonReport, serde_json::Value) {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", PropertyType::Int, false);
    catalog.get_or_create_property(table, "payload", PropertyType::String, false);
    let mut store = GraphStore::in_memory();
    for row in 0..END_TO_END_ROWS {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    ("score".to_string(), Value::Int(row as i64)),
                    (
                        "payload".to_string(),
                        Value::String("x".repeat(END_TO_END_PAYLOAD_BYTES)),
                    ),
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

    let comparison = ComparisonReport {
        rows: END_TO_END_ROWS,
        iterations: END_TO_END_ITERATIONS,
        row_ns,
        columnar_ns,
        row_checksum,
        columnar_checksum,
    };
    let requested_workers = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .min(NonZeroUsize::new(4).unwrap());
    let serial_context = RuntimeTaskContext::default();
    let parallel_context =
        RuntimeTaskContext::default().with_admitted_parallelism(requested_workers);
    let mut serial_samples = Vec::with_capacity(SAMPLES);
    let mut parallel_samples = Vec::with_capacity(SAMPLES);
    let mut serial_checksum = 0u64;
    let mut parallel_checksum = 0u64;
    for sample in 0..SAMPLES {
        let parallel_first = sample % 2 == 1;
        for parallel in [parallel_first, !parallel_first] {
            let context = if parallel {
                &parallel_context
            } else {
                &serial_context
            };
            let started = Instant::now();
            for _ in 0..END_TO_END_ITERATIONS {
                let rows = execute_with_row_limit_and_context(
                    black_box(&columnar_plan),
                    &mut catalog,
                    &mut store,
                    None,
                    context,
                )
                .expect("morsel production benchmark execution must succeed");
                let checksum = black_box(output_checksum(&rows));
                if parallel {
                    parallel_checksum = checksum;
                } else {
                    serial_checksum = checksum;
                }
            }
            if parallel {
                parallel_samples.push(started.elapsed().as_nanos());
            } else {
                serial_samples.push(started.elapsed().as_nanos());
            }
        }
    }
    assert_eq!(serial_checksum, parallel_checksum);
    serial_samples.sort_unstable();
    parallel_samples.sort_unstable();
    let serial_ns = serial_samples[SAMPLES / 2];
    let parallel_ns = parallel_samples[SAMPLES / 2];
    let production_morsel = json!({
        "rows": END_TO_END_ROWS,
        "admitted_workers": requested_workers,
        "iterations_per_sample": END_TO_END_ITERATIONS,
        "samples": SAMPLES,
        "serial_median_ns": serial_ns,
        "parallel_median_ns": parallel_ns,
        "speedup": serial_ns as f64 / parallel_ns.max(1) as f64,
        "improved": parallel_ns < serial_ns,
        "checksum": parallel_checksum,
    });
    (comparison, production_morsel)
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
