use serde_json::json;
use skein::executor::{execute_with_row_limit_profile, ExecutionMemoryConfig};
use skein::optimizer::PhysicalPlan;
use skein::planner::{ComparisonOp, Predicate, Projection, ProjectionExpression};
use skein::schema::{Catalog, PropertyType, TableKind};
use skein::store::{GraphSnapshotNodeImport, GraphStore, NodeId};
use skein::Value;
use skein_core::RuntimeTaskContext;
use skein_executor::{filter_numeric_column, ColumnVector, NumericLiteral, Selection, Validity};
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
const MORSEL_MATRIX_SAMPLES: usize = 3;
const LOCAL_MORSEL_BENCHMARK_PROTOCOL: &str = "skein-local-morsel-benchmark-v1";
const EXECUTOR_BENCH_MODE_ENV: &str = "SKEIN_EXECUTOR_BENCH_MODE";

#[path = "executor_vectorization/morsel.rs"]
mod morsel;

fn main() {
    let requested_workers = morsel::benchmark_workers();
    let mode = std::env::var(EXECUTOR_BENCH_MODE_ENV).unwrap_or_else(|_| "full".to_string());
    assert!(
        matches!(mode.as_str(), "full" | "scheduler" | "morsel"),
        "{EXECUTOR_BENCH_MODE_ENV} must be full, scheduler, or morsel"
    );
    let full = mode == "full";
    let micro = full.then(micro_benchmark);
    let (end_to_end, production_morsel) = if mode == "scheduler" {
        (None, None)
    } else {
        let (comparison, production) = end_to_end_benchmark(requested_workers, full);
        (comparison, Some(production))
    };
    let morsel = matches!(mode.as_str(), "full" | "scheduler")
        .then(|| morsel::scheduler_benchmark(requested_workers));
    if let Some(micro) = micro {
        assert_eq!(micro.row_checksum, micro.columnar_checksum);
    }
    if let Some(end_to_end) = end_to_end {
        assert_eq!(end_to_end.row_checksum, end_to_end.columnar_checksum);
    }

    println!(
        "executor_vectorization {}",
        json!({
            "micro": micro.map(ComparisonReport::json),
            "end_to_end": end_to_end.map(ComparisonReport::json),
            "morsel": morsel,
            "production_morsel": production_morsel,
            "end_to_end_payload_bytes_per_row": END_TO_END_PAYLOAD_BYTES,
            "mode": mode,
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

fn end_to_end_benchmark(
    requested_workers: NonZeroUsize,
    include_vectorization_comparison: bool,
) -> (Option<ComparisonReport>, serde_json::Value) {
    let workload_rows = if include_vectorization_comparison {
        END_TO_END_ROWS
    } else {
        morsel::benchmark_production_rows(requested_workers)
    };
    let iterations = if include_vectorization_comparison {
        END_TO_END_ITERATIONS
    } else {
        1
    };
    let samples = if include_vectorization_comparison {
        SAMPLES
    } else {
        MORSEL_MATRIX_SAMPLES
    };
    let memory = ExecutionMemoryConfig::default();
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", PropertyType::Int, false);
    catalog.get_or_create_property(table, "payload", PropertyType::String, false);
    let mut store = GraphStore::in_memory();
    let payload = "x".repeat(END_TO_END_PAYLOAD_BYTES);
    let nodes = (0..workload_rows)
        .map(|row| -> GraphSnapshotNodeImport {
            (
                NodeId(row as u64),
                "Item".to_string(),
                BTreeMap::from([
                    ("score".to_string(), Value::Int(row as i64)),
                    ("payload".to_string(), Value::String(payload.clone())),
                ]),
            )
        })
        .collect();
    store
        .import_graph_snapshot_rows(&mut catalog, nodes, Vec::new())
        .expect("benchmark node import must succeed");
    let threshold = if include_vectorization_comparison {
        workload_rows * 7 / 8
    } else {
        workload_rows.saturating_sub(workload_rows / 1024)
    } as i64;
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: ComparisonOp::Gte,
        value: Value::Int(threshold),
    };
    let columnar_plan = projection_plan(compare.clone());
    let comparison = include_vectorization_comparison.then(|| {
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
    });
    let serial_context = RuntimeTaskContext::default();
    let parallel_context =
        RuntimeTaskContext::default().with_admitted_parallelism(requested_workers);
    let mut serial_samples = Vec::with_capacity(samples);
    let mut parallel_samples = Vec::with_capacity(samples);
    let mut serial_checksum = 0u64;
    let mut parallel_checksum = 0u64;
    for sample in 0..samples {
        let parallel_first = sample % 2 == 1;
        for parallel in [parallel_first, !parallel_first] {
            let context = if parallel {
                &parallel_context
            } else {
                &serial_context
            };
            let started = Instant::now();
            for _ in 0..iterations {
                let execution = morsel::stream_probe(
                    black_box(&columnar_plan),
                    &mut catalog,
                    &mut store,
                    context,
                    &memory,
                );
                let checksum = black_box(execution.checksum);
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
    let serial_ns = morsel::percentile(&serial_samples, 50);
    let parallel_ns = morsel::percentile(&parallel_samples, 50);
    let serial_probe = morsel::stream_probe(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &serial_context,
        &memory,
    );
    let parallel_probe = morsel::stream_probe(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &parallel_context,
        &memory,
    );
    assert_eq!(serial_probe.checksum, parallel_probe.checksum);
    assert!(serial_probe.fully_streamed && parallel_probe.fully_streamed);
    assert_eq!(parallel_probe.max_admitted_workers, requested_workers.get());
    assert_eq!(parallel_probe.peak_active_workers, requested_workers.get());
    let serial_ns_per_iteration = serial_ns as f64 / iterations as f64;
    let parallel_ns_per_iteration = parallel_ns as f64 / iterations as f64;
    let production_morsel = json!({
        "protocol": LOCAL_MORSEL_BENCHMARK_PROTOCOL,
        "evidence_kind": "local_kernel_diagnostic",
        "production_eligible": false,
        "process_id": std::process::id(),
        "rows": workload_rows,
        "batch_rows": memory.batch_rows,
        "requested_workers": requested_workers,
        "morsel_count": parallel_probe.morsel_count,
        "morsel_max_admitted_workers": parallel_probe.max_admitted_workers,
        "morsel_peak_active_workers": parallel_probe.peak_active_workers,
        "iterations_per_sample": iterations,
        "samples": samples,
        "serial_p50_ns": serial_ns,
        "serial_p95_ns": morsel::percentile(&serial_samples, 95),
        "serial_p99_ns": morsel::percentile(&serial_samples, 99),
        "parallel_p50_ns": parallel_ns,
        "parallel_p95_ns": morsel::percentile(&parallel_samples, 95),
        "parallel_p99_ns": morsel::percentile(&parallel_samples, 99),
        "serial_rows_per_second": workload_rows as f64 * 1_000_000_000.0
            / serial_ns_per_iteration,
        "parallel_rows_per_second": workload_rows as f64 * 1_000_000_000.0
            / parallel_ns_per_iteration,
        "speedup": serial_ns as f64 / parallel_ns.max(1) as f64,
        "improved": parallel_ns < serial_ns,
        "steady_resident_bytes": parallel_probe.steady_resident_bytes,
        "peak_resident_bytes": parallel_probe.peak_resident_bytes,
        "minor_page_faults": parallel_probe.minor_page_faults,
        "major_page_faults": parallel_probe.major_page_faults,
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
    rows.iter()
        .fold(0u64, |total, row| total.wrapping_add(output_row_score(row)))
}

fn output_row_score(row: &BTreeMap<String, Value>) -> u64 {
    match row.get("score") {
        Some(Value::Int(value)) => *value as u64,
        _ => panic!("benchmark output score must be an integer"),
    }
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
