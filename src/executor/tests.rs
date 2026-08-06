//! Executor admission, streaming, spill, and graph operator regressions.

use super::*;
use crate::planner::{
    AggregateFunction, AggregateTarget, ProjectionExpression, SortDirection, SortKey,
};
use crate::store::{DurabilityPolicy, ScanPruningStrategy, StorageResidencyMode, WalReplayConfig};

fn spill_test_config(name: &str) -> ExecutionMemoryConfig {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(2).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        max_spill_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        max_spill_runs: NonZeroUsize::new(64).unwrap(),
        max_total_spill_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
        max_total_spill_runs: NonZeroUsize::new(256).unwrap(),
        min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
        spill_orphan_grace_period: std::time::Duration::ZERO,
        spill_directory: std::env::temp_dir().join(format!("skein-{name}-{nonce}")),
    }
}

#[test]
fn sort_pipeline_spills_runs_under_a_tight_memory_budget() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..12).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            name: "rank".to_string(),
        }],
        input: Box::new(PhysicalPlan::SortExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = spill_test_config("sort-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["rank"].clone())
            .collect::<Vec<_>>(),
        (0..12).map(Value::Int).collect::<Vec<_>>()
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "SortExec")
        .unwrap();
    assert_eq!(report.input_rows, 12);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 12);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    let pipeline = &output.profile.pipeline_memory_report;
    assert_eq!(pipeline.intermediate_rows, 36);
    assert!(pipeline.intermediate_payload_bytes >= pipeline.output_payload_bytes);
    assert_eq!(pipeline.peak_batch_rows, 2);
    assert_eq!(pipeline.output_rows, 12);
    assert!(pipeline.output_payload_bytes > 0);
    assert!(pipeline.start_resident_bytes.is_some());
    assert!(pipeline.steady_resident_bytes.is_some());
    assert!(pipeline.peak_resident_bytes.is_some());
    assert!(pipeline.total_page_faults.is_some());
    assert_eq!(pipeline.minor_page_faults.is_some(), cfg!(unix));
    assert_eq!(pipeline.major_page_faults.is_some(), cfg!(unix));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn grouped_aggregate_pipeline_spills_and_merges_groups() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("group", Value::Int(value % 8))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::All,
            distinct: false,
            name: "count".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = spill_test_config("aggregate-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| (row["group"].clone(), row["count"].clone()))
            .collect::<Vec<_>>(),
        vec![
            (Value::Int(0), Value::Int(3)),
            (Value::Int(1), Value::Int(3)),
            (Value::Int(2), Value::Int(3)),
            (Value::Int(3), Value::Int(3)),
            (Value::Int(4), Value::Int(2)),
            (Value::Int(5), Value::Int(2)),
            (Value::Int(6), Value::Int(2)),
            (Value::Int(7), Value::Int(2)),
        ]
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert_eq!(report.input_rows, 20);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn grouped_partial_aggregate_spill_does_not_write_unused_binding_payloads() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let payload = "x".repeat(4096);
    for value in 0..24 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("group", Value::Int(value % 8)),
                    ("value", Value::Int(value)),
                    ("payload", Value::String(payload.clone())),
                ]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![
            Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::All,
                distinct: false,
                name: "count".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "min".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "max".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "avg".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let mut memory = spill_test_config("aggregate-partial-spill");
    memory.blocking_operator_bytes = NonZeroUsize::new(2048).unwrap();
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(output.rows.len(), 8);
    for (group, row) in output.rows.iter().enumerate() {
        assert_eq!(row["group"], Value::Int(group as i64));
        assert_eq!(row["count"], Value::Int(3));
        assert_eq!(row["min"], Value::Int(group as i64));
        assert_eq!(row["max"], Value::Int(group as i64 + 16));
        assert_eq!(row["avg"], Value::Float(group as f64 + 8.0));
    }
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 24);
    assert!(report.spilled_bytes < 24 * payload.len() as u64);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn top_n_pipeline_spills_without_changing_order_or_offset() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..50).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            name: "rank".to_string(),
        }],
        input: Box::new(PhysicalPlan::TopNExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            offset: 7,
            limit: 5,
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = spill_test_config("topn-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["rank"].clone())
            .collect::<Vec<_>>(),
        (7..12).map(Value::Int).collect::<Vec<_>>()
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "TopNExec")
        .unwrap();
    assert!(report.spill_run_count > 1);
    assert!(report.spilled_rows > 0);
    assert!(report.spilled_rows <= report.input_rows);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn distinct_spills_and_deduplicates_across_memory_bounded_runs() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([(
                    "value",
                    Value::String(format!("{}-{}", value % 5, "x".repeat(96))),
                )]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::DistinctExec {
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                name: "value".to_string(),
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        ..spill_test_config("distinct-admission")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    assert_eq!(output.rows.len(), 5);
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "DistinctExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn collect_aggregate_rejects_unbounded_group_state() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([(
                    "value",
                    Value::String(format!("{value}-{}", "x".repeat(64))),
                )]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property {
                variable: "n".to_string(),
                property: "value".to_string(),
            },
            distinct: false,
            name: "values".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        ..spill_test_config("collect-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("AggregateExec state exceeds"));
}

#[test]
fn grouped_mixed_aggregate_spills_only_required_operands() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..64i64 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("group", Value::Int(value % 4)),
                    ("value", Value::Int(value)),
                    ("payload", Value::String("x".repeat(16 * 1024))),
                ]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![
            Aggregation {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "values".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: true,
                name: "distinct_values".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        ..spill_test_config("aggregate-compact-operands")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(output.rows.len(), 4);
    for row in &output.rows {
        assert_eq!(row["distinct_values"], Value::Int(16));
        let Value::List(values) = &row["values"] else {
            panic!("collect must return a list");
        };
        assert_eq!(values.len(), 16);
    }
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes < 64 * 16 * 1024);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn cartesian_product_spills_an_oversized_build_side() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Right",
                properties([("value", Value::Int(value))]),
            )
            .unwrap();
    }
    store
        .create_node(&mut catalog, "Left", BTreeMap::new())
        .unwrap();
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "left".to_string(),
            label: "Left".to_string(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "right".to_string(),
            label: "Right".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        ..spill_test_config("cartesian-admission")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    assert_eq!(output.rows.len(), 20);
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "NodeCartesianProductExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spill_run_count > 0);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn shortest_path_rejects_an_oversized_frontier() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    for _ in 0..32 {
        let middle = store
            .create_node(&mut catalog, "Node", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, middle, "LINK", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, middle, target, "LINK", BTreeMap::new())
            .unwrap();
    }
    let error = all_shortest_paths(
        &store,
        ShortestPathSearch {
            source,
            target,
            rel_type_id: catalog.rel_type_id("LINK"),
            direction: RelationshipDirection::Outgoing,
            min_hops: 1,
            max_hops: 2,
            path_node_visibility_filter: None,
        },
        NonZeroUsize::new(512).unwrap(),
        usize::MAX,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("blocking_operator_bytes"));
}

#[test]
fn sort_rejects_spill_run_count_over_budget() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..50).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            direction: SortDirection::Asc,
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        max_spill_runs: NonZeroUsize::new(1).unwrap(),
        ..spill_test_config("sort-run-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeded max_spill_runs 1"));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn sort_rejects_spill_bytes_over_budget_and_removes_partial_run() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..20).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            direction: SortDirection::Asc,
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        max_spill_bytes: NonZeroU64::new(32).unwrap(),
        ..spill_test_config("sort-byte-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeded max_spill_bytes 32"));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

fn graph_algorithm_fixture() -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    store
        .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
        .unwrap();
    store
        .register_projected_graph(
            "MemoryGraph",
            ProjectedGraphDefinition {
                node_labels: vec!["Memory".to_string()],
                rel_types: vec!["MENTIONS".to_string()],
            },
        )
        .unwrap();
    (catalog, store)
}

fn graph_algorithm_plan(algorithm: GraphAlgorithmKind) -> PhysicalPlan {
    PhysicalPlan::GraphAlgorithm {
        algorithm,
        graph_name: "MemoryGraph".to_string(),
        options: crate::planner::GraphAlgorithmOptions {
            damping: None,
            max_iterations: Some(2),
            max_levels: Some(1),
        },
        score_column: "score".to_string(),
        node_visibility_predicate: None,
    }
}

#[test]
fn graph_algorithms_admit_direction_specific_projections() {
    let (mut catalog, mut store) = graph_algorithm_fixture();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..spill_test_config("algorithm-admission")
    };

    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let plan = graph_algorithm_plan(algorithm);
        assert!(BatchPlanRef::try_new(&plan).is_some());
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();
        assert_eq!(output.rows.len(), 2);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "GraphAlgorithm")
            .unwrap();
        assert_eq!(report.budget_bytes, 4096);
        assert_eq!(report.input_rows, 2);
        assert!(report.peak_tracked_bytes > 0);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
        assert_eq!(report.spilled_bytes, 0);
    }
}

#[test]
fn graph_algorithm_rejects_scratch_before_allocation() {
    let (mut catalog, mut store) = graph_algorithm_fixture();
    let plan = graph_algorithm_plan(GraphAlgorithmKind::PageRank);
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(150).unwrap(),
        ..spill_test_config("algorithm-scratch-rejection")
    };
    let mut external = NoExternalReadOperator;

    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("GraphAlgorithm PageRank scratch and result state"));
    assert!(error
        .to_string()
        .contains("exceeding blocking_operator_bytes 150"));
}

#[test]
fn batch_plan_ref_rejects_an_unsupported_descendant() {
    let plan = PhysicalPlan::FilterExec {
        predicate: Predicate::ConstantBool(true),
        input: Box::new(PhysicalPlan::CreateNode {
            label: "Item".to_string(),
            properties: BTreeMap::new(),
        }),
    };

    assert!(BatchPlanRef::try_new(&plan).is_none());
}

#[test]
fn columnar_numeric_fragment_matches_row_pipeline_and_reports_morsels() {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Int, true);
    let mut store = GraphStore::in_memory();
    for row in 0..513i64 {
        let values = if row % 10 == 0 {
            properties([("name", Value::String(format!("item-{row}")))])
        } else {
            properties([
                ("score", Value::Int(row)),
                ("name", Value::String(format!("item-{row}"))),
            ])
        };
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Float(480.0),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "name".to_string(),
            },
            name: "name".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::LimitExec {
        offset: 3,
        limit: Some(17),
        input: Box::new(PhysicalPlan::ProjectExec {
            items: items.clone(),
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: compare.clone(),
                input: Box::new(scan.clone()),
            }),
        }),
    };
    let row_plan = PhysicalPlan::LimitExec {
        offset: 3,
        limit: Some(17),
        input: Box::new(PhysicalPlan::ProjectExec {
            items,
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: Predicate::And(vec![compare]),
                input: Box::new(scan),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(4).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let morsel_count = 513usize.div_ceil(4 * 16);
    let expected_workers = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .get()
        .min(MAX_MORSEL_PARALLELISM)
        .min(morsel_count / 4)
        .max(1);
    let task_context = RuntimeTaskContext::default().with_admitted_parallelism(
        NonZeroUsize::new(MAX_MORSEL_PARALLELISM).expect("default morsel parallelism is non-zero"),
    );
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_output_limits_profile_and_external_and_context_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &task_context,
        &memory,
    )
    .unwrap();
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert_eq!(columnar.rows.len(), 17);
    let report = &columnar.profile.pipeline_memory_report;
    assert!(report.columnar_batches > 0);
    assert!(report.columnar_input_rows >= report.columnar_selected_rows);
    assert_eq!(report.columnar_batches, report.morsel_count);
    assert_eq!(report.morsel_max_admitted_workers, expected_workers);
    assert_eq!(report.morsel_peak_active_workers, 1);
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);

    let columnar_scan_plan = match &columnar_plan {
        PhysicalPlan::LimitExec { input, .. } => input.as_ref(),
        _ => unreachable!("test plan has a limit root"),
    };
    let row_scan_plan = match &row_plan {
        PhysicalPlan::LimitExec { input, .. } => input.as_ref(),
        _ => unreachable!("test plan has a limit root"),
    };
    let parallel = execute_with_output_limits_profile_and_external_and_context_and_memory(
        columnar_scan_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &task_context,
        &memory,
    )
    .unwrap();
    let sequential = execute_with_row_limit_profile_and_external_and_memory(
        row_scan_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(parallel.rows, sequential.rows);
    assert_eq!(
        parallel
            .profile
            .pipeline_memory_report
            .morsel_max_admitted_workers,
        expected_workers
    );
    assert_eq!(
        parallel
            .profile
            .pipeline_memory_report
            .morsel_peak_active_workers,
        expected_workers
    );
}

#[test]
fn columnar_lending_fragment_matches_row_for_narrow_numeric_projection() {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Float, true);
    let mut store = GraphStore::in_memory();
    for row in 0..65i64 {
        let values = if row % 9 == 0 {
            BTreeMap::new()
        } else {
            properties([("score", Value::Float(row as f64 + 0.5))])
        };
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Float(48.5),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Literal(Value::String("item".to_string())),
            name: "kind".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::ProjectExec {
        items: items.clone(),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: compare.clone(),
            input: Box::new(scan.clone()),
        }),
    };
    let row_plan = PhysicalPlan::ProjectExec {
        items,
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::And(vec![compare]),
            input: Box::new(scan),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_row_limit_profile_and_external_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert!(columnar.profile.pipeline_memory_report.columnar_batches > 1);
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);
}

#[test]
fn out_of_core_columnar_scan_drops_full_records_before_batching() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-columnar-owned-{nonce}"));
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Int, true);
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    for row in 0..32i64 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("score", Value::Int(row)),
                    ("payload", Value::String("x".repeat(4096))),
                ]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    drop(store);

    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    assert!(store.is_out_of_core());
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Int(24),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::ProjectExec {
        items: items.clone(),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: compare.clone(),
            input: Box::new(scan.clone()),
        }),
    };
    let row_plan = PhysicalPlan::ProjectExec {
        items,
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::And(vec![compare]),
            input: Box::new(scan),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_row_limit_profile_and_external_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    let row_error = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(row_error.to_string().contains("intermediate row uses"));
    let row_memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &row_memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert_eq!(columnar.rows.len(), 8);
    assert_eq!(columnar.profile.pipeline_memory_report.columnar_batches, 4);
    assert_eq!(
        columnar.profile.pipeline_memory_report.columnar_input_rows,
        32
    );
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn node_column_lookup_uses_property_index_pruning_for_exact_label() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:1".to_string())),
                ("title", Value::String("Graph foundations".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:2".to_string())),
                ("title", Value::String("Storage notes".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:3".to_string())),
                ("title", Value::String("Runtime notes".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Seed",
            properties([("target_stable_id", Value::String("memory:2".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Seed",
            properties([("target_stable_id", Value::String("memory:4".to_string()))]),
        )
        .unwrap();

    let plan = PhysicalPlan::ProjectExec {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "stable_id".to_string(),
                },
                name: "stable_id".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::NodeColumnLookupExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "stable_id".to_string(),
            column: "lookup_id".to_string(),
            optional: true,
            input: Box::new(PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "s".to_string(),
                        property: "target_stable_id".to_string(),
                    },
                    name: "lookup_id".to_string(),
                }],
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "s".to_string(),
                    label: "Seed".to_string(),
                }),
            }),
        }),
    };

    let output = execute_with_row_limit_profile(&plan, &mut catalog, &mut store, None).unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("stable_id"),
        Some(&Value::String("memory:2".to_string()))
    );
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Storage notes".to_string()))
    );
    assert_eq!(output.rows[1].get("stable_id"), Some(&Value::Null));
    assert_eq!(output.rows[1].get("title"), Some(&Value::Null));
    let lookup_scan = output
        .profile
        .scan_pruning_reports
        .iter()
        .find(|report| {
            report.strategy
                == ScanPruningStrategy::PropertyIn {
                    property: "stable_id".to_string(),
                }
        })
        .expect("node column lookup should emit property-in pruning evidence");
    assert_eq!(
        lookup_scan.target_kind,
        crate::store::ScanPruningTargetKind::Node
    );
    assert!(lookup_scan.pruned);
    assert!(!lookup_scan.exact_empty);
    assert_eq!(lookup_scan.candidate_count_before_pruning, 3);
    assert_eq!(lookup_scan.candidate_count_before_filter, 1);
    assert_eq!(lookup_scan.pruned_candidate_count, 2);
    assert_eq!(lookup_scan.output_count, 2);
}

#[test]
fn source_segment_scan_uses_checkpoint_sidecar_and_keeps_filter_semantics() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-source-segment-executor-{nonce}"));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([
                ("id".to_string(), Value::String("source-a".to_string())),
                ("space_id".to_string(), Value::String("alpha".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([
                ("id".to_string(), Value::String("source-b".to_string())),
                ("space_id".to_string(), Value::String("beta".to_string())),
            ]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();

    let predicate = Predicate::PropertyEq {
        variable: "s".to_string(),
        property: "space_id".to_string(),
        value: Value::String("alpha".to_string()),
    };
    let plan = PhysicalPlan::FilterExec {
        predicate: predicate.clone(),
        input: Box::new(PhysicalPlan::SourceSegmentScan {
            variable: "s".to_string(),
            predicate,
        }),
    };
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let memory = ExecutionMemoryConfig::default();
    let observer = QueryExecutionObserver::default();
    let mut context = ExecutionContext {
        parameters: &parameters,
        external: &mut external,
        memory: &memory,
        task_context: None,
        observer: &observer,
    };
    let bindings = execute_bindings_with_limit(
        &plan,
        &mut catalog,
        &mut store,
        &mut context,
        ExecutionLimit::unlimited(),
    )
    .unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].nodes["s"].properties["id"],
        Value::String("source-a".to_string())
    );
    std::fs::remove_dir_all(path).unwrap();
}

fn properties(items: impl IntoIterator<Item = (&'static str, Value)>) -> BTreeMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
