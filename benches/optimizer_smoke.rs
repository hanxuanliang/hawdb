use skein::optimizer::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
    OptimizerConfig, PlanCost,
};
use skein::planner::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, LogicalPlan, Predicate,
    Projection, ProjectionExpression, RelationshipCountLeg, SortDirection, SortItem, SortKey,
};
use skein::RelationshipDirection;
use skein::Value;
use std::collections::BTreeMap;
use std::time::Instant;

const ITERATIONS: usize = 2_000;

fn main() {
    let cases = optimizer_smoke_cases();
    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 128 });

    let expected = cases
        .iter()
        .map(|case| {
            let (_, trace) = optimizer.optimize_with_catalog(&case.logical, &case.catalog);
            assert_trace(case, &trace);
            trace.selected_plan_fingerprint
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for (case, expected_fingerprint) in cases.iter().zip(expected.iter()) {
            let (_, trace) = optimizer.optimize_with_catalog(&case.logical, &case.catalog);
            assert_trace(case, &trace);
            assert_eq!(&trace.selected_plan_fingerprint, expected_fingerprint);
        }
    }
    let elapsed = start.elapsed();
    println!(
        "optimizer_smoke cases={} iterations={ITERATIONS} elapsed_ms={} fingerprints={}",
        cases.len(),
        elapsed.as_millis(),
        expected.join("|")
    );

    let budgeted = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
    let (_, budgeted_trace) = budgeted.optimize_with_catalog(&cases[0].logical, &cases[0].catalog);
    assert!(budgeted_trace
        .warnings
        .iter()
        .any(|warning| warning.contains("optimizer memo budget exceeded")));
    assert_eq!(budgeted_trace.selected_plan_cost, cases[0].expected_cost);
}

struct OptimizerSmokeCase {
    name: &'static str,
    logical: LogicalPlan,
    catalog: OptimizerCatalog,
    expected_cost: PlanCost,
    fingerprint_contains: &'static str,
    decision_contains: &'static [&'static str],
}

fn assert_trace(case: &OptimizerSmokeCase, trace: &skein::optimizer::OptimizerTrace) {
    assert!(
        trace.warnings.is_empty(),
        "{} unexpectedly warned: {:?}",
        case.name,
        trace.warnings
    );
    assert_eq!(
        trace.selected_plan_cost, case.expected_cost,
        "{} selected cost changed",
        case.name
    );
    assert!(
        trace
            .selected_plan_fingerprint
            .contains(case.fingerprint_contains),
        "{} fingerprint did not contain {}: {}",
        case.name,
        case.fingerprint_contains,
        trace.selected_plan_fingerprint
    );
    for expected in case.decision_contains {
        assert!(
            trace
                .decisions
                .iter()
                .any(|decision| decision.contains(expected)),
            "{} missing decision containing {}: {:?}",
            case.name,
            expected,
            trace.decisions
        );
    }
}

fn optimizer_smoke_cases() -> Vec<OptimizerSmokeCase> {
    vec![
        OptimizerSmokeCase {
            name: "range_expand",
            logical: range_expand_plan(),
            catalog: range_expand_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 7,
                cost: 366,
            },
            fingerprint_contains: "IndexNodeRangeSeek",
            decision_contains: &[
                "choose IndexNodeRangeSeek",
                "estimate AdjacencyExpand",
                "estimated_rows=125",
                "selected physical plan cost: estimated_rows=7 cost=366",
            ],
        },
        OptimizerSmokeCase {
            name: "composite_seek",
            logical: composite_seek_plan(),
            catalog: composite_seek_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 6,
            },
            fingerprint_contains: "IndexNodeCompositeSeek",
            decision_contains: &[
                "choose IndexNodeCompositeSeek",
                "selected physical plan cost: estimated_rows=1 cost=6",
            ],
        },
        OptimizerSmokeCase {
            name: "text_seek",
            logical: text_seek_plan(),
            catalog: text_seek_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 250,
                cost: 1003,
            },
            fingerprint_contains: "IndexNodeTextSeek",
            decision_contains: &[
                "choose IndexNodeTextSeek",
                "selected physical plan cost: estimated_rows=250 cost=1003",
            ],
        },
        OptimizerSmokeCase {
            name: "residual_node_string_contains",
            logical: residual_node_string_contains_plan(),
            catalog: residual_node_string_contains_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 250,
                cost: 2254,
            },
            fingerprint_contains: "PropertyContains",
            decision_contains: &[
                "selected physical plan cost: estimated_rows=250 cost=2254",
            ],
        },
        OptimizerSmokeCase {
            name: "low_selectivity_scan",
            logical: low_selectivity_scan_plan(),
            catalog: low_selectivity_scan_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1000,
                cost: 3004,
            },
            fingerprint_contains: "SeqNodeScan",
            decision_contains: &[
                "choose SeqNodeScan",
                "selected physical plan cost: estimated_rows=1000 cost=3004",
            ],
        },
        OptimizerSmokeCase {
            name: "memory_seed_entity_mentions",
            logical: memory_seed_entity_mentions_plan(),
            catalog: memory_seed_entity_mentions_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 15,
            },
            fingerprint_contains: "IndexNodeSeek",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "estimate AdjacencyExpand",
                "selected physical plan cost: estimated_rows=1 cost=15",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_property_expand",
            logical: relationship_property_expand_plan(),
            catalog: relationship_property_expand_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 6,
            },
            fingerprint_contains: "AdjacencyExpandExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "rel_property_distinct_product=10",
                "selected physical plan cost: estimated_rows=1 cost=6",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_status_range_filter",
            logical: relationship_status_range_filter_plan(),
            catalog: relationship_status_range_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 9,
            },
            fingerprint_contains: "FilterExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "rel_property_distinct_product=2",
                "selected physical plan cost: estimated_rows=1 cost=9",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_property_in_filter",
            logical: relationship_property_in_filter_plan(),
            catalog: relationship_property_in_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1_200,
                cost: 10_004,
            },
            fingerprint_contains: "PropertyIn",
            decision_contains: &[
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "selected physical plan cost: estimated_rows=1200 cost=10004",
            ],
        },
        OptimizerSmokeCase {
            name: "source_memory_label_cross_pattern",
            logical: source_memory_label_cross_pattern_plan(),
            catalog: source_memory_label_cross_pattern_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 10,
                cost: 1434,
            },
            fingerprint_contains: "AdjacencyExpandExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "estimate AdjacencyExpand for Source-[:SOURCED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:HAS_LABEL*1..1]->Label",
                "selected physical plan cost: estimated_rows=10 cost=1434",
            ],
        },
        OptimizerSmokeCase {
            name: "source_memory_entity_label_workload",
            logical: source_memory_entity_label_workload_plan(),
            catalog: source_memory_entity_label_workload_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 25,
                cost: 2445,
            },
            fingerprint_contains: "SortExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "estimate AdjacencyExpand for Source-[:SOURCED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "estimate AdjacencyExpand for Memory-[:HAS_LABEL*1..1]->Label",
                "selected physical plan cost: estimated_rows=25 cost=2445",
            ],
        },
        OptimizerSmokeCase {
            name: "community_synthesized_source_coverage",
            logical: community_synthesized_source_coverage_plan(),
            catalog: community_synthesized_source_coverage_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 21,
            },
            fingerprint_contains: "AggregateExec",
            decision_contains: &[
                "choose IndexNodeSeek for Community.id",
                "estimate AdjacencyExpand for Community-[:SYNTHESIZED_FROM*1..1]->Source",
                "selected physical plan cost: estimated_rows=1 cost=21",
            ],
        },
        OptimizerSmokeCase {
            name: "thread_cleanup_optional_count",
            logical: thread_cleanup_optional_count_plan(),
            catalog: thread_cleanup_optional_count_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 11,
            },
            fingerprint_contains: "OptionalRelationshipCountSumExec",
            decision_contains: &[
                "estimate OptionalRelationshipCountSum for Thread: seed_rows=1 leg_rows=[CONTAINS:out:5] estimated_rows=1 cost=11",
                "selected physical plan cost: estimated_rows=1 cost=11",
            ],
        },
        OptimizerSmokeCase {
            name: "endpoint_existence_cartesian_product",
            logical: endpoint_existence_cartesian_product_plan(),
            catalog: endpoint_existence_cartesian_product_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 8,
            },
            fingerprint_contains: "NodeCartesianProductExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "choose IndexNodeSeek for Source.id",
                "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=3 cost=7",
                "selected physical plan cost: estimated_rows=1 cost=8",
            ],
        },
        OptimizerSmokeCase {
            name: "nested_endpoint_existence_cartesian_product",
            logical: nested_endpoint_existence_cartesian_product_plan(),
            catalog: nested_endpoint_existence_cartesian_product_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 14,
            },
            fingerprint_contains: "NodeCartesianProductExec(IndexNodeSeek(1:e:6:Entity",
            decision_contains: &[
                "order NodeCartesianProduct single-row inputs: inputs=3",
                "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=9 cost=13",
                "selected physical plan cost: estimated_rows=1 cost=14",
            ],
        },
        OptimizerSmokeCase {
            name: "post_product_node_property_filter",
            logical: post_product_node_property_filter_plan(),
            catalog: post_product_node_property_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 100,
                cost: 3007,
            },
            fingerprint_contains: "FilterExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "keep NodeCartesianProduct input order: left_rows=1000 right_rows=1 reason=non_single_row_input",
                "selected physical plan cost: estimated_rows=100 cost=3007",
            ],
        },
        OptimizerSmokeCase {
            name: "residual_node_property_in",
            logical: residual_node_property_in_plan(),
            catalog: residual_node_property_in_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 30,
                cost: 2004,
            },
            fingerprint_contains: "PropertyIn",
            decision_contains: &[
                "selected physical plan cost: estimated_rows=30 cost=2004",
            ],
        },
    ]
}

fn range_expand_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "e".to_string(),
                property: "name".to_string(),
            },
            name: "name".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "LINKS".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 2,
            max_hops: 2,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::And(vec![
                    Predicate::PropertyCompare {
                        variable: "m".to_string(),
                        property: "created_at".to_string(),
                        op: ComparisonOp::Gte,
                        value: Value::Int(10),
                    },
                    Predicate::PropertyCompare {
                        variable: "m".to_string(),
                        property: "created_at".to_string(),
                        op: ComparisonOp::Lt,
                        value: Value::Int(20),
                    },
                ]),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    }
}

fn range_expand_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [],
            [("Memory".to_string(), "created_at".to_string())],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 2_000)],
            [("LINKS".to_string(), 500)],
            [("LINKS".to_string(), 250)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                25,
            )],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                    2,
                ),
                125,
            )],
            [],
            [(
                ("Memory".to_string(), "created_at".to_string()),
                (0..100).map(Value::Int).collect::<Vec<_>>(),
            )],
        ),
    )
}

fn composite_seek_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(42),
            },
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: Value::String("note".to_string()),
            },
        ]),
        input: Box::new(memory_scan()),
    })
}

fn composite_seek_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 1_000),
                (("Memory".to_string(), "kind".to_string()), 10),
            ],
            [],
        ),
    )
}

fn text_seek_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(memory_scan()),
    })
}

fn text_seek_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], [("Memory".to_string(), "body".to_string())]),
        OptimizerCatalogStatistics::new([("Memory".to_string(), 1_000)], [], [], [], [], [], []),
    )
}

fn residual_node_string_contains_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(memory_scan()),
    })
}

fn residual_node_string_contains_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "body".to_string()), 100)],
            [],
        ),
    )
}

fn low_selectivity_scan_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(memory_scan()),
    })
}

fn low_selectivity_scan_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "kind".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "kind".to_string()), 1)],
            [],
        ),
    )
}

fn memory_seed_entity_mentions_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("mention_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "e".to_string(),
                            property: "id".to_string(),
                        },
                        name: "entity_id".to_string(),
                    },
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "e".to_string(),
                            property: "name".to_string(),
                        },
                        name: "entity_name".to_string(),
                    },
                ],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("m".to_string()),
                    distinct: true,
                    name: "mention_count".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: Default::default(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "e".to_string(),
                    target_label: "Entity".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Filter {
                        predicate: Predicate::PropertyEq {
                            variable: "m".to_string(),
                            property: "id".to_string(),
                            value: Value::String("memory-42".to_string()),
                        },
                        input: Box::new(memory_scan()),
                    }),
                }),
            }),
        }),
    }
}

fn memory_seed_entity_mentions_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        ),
    )
}

fn relationship_property_expand_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "entity".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "weight".to_string(),
                },
                name: "weight".to_string(),
            },
        ],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::from([("weight".to_string(), Value::Int(4))]),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::String("memory-42".to_string()),
                },
                input: Box::new(memory_scan()),
            }),
        }),
    }
}

fn relationship_property_expand_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    )
}

fn relationship_status_range_filter_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "entity".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "created_at".to_string(),
                },
                name: "created_at".to_string(),
            },
        ],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyCompare {
                variable: "r".to_string(),
                property: "created_at".to_string(),
                op: ComparisonOp::Gt,
                value: Value::Int(80),
            },
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::from([(
                    "status".to_string(),
                    Value::String("active".to_string()),
                )]),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::String("memory-42".to_string()),
                    },
                    input: Box::new(memory_scan()),
                }),
            }),
        }),
    }
}

fn relationship_status_range_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "status".to_string()),
            2,
        )])
        .with_relationship_property_histograms([(
            ("MENTIONS".to_string(), "created_at".to_string()),
            (0..10).map(|bucket| Value::Int(bucket * 10)).collect(),
        )]),
    )
}

fn relationship_property_in_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "r".to_string(),
            property: "kind".to_string(),
            values: vec![
                Value::String("mentioned".to_string()),
                Value::String("quoted".to_string()),
                Value::String("quoted".to_string()),
                Value::String("linked".to_string()),
            ],
        },
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(memory_scan()),
        }),
    }
}

fn relationship_property_in_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
            [("MENTIONS".to_string(), 4_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                4_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                4_000,
            )],
            [],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "kind".to_string()),
            10,
        )]),
    )
}

fn source_memory_label_cross_pattern_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(20),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("memory_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "l".to_string(),
                        property: "name".to_string(),
                    },
                    name: "label_name".to_string(),
                }],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("m".to_string()),
                    distinct: true,
                    name: "memory_count".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "HAS_LABEL".to_string(),
                    rel_properties: Default::default(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "l".to_string(),
                    target_label: "Label".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "s".to_string(),
                        source_label: "Source".to_string(),
                        rel_variable: None,
                        rel_type: "SOURCED_FROM".to_string(),
                        rel_properties: Default::default(),
                        direction: RelationshipDirection::Incoming,
                        target_variable: "m".to_string(),
                        target_label: "Memory".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Filter {
                            predicate: Predicate::PropertyEq {
                                variable: "s".to_string(),
                                property: "id".to_string(),
                                value: Value::String("source-42".to_string()),
                            },
                            input: Box::new(LogicalPlan::NodeScan {
                                variable: "s".to_string(),
                                label: "Source".to_string(),
                            }),
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn source_memory_label_cross_pattern_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Source".to_string(), 1_000),
                ("Memory".to_string(), 100_000),
                ("Label".to_string(), 2_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 250_000),
                ("HAS_LABEL".to_string(), 180_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 50_000),
                ("HAS_LABEL".to_string(), 80_000),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    250_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    180_000,
                ),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    250_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    180_000,
                ),
            ],
            [
                (("Source".to_string(), "id".to_string()), 1_000),
                (("Label".to_string(), "name".to_string()), 10),
            ],
            [],
        ),
    )
}

fn source_memory_entity_label_workload_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(25),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("memory_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "e".to_string(),
                            property: "community_id".to_string(),
                        },
                        name: "community_id".to_string(),
                    },
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "l".to_string(),
                            property: "name".to_string(),
                        },
                        name: "label_name".to_string(),
                    },
                ],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("m".to_string()),
                    distinct: true,
                    name: "memory_count".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "HAS_LABEL".to_string(),
                    rel_properties: Default::default(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "l".to_string(),
                    target_label: "Label".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "m".to_string(),
                        source_label: "Memory".to_string(),
                        rel_variable: None,
                        rel_type: "MENTIONS".to_string(),
                        rel_properties: Default::default(),
                        direction: RelationshipDirection::Outgoing,
                        target_variable: "e".to_string(),
                        target_label: "Entity".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Expand {
                            source_variable: "s".to_string(),
                            source_label: "Source".to_string(),
                            rel_variable: None,
                            rel_type: "SOURCED_FROM".to_string(),
                            rel_properties: Default::default(),
                            direction: RelationshipDirection::Incoming,
                            target_variable: "m".to_string(),
                            target_label: "Memory".to_string(),
                            min_hops: 1,
                            max_hops: 1,
                            optional: false,
                            input: Box::new(LogicalPlan::Filter {
                                predicate: Predicate::PropertyEq {
                                    variable: "s".to_string(),
                                    property: "id".to_string(),
                                    value: Value::String("source-42".to_string()),
                                },
                                input: Box::new(LogicalPlan::NodeScan {
                                    variable: "s".to_string(),
                                    label: "Source".to_string(),
                                }),
                            }),
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn source_memory_entity_label_workload_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Source".to_string(), 1_000),
                ("Memory".to_string(), 100_000),
                ("Entity".to_string(), 50_000),
                ("Label".to_string(), 2_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 250_000),
                ("MENTIONS".to_string(), 300_000),
                ("HAS_LABEL".to_string(), 180_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 50_000),
                ("MENTIONS".to_string(), 90_000),
                ("HAS_LABEL".to_string(), 80_000),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    300_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    180_000,
                ),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    300_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    180_000,
                ),
            ],
            [
                (("Source".to_string(), "id".to_string()), 1_000),
                (("Entity".to_string(), "community_id".to_string()), 8),
                (("Label".to_string(), "name".to_string()), 10),
            ],
            [],
        ),
    )
}

fn community_synthesized_source_coverage_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(1),
        input: Box::new(LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "c".to_string(),
                    property: "id".to_string(),
                },
                name: "cid".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("s".to_string()),
                distinct: true,
                name: "covered".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "c".to_string(),
                source_label: "Community".to_string(),
                rel_variable: None,
                rel_type: "SYNTHESIZED_FROM".to_string(),
                rel_properties: Default::default(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "s".to_string(),
                target_label: "Source".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "c".to_string(),
                        property: "id".to_string(),
                        value: Value::String("community-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "c".to_string(),
                        label: "Community".to_string(),
                    }),
                }),
            }),
        }),
    }
}

fn community_synthesized_source_coverage_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Community".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Community".to_string(), 5_000),
                ("Source".to_string(), 20_000),
            ],
            [("SYNTHESIZED_FROM".to_string(), 40_000)],
            [("SYNTHESIZED_FROM".to_string(), 12_000)],
            [(
                (
                    "Community".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Source".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Community".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Source".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Community".to_string(), "id".to_string()), 5_000)],
            [],
        ),
    )
}

fn thread_cleanup_optional_count_plan() -> LogicalPlan {
    LogicalPlan::OptionalRelationshipCountSum {
        variable: "t".to_string(),
        label: "Thread".to_string(),
        properties: BTreeMap::from([("id".to_string(), Value::String("thread-42".to_string()))]),
        legs: vec![RelationshipCountLeg {
            rel_type: "CONTAINS".to_string(),
            direction: RelationshipDirection::Outgoing,
            distinct: false,
            filter: None,
        }],
        output: "message_count".to_string(),
    }
}

fn thread_cleanup_optional_count_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Thread".to_string(), 1_000),
                ("Message".to_string(), 50_000),
            ],
            [("CONTAINS".to_string(), 5_000)],
            [("CONTAINS".to_string(), 1_000)],
            [],
            [],
            [(("Thread".to_string(), "id".to_string()), 1_000)],
            [],
        ),
    )
}

fn endpoint_existence_cartesian_product_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "id".to_string(),
            },
            name: "memory_id".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::String("memory-42".to_string()),
                },
                input: Box::new(memory_scan()),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        }),
    }
}

fn endpoint_existence_cartesian_product_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Memory".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    )
}

fn nested_endpoint_existence_cartesian_product_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "id".to_string(),
            },
            name: "memory_id".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeCartesianProduct {
                left: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::And(vec![
                        Predicate::PropertyEq {
                            variable: "m".to_string(),
                            property: "id".to_string(),
                            value: Value::String("memory-42".to_string()),
                        },
                        Predicate::PropertyEq {
                            variable: "m".to_string(),
                            property: "kind".to_string(),
                            value: Value::String("note".to_string()),
                        },
                    ]),
                    input: Box::new(memory_scan()),
                }),
                right: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "s".to_string(),
                        property: "id".to_string(),
                        value: Value::String("source-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "s".to_string(),
                        label: "Source".to_string(),
                    }),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "e".to_string(),
                    property: "id".to_string(),
                    value: Value::String("entity-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "e".to_string(),
                    label: "Entity".to_string(),
                }),
            }),
        }),
    }
}

fn nested_endpoint_existence_cartesian_product_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Entity".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Entity".to_string(), 50_000),
                ("Memory".to_string(), 10_000),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Entity".to_string(), "id".to_string()), 50_000),
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Memory".to_string(), "kind".to_string()), 2),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    )
}

fn post_product_node_property_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(memory_scan()),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        }),
    }
}

fn post_product_node_property_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Source".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "kind".to_string()), 10),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    )
}

fn residual_node_property_in_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(3)],
        },
        input: Box::new(memory_scan()),
    }
}

fn residual_node_property_in_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    )
}

fn project_memory_title(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(input),
    }
}

fn memory_scan() -> LogicalPlan {
    LogicalPlan::NodeScan {
        variable: "m".to_string(),
        label: "Memory".to_string(),
    }
}
