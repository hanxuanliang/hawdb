use skein::optimizer::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
    OptimizerConfig, PlanCost,
};
use skein::planner::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, LogicalPlan, Predicate,
    Projection, ProjectionExpression, SortDirection, SortItem, SortKey,
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
                estimated_rows: 125,
                cost: 878,
            },
            fingerprint_contains: "IndexNodeTextSeek",
            decision_contains: &[
                "choose IndexNodeTextSeek",
                "selected physical plan cost: estimated_rows=125 cost=878",
            ],
        },
        OptimizerSmokeCase {
            name: "low_selectivity_scan",
            logical: low_selectivity_scan_plan(),
            catalog: low_selectivity_scan_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 500,
                cost: 2504,
            },
            fingerprint_contains: "SeqNodeScan",
            decision_contains: &[
                "choose SeqNodeScan",
                "selected physical plan cost: estimated_rows=500 cost=2504",
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
