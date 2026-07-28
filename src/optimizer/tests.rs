use super::{
    CascadesOptimizer, Distribution, MemoryBudgetClass, OptimizerCatalog, OptimizerCatalogIndexes,
    OptimizerCatalogStatistics, OptimizerConfig, PhysicalOperatorDomain, PhysicalPlan,
    PhysicalPlanChildren, PhysicalPlanClass, PhysicalPlanKind, PlanCost, RuleOutcome,
    ScanPruningSupport, VectorPrecision,
};
use crate::cypher::RelationshipDirection;
use crate::planner::{
    AggregateFunction, AggregateTarget, Aggregation, LogicalPlan, Predicate, Projection,
    ProjectionExpression, RelationshipCountLeg, SortDirection, SortItem, SortKey,
};
use crate::value::Value;
use std::collections::BTreeMap;

#[test]
fn physical_plan_metadata_describes_kind_class_and_children() {
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            },
            input: Box::new(PhysicalPlan::IndexNodeSeek {
                variable: "m".to_string(),
                label: "Memory".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            }),
        }),
    };

    assert_eq!(plan.kind(), PhysicalPlanKind::ProjectExec);
    assert_eq!(plan.kind().as_str(), "ProjectExec");
    assert_eq!(plan.class(), PhysicalPlanClass::Relational);
    assert_eq!(plan.domain(), PhysicalOperatorDomain::Relational);
    assert_eq!(plan.class().as_str(), "relational");
    assert_eq!(plan.children().len(), 1);

    let PhysicalPlanChildren::Unary(filter) = plan.children() else {
        panic!("project should expose a unary child");
    };
    assert_eq!(filter.kind(), PhysicalPlanKind::FilterExec);
    assert_eq!(filter.class(), PhysicalPlanClass::Relational);

    let PhysicalPlanChildren::Unary(seek) = filter.children() else {
        panic!("filter should expose a unary child");
    };
    assert_eq!(seek.kind(), PhysicalPlanKind::IndexNodeSeek);
    assert_eq!(seek.class(), PhysicalPlanClass::Access);
    assert_eq!(seek.domain(), PhysicalOperatorDomain::Access);
    assert!(seek.children().is_empty());
}

#[test]
fn physical_plan_metadata_describes_binary_children() {
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "s".to_string(),
            label: "Source".to_string(),
        }),
    };

    assert_eq!(plan.kind(), PhysicalPlanKind::NodeCartesianProductExec);
    assert_eq!(plan.class(), PhysicalPlanClass::Relational);

    let PhysicalPlanChildren::Binary(left, right) = plan.children() else {
        panic!("cartesian product should expose binary children");
    };
    assert_eq!(left.kind(), PhysicalPlanKind::SeqNodeScan);
    assert_eq!(right.kind(), PhysicalPlanKind::SeqNodeScan);
    assert_eq!(left.class(), PhysicalPlanClass::Access);
    assert_eq!(right.class(), PhysicalPlanClass::Access);
}

#[test]
fn optimizer_trace_reports_physical_plan_operator_and_class_counts() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(
        trace.selected_plan_operator_counts.get("ProjectExec"),
        Some(&1)
    );
    assert_eq!(
        trace.selected_plan_operator_counts.get("IndexNodeSeek"),
        Some(&1)
    );
    assert_eq!(trace.selected_plan_operator_counts.get("FilterExec"), None);
    assert_eq!(trace.selected_plan_class_counts.get("relational"), Some(&1));
    assert_eq!(trace.selected_plan_class_counts.get("access"), Some(&1));
    assert_eq!(
        trace.selected_plan_cost_breakdown.as_plan_cost(),
        trace.selected_plan_cost
    );
    assert_eq!(trace.selected_plan_cost_breakdown.cpu, 1);
    assert_eq!(trace.selected_plan_cost_breakdown.random_io, 3);
    assert_eq!(trace.selected_plan_cost_breakdown.sequential_io, 0);

    let (_, fallback_trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 1 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(
        fallback_trace
            .selected_plan_operator_counts
            .get("ProjectExec"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace
            .selected_plan_operator_counts
            .get("IndexNodeSeek"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace.selected_plan_class_counts.get("relational"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace.selected_plan_class_counts.get("access"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace.selected_plan_cost_breakdown.as_plan_cost(),
        fallback_trace.selected_plan_cost
    );
}

#[test]
fn selected_plan_properties_report_distribution_and_sort_ordering() {
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Sort {
                items: vec![SortItem {
                    key: SortKey::Column("title".to_string()),
                    direction: SortDirection::Asc,
                }],
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &OptimizerCatalog::default());

    assert_eq!(
        trace.selected_plan_properties.distribution,
        Distribution::Single
    );
    assert_eq!(
        trace.selected_plan_properties.ordering,
        vec!["title asc".to_string()]
    );
    assert_eq!(
        trace.selected_plan_properties.scan_pruning,
        ScanPruningSupport::Label
    );
    assert_eq!(
        trace.selected_plan_properties.vector_precision,
        VectorPrecision::NotVector
    );
    assert_eq!(
        trace.selected_plan_properties.memory_budget,
        MemoryBudgetClass::Blocking
    );
}

#[test]
fn optimizer_budget_uses_direct_fallback_with_trace_warning() {
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(1),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );

    let budgeted = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
    let (_, budgeted_trace) = budgeted.optimize_with_catalog(&logical, &catalog);
    assert_eq!(budgeted_trace.groups, 4);
    assert!(budgeted_trace
        .warnings
        .iter()
        .any(|warning| warning.contains("optimizer memo budget exceeded")));
    assert!(budgeted_trace.selected_plan.contains("IndexNodeSeek"));
    assert!(budgeted_trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));

    let full = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 });
    let (_, full_trace) = full.optimize_with_catalog(&logical, &catalog);
    assert!(full_trace.warnings.is_empty());
    assert_eq!(
        budgeted_trace.selected_plan_fingerprint,
        full_trace.selected_plan_fingerprint
    );
}

#[test]
fn cartesian_product_trace_reports_input_rows_and_costs() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::String("memory-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
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
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("keep NodeCartesianProduct single-row input order: inputs=2")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=3 cost=7"
        }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 7,
        }
    );
}

#[test]
fn cartesian_product_orders_single_row_inputs_by_cost() {
    let logical = LogicalPlan::NodeCartesianProduct {
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
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
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
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Source".to_string(), "id".to_string())],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
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
                (("Memory".to_string(), "kind".to_string()), 2),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=2")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=5 cost=9"
        }));
    assert!(plan
        .fingerprint()
        .contains("NodeCartesianProductExec(IndexNodeSeek(1:s:6:Source"));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 9,
        }
    );
}

#[test]
fn cartesian_product_orders_nested_single_row_inputs() {
    let logical = LogicalPlan::NodeCartesianProduct {
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
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
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
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=3")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=9 cost=13"
        }));
    let fingerprint = plan.fingerprint();
    assert!(fingerprint.contains("NodeCartesianProductExec(IndexNodeSeek(1:e:6:Entity"));
    assert!(fingerprint.contains("IndexNodeSeek(1:s:6:Source"));
    assert!(fingerprint.contains("FilterExec(And(PropertyEq(1:m.2:id=string:9:memory-42)"));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 13,
        }
    );
}

#[test]
fn cartesian_product_keeps_nested_multi_row_inputs() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
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
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Entity".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Entity".to_string(), 50_000),
                ("Memory".to_string(), 10),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Entity".to_string(), "id".to_string()), 50_000),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
            decision == "keep NodeCartesianProduct input order: left_rows=10 right_rows=1 reason=non_single_row_input"
        }));
    assert!(plan.fingerprint().starts_with(
        "NodeCartesianProductExec(NodeCartesianProductExec(SeqNodeScan(1:m:6:Memory)"
    ));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 10,
            cost: 40,
        }
    );
}

#[test]
fn post_product_node_property_filter_uses_node_statistics() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
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
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 100,
            cost: 3_007,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=100 cost=3007"));
}

#[test]
fn residual_node_property_in_uses_distinct_value_count() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(3)],
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 30,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_property_in_empty_list_estimates_zero_rows() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: Vec::new(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 0,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_string_predicate_uses_distinct_count_cap() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 250,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_or_filter_uses_branch_selectivity() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::Or(vec![
            Predicate::ConstantBool(false),
            Predicate::PropertyEq {
                variable: "t".to_string(),
                property: "source".to_string(),
                value: Value::String("slack".to_string()),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "t".to_string(),
            label: "Thread".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Thread".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Thread".to_string(), "source".to_string()), 10)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 100,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_null_predicate_uses_conservative_selectivity() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIsNotNull {
            variable: "c".to_string(),
            property: "ai_summary".to_string(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "c".to_string(),
            label: "Community".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Community".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Community".to_string(), "ai_summary".to_string()), 100)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 900,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_not_eq_filter_uses_distinct_counts() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyNotEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::String("default".to_string()),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "space_id".to_string()), 10)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 900,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_relationship_id_in_uses_literal_list_width() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::IdIn {
            variable: "r".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2)],
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
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
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
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 2,
            cost: 10_004,
        }
    );
}

#[test]
fn residual_relationship_property_in_uses_relationship_distinct_counts() {
    let logical = LogicalPlan::Filter {
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
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1_200,
            cost: 10_004,
        }
    );
}

#[test]
fn aggregate_group_keys_use_node_property_distinct_counts() {
    let logical = LogicalPlan::Aggregate {
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
            rel_properties: BTreeMap::new(),
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
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 1_000),
                ("Entity".to_string(), 500),
                ("Label".to_string(), 100),
            ],
            [
                ("MENTIONS".to_string(), 2_000),
                ("HAS_LABEL".to_string(), 1_000),
            ],
            [
                ("MENTIONS".to_string(), 1_000),
                ("HAS_LABEL".to_string(), 1_000),
            ],
            [
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    1_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    1_000,
                ),
            ],
            [
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    1_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    1_000,
                ),
            ],
            [
                (("Entity".to_string(), "community_id".to_string()), 4),
                (("Label".to_string(), "name".to_string()), 5),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 20,
            cost: 7_004,
        }
    );
}

#[test]
fn aggregate_group_keys_use_relationship_property_distinct_counts() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "r".to_string(),
                property: "weight".to_string(),
            },
            name: "weight".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "entity_count".to_string(),
        }],
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
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
            [("MENTIONS".to_string(), 1_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                1_000,
            )],
            [],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 10,
            cost: 5_004,
        }
    );
}

#[test]
fn aggregate_distinct_property_targets_add_bounded_work_cost() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property {
                variable: "m".to_string(),
                property: "community_id".to_string(),
            },
            distinct: true,
            name: "community_span".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "unit_type".to_string()), 5),
                (("Memory".to_string(), "community_id".to_string()), 10),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 2_014,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_add_bounded_work_cost() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("m".to_string()),
            distinct: true,
            name: "memory_count".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 3_004,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_use_path_target_coverage() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "entity_count".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
            [("MENTIONS".to_string(), 1_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                1_000,
            )],
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        )
        .with_path_target_distinct_counts([(
            (
                "Memory".to_string(),
                "MENTIONS".to_string(),
                "Entity".to_string(),
            ),
            12,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 4_016,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_use_bounded_path_target_coverage() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "two_hop_entities".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "RELATES_TO".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 2,
            max_hops: 2,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
            [("RELATES_TO".to_string(), 10_000)],
            [("RELATES_TO".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                ),
                2_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                    2,
                ),
                1_500,
            )],
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        )
        .with_bounded_path_target_distinct_counts([(
            (
                "Memory".to_string(),
                "RELATES_TO".to_string(),
                "Entity".to_string(),
                2,
            ),
            40,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 5_044,
        }
    );
}

#[test]
fn optional_relationship_count_sum_uses_seed_and_relationship_statistics() {
    let logical = LogicalPlan::OptionalRelationshipCountSum {
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
    };
    let catalog = OptimizerCatalog::new(
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
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 11,
        }
    );
    assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Thread: seed_rows=1 leg_rows=[CONTAINS:out:5] estimated_rows=1 cost=11",
            )
        }));
}

#[test]
fn incoming_optional_relationship_count_sum_uses_target_statistics() {
    let logical = LogicalPlan::OptionalRelationshipCountSum {
        variable: "e".to_string(),
        label: "Entity".to_string(),
        properties: BTreeMap::from([("id".to_string(), Value::String("entity-42".to_string()))]),
        legs: vec![RelationshipCountLeg {
            rel_type: "MENTIONS".to_string(),
            direction: RelationshipDirection::Incoming,
            distinct: false,
            filter: None,
        }],
        output: "mention_count".to_string(),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 1_000),
            ],
            [("MENTIONS".to_string(), 5_000)],
            [("MENTIONS".to_string(), 5_000)],
            [],
            [],
            [(("Entity".to_string(), "id".to_string()), 1_000)],
            [],
        )
        .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 16,
        }
    );
    assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Entity: seed_rows=1 leg_rows=[MENTIONS:in:10] estimated_rows=1 cost=16",
            )
        }));
}

#[test]
fn incoming_optional_degree_cost_uses_target_statistics() {
    let logical = LogicalPlan::OptionalDegree {
        source_variable: "e".to_string(),
        rel_type: "MENTIONS".to_string(),
        rel_properties: BTreeMap::new(),
        direction: RelationshipDirection::Incoming,
        target_label: "Memory".to_string(),
        target_properties: BTreeMap::new(),
        alias: "mention_count".to_string(),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "e".to_string(),
            label: "Entity".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 1_000),
            ],
            [("MENTIONS".to_string(), 5_000)],
            [("MENTIONS".to_string(), 5_000)],
            [],
            [],
            [],
            [],
        )
        .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1_000,
            cost: 12_004,
        }
    );
}

#[test]
fn expand_trace_marks_fallback_hop_estimates() {
    let logical = LogicalPlan::Project {
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
            min_hops: 1,
            max_hops: 3,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10), ("Entity".to_string(), 100)],
            [("LINKS".to_string(), 20)],
            [("LINKS".to_string(), 10)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                4,
            )],
            [],
            [],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("hop_rows=[1:fallback:4,2:fallback:8,3:fallback:16]")
            && decision.contains("estimated_rows=28")
    }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 28,
            cost: 80,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=28 cost=80"));
}

#[test]
fn expand_cost_scales_with_selective_input_rows() {
    let logical = LogicalPlan::Project {
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
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(7),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
            [("LINKS".to_string(), 1000)],
            [("LINKS".to_string(), 1000)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                1000,
            )],
            [],
            [(("Memory".to_string(), "id".to_string()), 1000)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 6,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek for Memory.id")));
    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_equality_index_seek:")
    }));
    assert!(trace.rule_events.iter().any(|event| {
        event.rule() == "implementation:node_equality_index_seek"
            && event.outcome() == RuleOutcome::Applied
    }));
    assert!(trace.stage_events.iter().any(|event| {
        event.name() == "access_path_selection" && event.stats().applied_rules == 1
    }));
    assert!(trace
        .stage_events
        .iter()
        .any(|event| { event.name() == "physical_search" && event.stats().applied_rules >= 1 }));
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=1 cost=6"));
}

#[test]
fn expand_cost_uses_relationship_property_distinct_counts() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "r".to_string(),
                property: "weight".to_string(),
            },
            name: "weight".to_string(),
        }],
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
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
            [("MENTIONS".to_string(), 1000)],
            [("MENTIONS".to_string(), 1000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1000,
            )],
            [],
            [(("Memory".to_string(), "id".to_string()), 1000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("rel_property_distinct_product=10")
            && decision.contains("estimated_rows=100")
    }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 6,
        }
    );
}
