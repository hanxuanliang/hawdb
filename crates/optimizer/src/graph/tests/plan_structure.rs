use super::super::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerCatalogIndexes,
    OptimizerCatalogStatistics, PlanPhaseKind,
};
use crate::{
    Distribution, MemoryBudgetClass, OptimizerConfig, PhysicalPlanClass, PhysicalPlanKind,
    ScanPruningSupport, VectorPrecision,
};
use skein_core::Value;
use skein_plan::{
    LogicalPlan, PhysicalOperatorDomain, PhysicalPlan, PhysicalPlanChildren, PhysicalPlanDomainRef,
    Predicate, Projection, ProjectionExpression, SortDirection, SortItem, SortKey,
};

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
    let PhysicalPlanDomainRef::Access(access) = seek.as_domain() else {
        panic!("seek should expose the access domain wrapper");
    };
    assert!(std::ptr::eq(access.plan(), seek));
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
fn optimizer_roots_preserve_logical_and_physical_phase_boundaries() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "id".to_string(),
            value: Value::Int(1),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let logical_root = LogicalPlanRoot::new(logical);
    assert_eq!(logical_root.phase(), PlanPhaseKind::Logical);

    let optimized_root = logical_root.clone().into_optimized();
    assert_eq!(optimized_root.phase(), PlanPhaseKind::OptimizedLogical);

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
    let physical_root = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_optimized_root_with_catalog(&optimized_root, &catalog);

    assert_eq!(physical_root.phase(), PlanPhaseKind::Physical);
    assert_eq!(physical_root.plan().kind(), PhysicalPlanKind::IndexNodeSeek);
    assert!(physical_root
        .trace()
        .stage_events
        .iter()
        .any(|event| event.name() == "access_path_selection"));
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
