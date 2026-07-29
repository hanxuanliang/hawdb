use super::super::{CascadesOptimizer, OptimizerCatalog};
use crate::OptimizerConfig;
use skein_core::Value;
use skein_plan::{LogicalPlan, PhysicalPlan, Predicate, VectorPhysicalPlan};

fn vector_seed_filter(property: &str) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: property.to_string(),
            value: Value::String("selected".to_string()),
        },
        input: Box::new(LogicalPlan::NodeColumnLookup {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "id".to_string(),
            column: "external_id".to_string(),
            optional: false,
            input: Box::new(LogicalPlan::VectorSeed {
                embedding_parameter: "embedding".to_string(),
                embedding_dimension: 2,
                top_k: 8,
                output_external_id: true,
            }),
        }),
    }
}

fn vector_seed_scan(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. } => vector_seed_scan(input),
        PhysicalPlan::VectorSeedScan { .. } => plan,
        other => panic!("expected vector seed plan, got {}", other.kind().as_str()),
    }
}

fn vector_filter_fields(plan: &VectorPhysicalPlan) -> &[String] {
    match plan {
        VectorPhysicalPlan::Filter { fields } => fields,
        VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | VectorPhysicalPlan::ResidualFilter { input, .. }
        | VectorPhysicalPlan::RawVectorRerank { input, .. }
        | VectorPhysicalPlan::TopK { input, .. } => vector_filter_fields(input),
    }
}

#[test]
fn descriptor_safe_filter_is_pushed_into_vector_seed_scan() {
    let (physical, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(
            &vector_seed_filter("space_id"),
            &OptimizerCatalog::default(),
        );

    let PhysicalPlan::VectorSeedScan {
        metadata_filters,
        vector_plan,
        ..
    } = vector_seed_scan(&physical)
    else {
        unreachable!();
    };
    assert_eq!(
        metadata_filters.get("space_id"),
        Some(&"selected".to_string())
    );
    assert_eq!(vector_filter_fields(vector_plan), &["space_id".to_string()]);
    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("push descriptor-safe vector seed filter m.space_id")
    }));
}

#[test]
fn unsupported_filter_remains_graph_side_only() {
    let physical = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize(&vector_seed_filter("custom_property"));

    assert!(matches!(physical, PhysicalPlan::FilterExec { .. }));
    let PhysicalPlan::VectorSeedScan {
        metadata_filters,
        vector_plan,
        ..
    } = vector_seed_scan(&physical)
    else {
        unreachable!();
    };
    assert!(metadata_filters.is_empty());
    assert!(vector_filter_fields(vector_plan).is_empty());
}
