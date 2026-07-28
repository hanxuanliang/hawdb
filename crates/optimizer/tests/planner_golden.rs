use skein_cypher::parse;
use skein_optimizer::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerCatalogIndexes,
    OptimizerCatalogStatistics, OptimizerConfig,
};
use skein_plan::plan;

const QUERY: &str = "MATCH (m:Memory) WHERE m.id = 7 RETURN m.title AS title";
const EXPECTED: &str = include_str!("golden/indexed_memory_lookup.golden");

#[test]
fn indexed_memory_lookup_matches_planner_golden() {
    let statement = parse(QUERY).expect("golden Cypher must parse");
    let logical = plan(&statement).expect("golden Cypher must plan");
    let logical_root = LogicalPlanRoot::new(logical);
    let optimized_root = logical_root.clone().into_optimized();
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

    let actual = render_planner_golden(
        QUERY,
        logical_root.plan(),
        optimized_root.plan(),
        physical_root.plan(),
        physical_root.trace(),
    );
    assert_eq!(actual.trim_end(), EXPECTED.trim_end());
}

fn render_planner_golden(
    query: &str,
    logical: &skein_plan::LogicalPlan,
    optimized_logical: &skein_plan::LogicalPlan,
    physical: &skein_plan::PhysicalPlan,
    trace: &skein_optimizer::OptimizerTrace,
) -> String {
    let stages = trace
        .stage_events
        .iter()
        .map(|stage| {
            let stats = stage.stats();
            format!(
                "{} order={} input={} output={} applied={} skipped={}",
                stage.name(),
                stage.apply_order().as_str(),
                stats.input_count,
                stats.output_count,
                stats.applied_rules,
                stats.skipped_rules
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let cost = trace.selected_plan_cost_breakdown;
    let properties = &trace.selected_plan_properties;

    format!(
        "[cypher]\n{query}\n\n\
         [logical]\n{logical:#?}\n\n\
         [optimized-logical]\n{optimized_logical:#?}\n\n\
         [physical]\n{}\n\n\
         [stage-trace]\n{stages}\n\n\
         [cost]\nestimated_rows={} total={} cpu={} random_io={} sequential_io={} output_rows={}\n\n\
         [properties]\ndistribution={} ordering={:?} covering_fields={:?} scan_pruning={} vector_precision={} memory_budget={}\n",
        physical.explain(0),
        cost.estimated_rows,
        cost.cost,
        cost.cpu,
        cost.random_io,
        cost.sequential_io,
        cost.output_rows,
        properties.distribution.as_str(),
        properties.ordering,
        properties.covering_fields,
        properties.scan_pruning.as_str(),
        properties.vector_precision.as_str(),
        properties.memory_budget.as_str(),
    )
}
