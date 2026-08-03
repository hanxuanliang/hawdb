use serde_json::json;
use skein::schema::{Catalog, RelTypeId};
use skein::store::{GraphStore, NodeId};
use skein_qos::ProcessMemorySnapshot;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

const SOURCE_COUNT: usize = 513;
const TARGET_COUNT: usize = 64;
const RETAINED_MUTATIONS: usize = 64;
const DENSE_TARGET_COUNT: usize = 8_192;
const DENSE_RETAINED_MUTATIONS: usize = 64;
const LOOKUP_ITERATIONS: usize = 20_000;
const LOOKUP_SAMPLES: usize = 7;

fn main() {
    let (base, mut catalog, sources, targets) = fixture();
    let rel_type = catalog
        .rel_type_id("LINKS_TO")
        .expect("fixture relationship type must exist");

    let _ = measure_lookups(&base, &sources, rel_type, LOOKUP_ITERATIONS / 10);
    let mut lookup_samples = (0..LOOKUP_SAMPLES)
        .map(|_| measure_lookups(&base, &sources, rel_type, LOOKUP_ITERATIONS))
        .collect::<Vec<_>>();
    lookup_samples.sort_unstable_by_key(|sample| sample.elapsed_ns);
    let lookup = lookup_samples[lookup_samples.len() / 2];
    let (dense_base, mut dense_catalog, dense_sources, dense_targets) = dense_fixture();

    let memory_start = ProcessMemorySnapshot::capture().ok();
    let (retained, mutation_elapsed_ns) =
        measure_retained_mutations(&base, &mut catalog, &sources, &targets, RETAINED_MUTATIONS);
    let (dense_retained, dense_mutation_elapsed_ns) = measure_retained_mutations(
        &dense_base,
        &mut dense_catalog,
        &dense_sources,
        &dense_targets,
        DENSE_RETAINED_MUTATIONS,
    );
    black_box((&retained, &dense_retained));
    let memory_end = ProcessMemorySnapshot::capture().ok();

    let resident_delta_bytes = memory_start
        .zip(memory_end)
        .map(|(start, end)| end.resident_bytes.saturating_sub(start.resident_bytes));
    let report = json!({
        "source_count": SOURCE_COUNT,
        "target_count": TARGET_COUNT,
        "base_relationship_count": SOURCE_COUNT * TARGET_COUNT,
        "retained_mutations": RETAINED_MUTATIONS,
        "mutation_elapsed_ns": mutation_elapsed_ns,
        "mutation_ns_per_op": mutation_elapsed_ns / RETAINED_MUTATIONS as u128,
        "dense_target_count": DENSE_TARGET_COUNT,
        "dense_base_relationship_count": DENSE_TARGET_COUNT,
        "dense_retained_mutations": DENSE_RETAINED_MUTATIONS,
        "dense_mutation_elapsed_ns": dense_mutation_elapsed_ns,
        "dense_mutation_ns_per_op": dense_mutation_elapsed_ns / DENSE_RETAINED_MUTATIONS as u128,
        "resident_delta_bytes": resident_delta_bytes,
        "lookup_iterations": LOOKUP_ITERATIONS,
        "lookup_samples": LOOKUP_SAMPLES,
        "lookup_rows": lookup.rows,
        "lookup_elapsed_ns_p50": lookup.elapsed_ns,
        "lookup_ns_per_op_p50": lookup.elapsed_ns / LOOKUP_ITERATIONS as u128,
    });
    println!("store_cow_feasibility {report}");
}

fn measure_retained_mutations(
    base: &GraphStore,
    catalog: &mut Catalog,
    sources: &[NodeId],
    targets: &[NodeId],
    retained_mutations: usize,
) -> (Vec<(GraphStore, GraphStore)>, u128) {
    let start = Instant::now();
    let mut retained = Vec::with_capacity(retained_mutations);
    for iteration in 0..retained_mutations {
        let mut working = base.snapshot();
        let reader = working.snapshot();
        working
            .create_relationship(
                catalog,
                sources[iteration % sources.len()],
                targets[iteration % targets.len()],
                "LINKS_TO",
                BTreeMap::new(),
            )
            .expect("snapshot mutation must succeed");
        retained.push((working, reader));
    }
    (retained, start.elapsed().as_nanos())
}

#[derive(Clone, Copy)]
struct LookupSample {
    rows: usize,
    elapsed_ns: u128,
}

fn measure_lookups(
    store: &GraphStore,
    sources: &[NodeId],
    rel_type: RelTypeId,
    iterations: usize,
) -> LookupSample {
    let start = Instant::now();
    let mut rows = 0usize;
    for iteration in 0..iterations {
        let source = sources[iteration % sources.len()];
        rows = rows.saturating_add(
            black_box(store)
                .outgoing_relationships(source, rel_type)
                .count(),
        );
    }
    LookupSample {
        rows,
        elapsed_ns: start.elapsed().as_nanos(),
    }
}

fn fixture() -> (GraphStore, Catalog, Vec<NodeId>, Vec<NodeId>) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let sources = (0..SOURCE_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Source", BTreeMap::new())
                .expect("fixture source must be created")
        })
        .collect::<Vec<_>>();
    let targets = (0..TARGET_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .expect("fixture target must be created")
        })
        .collect::<Vec<_>>();
    for source in &sources {
        for target in &targets {
            store
                .create_relationship(&mut catalog, *source, *target, "LINKS_TO", BTreeMap::new())
                .expect("fixture relationship must be created");
        }
    }
    (store, catalog, sources, targets)
}

fn dense_fixture() -> (GraphStore, Catalog, Vec<NodeId>, Vec<NodeId>) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .expect("dense fixture source must be created");
    let targets = (0..DENSE_TARGET_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .expect("dense fixture target must be created")
        })
        .collect::<Vec<_>>();
    for target in &targets {
        store
            .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
            .expect("dense fixture relationship must be created");
    }
    (store, catalog, vec![source], targets)
}
