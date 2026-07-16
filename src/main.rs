use skein::{
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_to_json,
    scan_nowledge_query_inventory_to_json, Database, ExternalShadowCommand, Result, SkeinError,
};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(command) = args.next() {
        if command == "scan-nowledge-inventory" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "scan-nowledge-cypher-coverage" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_cypher_coverage_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "scan-nowledge-cypher-coverage-detail" {
            let root = args.next().unwrap_or_else(|| ".".to_string());
            let json = scan_nowledge_query_inventory_cypher_coverage_detail_to_json(root)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        if command == "nowledge-cypher-migration-gate" {
            let root = args.next().ok_or_else(|| {
                SkeinError::Semantic(
                    "nowledge-cypher-migration-gate requires <root> <shadow-name> <program> [args...]"
                        .to_string(),
                )
            })?;
            let shadow_name = args.next().ok_or_else(|| {
                SkeinError::Semantic(
                    "nowledge-cypher-migration-gate requires <root> <shadow-name> <program> [args...]"
                        .to_string(),
                )
            })?;
            let program = args.next().ok_or_else(|| {
                SkeinError::Semantic(
                    "nowledge-cypher-migration-gate requires <root> <shadow-name> <program> [args...]"
                        .to_string(),
                )
            })?;
            let program_args = args.collect::<Vec<_>>();
            let mut shadow = ExternalShadowCommand::spawn(shadow_name, program, program_args)?;
            let json =
                scan_nowledge_query_inventory_cypher_migration_gate_to_json(root, &mut shadow)?;
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            return Ok(());
        }
        return Err(SkeinError::Semantic(format!("unknown command '{command}'")));
    }

    let path = std::env::temp_dir().join("skein-demo");
    let _ = std::fs::remove_dir_all(&path);
    let mut db = Database::open(&path)?;
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")?;
    db.query("CREATE (:Memory {id: 2, title: 'Runtime strategy'})")?;
    db.checkpoint()?;

    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";
    let explain = db.explain_query(query)?;
    println!("{}", explain.trace.selected_plan);

    drop(db);
    let mut db = Database::open(&path)?;
    let output = db.query(query)?;
    for row in output.rows {
        println!("{row:?}");
    }

    Ok(())
}
