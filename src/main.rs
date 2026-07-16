use skein::{
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_to_json,
    scan_nowledge_query_inventory_to_json, CanonicalGraphSnapshotValidation,
    CanonicalSnapshotIdentityAudit, Database, DatabaseConfig, ExternalShadowCommand,
    ExternalShadowReady, GraphLightningBootstrapManifest, Result, SkeinError, Value,
};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).peekable();
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
            let mut require_ready = false;
            let mut allow_self_shadow = false;
            let mut shadow_ready = false;
            let mut shadow_trace = None;
            let mut shadow_timeout = None;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    "--allow-self-shadow" => {
                        allow_self_shadow = true;
                        args.next();
                    }
                    "--shadow-ready" => {
                        shadow_ready = true;
                        args.next();
                    }
                    "--shadow-trace" => {
                        args.next();
                        shadow_trace = Some(args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?);
                    }
                    "--shadow-timeout-ms" => {
                        args.next();
                        let raw_timeout = args.next().ok_or_else(|| {
                            SkeinError::Semantic(nowledge_cypher_migration_gate_usage())
                        })?;
                        shadow_timeout = Some(parse_shadow_timeout_ms(&raw_timeout)?);
                    }
                    _ => break,
                }
            }
            let root = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let shadow_name = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let program = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(nowledge_cypher_migration_gate_usage()))?;
            let program_args = args.collect::<Vec<_>>();
            if require_ready
                && !allow_self_shadow
                && is_self_shadow_command(&shadow_name, &program, &program_args)
            {
                return Err(SkeinError::Execution(
                    "nowledge migration gate requires a previous-wrapper shadow for --require-ready; pass --allow-self-shadow only for protocol smoke tests"
                    .to_string(),
                ));
            }
            let shadow_trace_report = shadow_trace.clone();
            let mut shadow = match (shadow_trace, shadow_timeout) {
                (Some(trace_path), Some(timeout)) => {
                    ExternalShadowCommand::spawn_with_trace_path_and_request_timeout(
                        shadow_name,
                        program,
                        program_args,
                        trace_path,
                        timeout,
                    )?
                }
                (Some(trace_path), None) => ExternalShadowCommand::spawn_with_trace_path(
                    shadow_name,
                    program,
                    program_args,
                    trace_path,
                )?,
                (None, Some(timeout)) => ExternalShadowCommand::spawn_with_request_timeout(
                    shadow_name,
                    program,
                    program_args,
                    timeout,
                )?,
                (None, None) => ExternalShadowCommand::spawn(shadow_name, program, program_args)?,
            };
            let shadow_ready_report = if should_run_shadow_ready(require_ready, shadow_ready) {
                Some(shadow.require_ready()?)
            } else {
                None
            };
            let mut json =
                scan_nowledge_query_inventory_cypher_migration_gate_to_json(root, &mut shadow)?;
            if let Some(ready) = shadow_ready_report {
                add_shadow_ready_report(&mut json, &ready)?;
            }
            if let Some(trace_path) = shadow_trace_report {
                add_shadow_trace_report(&mut json, &trace_path, shadow.request_count())?;
            }
            let rendered = serde_json::to_string_pretty(&json).unwrap();
            println!("{rendered}");
            if require_ready
                && json
                    .get("migration_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "nowledge migration gate is blocked".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "validate-canonical-snapshot" {
            let mut require_valid = false;
            let mut require_import_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-valid" => {
                        require_valid = true;
                        args.next();
                    }
                    "--require-import-ready" => {
                        require_import_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(validate_canonical_snapshot_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(validate_canonical_snapshot_usage()));
            }
            let db = Database::open_with_config(
                path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )?;
            let snapshot = db.export_canonical_graph_snapshot();
            let validation = snapshot.validate();
            let rendered = canonical_snapshot_validation_json(
                snapshot.graph_commit_epoch,
                snapshot.logical_checksum,
                snapshot.nodes.len(),
                snapshot.relationships.len(),
                &validation,
            );
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_valid && !validation.is_valid {
                return Err(SkeinError::Execution(
                    "canonical snapshot validation failed".to_string(),
                ));
            }
            if require_import_ready && !validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "canonical snapshot import readiness failed".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-bootstrap-manifest" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_bootstrap_manifest_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_bootstrap_manifest_usage(),
                ));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let rendered = graph_lightning_bootstrap_manifest_json(&export.manifest);
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_ready && !export.manifest.validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "graph lightning bootstrap manifest is not import ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-bootstrap-bundle" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_bootstrap_bundle_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(
                    graph_lightning_bootstrap_bundle_usage(),
                ));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let rendered = graph_lightning_bootstrap_bundle_json(&export);
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_ready
                && rendered
                    .get("export_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning bootstrap bundle is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-stage-bootstrap" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let database_path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()))?;
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_stage_bootstrap_usage()));
            }
            let mut db = Database::open(database_path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let catalog = stage_graph_lightning_bootstrap_export(&export, staging_dir)?;
            println!("{}", serde_json::to_string_pretty(&catalog).unwrap());
            if require_ready
                && catalog
                    .get("export_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning staged bootstrap is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-verify-staging" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let staging_dir = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_staging_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_verify_staging_usage()));
            }
            let report = verify_graph_lightning_staging_catalog(staging_dir)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if require_ready
                && report
                    .get("validation_gate")
                    .and_then(|gate| gate.get("decision"))
                    .and_then(serde_json::Value::as_str)
                    != Some("ready")
            {
                return Err(SkeinError::Execution(
                    "graph lightning staging verification is not ready".to_string(),
                ));
            }
            return Ok(());
        }
        if command == "graph-lightning-graph-stream" {
            let mut require_ready = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-ready" => {
                        require_ready = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_graph_stream_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_graph_stream_usage()));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            if require_ready && !export.manifest.validation.is_import_ready {
                return Err(SkeinError::Execution(
                    "graph lightning graph stream is not import ready".to_string(),
                ));
            }
            print!("{}", export.graph_stream.encoded);
            return Ok(());
        }
        if command == "graph-lightning-verify-export" {
            let mut require_valid = false;
            while let Some(flag) = args.peek() {
                match flag.as_str() {
                    "--require-valid" => {
                        require_valid = true;
                        args.next();
                    }
                    _ => break,
                }
            }
            let path = args
                .next()
                .ok_or_else(|| SkeinError::Semantic(graph_lightning_verify_export_usage()))?;
            if args.next().is_some() {
                return Err(SkeinError::Semantic(graph_lightning_verify_export_usage()));
            }
            let mut db = Database::open(path)?;
            let export = db.prepare_graph_lightning_bootstrap_export()?;
            let validation = export
                .graph_stream
                .validate_against_manifest(&export.manifest);
            let rendered = graph_lightning_graph_stream_validation_json(&validation);
            println!("{}", serde_json::to_string_pretty(&rendered).unwrap());
            if require_valid && !validation.is_valid {
                return Err(SkeinError::Execution(
                    "graph lightning graph stream validation failed".to_string(),
                ));
            }
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

fn nowledge_cypher_migration_gate_usage() -> String {
    "nowledge-cypher-migration-gate requires [--require-ready] [--allow-self-shadow] [--shadow-ready] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] <root> <shadow-name> <program> [args...]"
        .to_string()
}

fn validate_canonical_snapshot_usage() -> String {
    "validate-canonical-snapshot requires [--require-valid] [--require-import-ready] <database-path>"
        .to_string()
}

fn graph_lightning_bootstrap_manifest_usage() -> String {
    "graph-lightning-bootstrap-manifest requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_bootstrap_bundle_usage() -> String {
    "graph-lightning-bootstrap-bundle requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_stage_bootstrap_usage() -> String {
    "graph-lightning-stage-bootstrap requires [--require-ready] <database-path> <staging-dir>"
        .to_string()
}

fn graph_lightning_verify_staging_usage() -> String {
    "graph-lightning-verify-staging requires [--require-ready] <staging-dir>".to_string()
}

fn graph_lightning_graph_stream_usage() -> String {
    "graph-lightning-graph-stream requires [--require-ready] <database-path>".to_string()
}

fn graph_lightning_verify_export_usage() -> String {
    "graph-lightning-verify-export requires [--require-valid] <database-path>".to_string()
}

fn parse_shadow_timeout_ms(raw_timeout: &str) -> Result<Duration> {
    let timeout_ms = raw_timeout.parse::<u64>().map_err(|error| {
        SkeinError::Semantic(format!(
            "invalid --shadow-timeout-ms '{raw_timeout}': {error}"
        ))
    })?;
    if timeout_ms == 0 {
        return Err(SkeinError::Semantic(
            "--shadow-timeout-ms must be greater than zero".to_string(),
        ));
    }
    Ok(Duration::from_millis(timeout_ms))
}

fn add_shadow_ready_report(
    bundle: &mut serde_json::Value,
    ready: &ExternalShadowReady,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_ready".to_string(),
        serde_json::json!({
            "protocol_version": ready.protocol_version,
            "capabilities": &ready.capabilities,
        }),
    );
    Ok(())
}

fn add_shadow_trace_report(
    bundle: &mut serde_json::Value,
    trace_path: &str,
    request_count: u64,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_trace".to_string(),
        serde_json::json!({
            "path": trace_path,
            "request_count": request_count,
        }),
    );
    Ok(())
}

fn should_run_shadow_ready(require_ready: bool, shadow_ready: bool) -> bool {
    require_ready || shadow_ready
}

fn is_self_shadow_command(shadow_name: &str, program: &str, program_args: &[String]) -> bool {
    shadow_name == "self"
        || program.ends_with("skein-shadow-self")
        || program_args.iter().any(|arg| arg == "skein-shadow-self")
}

fn canonical_snapshot_validation_json(
    graph_commit_epoch: u64,
    logical_checksum: u64,
    node_count: usize,
    relationship_count: usize,
    validation: &CanonicalGraphSnapshotValidation,
) -> serde_json::Value {
    serde_json::json!({
        "graph_commit_epoch": graph_commit_epoch,
        "logical_checksum": logical_checksum,
        "node_count": node_count,
        "relationship_count": relationship_count,
        "validation": {
            "is_valid": validation.is_valid,
            "is_import_ready": validation.is_import_ready,
            "checksum_matches": validation.checksum_matches,
            "expected_logical_checksum": validation.expected_logical_checksum,
            "stable_identity_matches": validation.stable_identity_matches,
            "stable_identity_ready": validation.stable_identity_ready,
            "expected_stable_identity": stable_identity_audit_json(&validation.expected_stable_identity),
            "duplicate_node_ids": validation.duplicate_node_ids,
            "duplicate_relationship_ids": validation.duplicate_relationship_ids,
            "missing_sources": endpoint_violations_json(&validation.missing_sources),
            "missing_targets": endpoint_violations_json(&validation.missing_targets),
        }
    })
}

fn graph_lightning_bootstrap_manifest_json(
    manifest: &GraphLightningBootstrapManifest,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": "graph-lightning-bootstrap",
        "protocol_version": manifest.protocol_version,
        "graph_commit_epoch": manifest.graph_commit_epoch,
        "logical_checksum": manifest.logical_checksum,
        "graph_stream_checksum": manifest.graph_stream_checksum,
        "graph_stream_byte_len": manifest.graph_stream_byte_len,
        "schema_checksum": manifest.schema_checksum,
        "node_count": manifest.node_count,
        "relationship_count": manifest.relationship_count,
        "label_count": manifest.label_count,
        "relationship_type_count": manifest.relationship_type_count,
        "node_property_count": manifest.node_property_count,
        "relationship_property_count": manifest.relationship_property_count,
        "validation": {
            "is_valid": manifest.validation.is_valid,
            "is_import_ready": manifest.validation.is_import_ready,
            "checksum_matches": manifest.validation.checksum_matches,
            "expected_logical_checksum": manifest.validation.expected_logical_checksum,
            "stable_identity_matches": manifest.validation.stable_identity_matches,
            "stable_identity_ready": manifest.validation.stable_identity_ready,
            "expected_stable_identity": stable_identity_audit_json(&manifest.validation.expected_stable_identity),
            "duplicate_node_ids": manifest.validation.duplicate_node_ids,
            "duplicate_relationship_ids": manifest.validation.duplicate_relationship_ids,
            "missing_sources": endpoint_violations_json(&manifest.validation.missing_sources),
            "missing_targets": endpoint_violations_json(&manifest.validation.missing_targets),
        }
    })
}

fn graph_lightning_bootstrap_bundle_json(
    export: &skein::GraphLightningBootstrapExport,
) -> serde_json::Value {
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let mut blockers = Vec::new();
    if !export.manifest.validation.is_import_ready {
        blockers.push("manifest validation is not import ready");
    }
    if !graph_stream_validation.is_valid {
        blockers.push("graph stream validation failed");
    }
    let decision = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    serde_json::json!({
        "protocol": "graph-lightning-bootstrap-bundle",
        "manifest": graph_lightning_bootstrap_manifest_json(&export.manifest),
        "graph_stream_validation": graph_lightning_graph_stream_validation_json(&graph_stream_validation),
        "export_gate": {
            "decision": decision,
            "blockers": blockers,
        },
    })
}

fn stage_graph_lightning_bootstrap_export(
    export: &skein::GraphLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    fs::create_dir_all(staging_dir)?;
    let bundle = graph_lightning_bootstrap_bundle_json(export);
    let manifest = graph_lightning_bootstrap_manifest_json(&export.manifest);
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let stage_state = if bundle
        .get("export_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        == Some("ready")
    {
        "READY"
    } else {
        "QUARANTINED"
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    let graph_stream_bytes = export.graph_stream.encoded.as_bytes();
    let bundle_bytes = serde_json::to_vec_pretty(&bundle).unwrap();
    let manifest_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_bootstrap_manifest.json",
        &manifest_bytes,
    )?;
    let graph_stream_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_graph_stream.txt",
        graph_stream_bytes,
    )?;
    let bundle_artifact = write_staging_artifact(
        staging_dir,
        "graph_lightning_bootstrap_bundle.json",
        &bundle_bytes,
    )?;
    let catalog = serde_json::json!({
        "protocol": "graph-lightning-staging-catalog",
        "protocol_version": 1,
        "stage_state": stage_state,
        "graph_commit_epoch": export.manifest.graph_commit_epoch,
        "logical_checksum": export.manifest.logical_checksum,
        "schema_checksum": export.manifest.schema_checksum,
        "export_gate": bundle["export_gate"].clone(),
        "artifacts": [
            manifest_artifact,
            graph_stream_artifact,
            bundle_artifact,
        ],
        "graph_stream_validation": graph_lightning_graph_stream_validation_json(&graph_stream_validation),
    });
    let catalog_bytes = serde_json::to_vec_pretty(&catalog).unwrap();
    write_staging_artifact(
        staging_dir,
        "graph_lightning_staging_catalog.json",
        &catalog_bytes,
    )?;
    sync_directory(staging_dir)?;
    Ok(catalog)
}

fn verify_graph_lightning_staging_catalog(
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    let catalog_path = staging_dir.join("graph_lightning_staging_catalog.json");
    let catalog = read_json_file(&catalog_path)?;
    let mut errors = Vec::new();
    let mut artifact_reports = Vec::new();
    let mut manifest = None;
    let mut graph_stream = None;
    let mut bundle = None;

    let artifacts = catalog
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            errors.push("staging catalog missing artifacts array".to_string());
            Vec::new()
        });
    for artifact in &artifacts {
        let kind = artifact
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let Some(path) = artifact.get("path").and_then(serde_json::Value::as_str) else {
            errors.push(format!("staging artifact {kind} missing path"));
            continue;
        };
        if path.contains('/') || path.contains('\\') {
            errors.push(format!(
                "staging artifact {kind} uses non-local path {path}"
            ));
            continue;
        }
        let artifact_path = staging_dir.join(path);
        let expected_byte_len = artifact.get("byte_len").and_then(serde_json::Value::as_u64);
        let expected_checksum = artifact.get("checksum").and_then(serde_json::Value::as_u64);
        match fs::read(&artifact_path) {
            Ok(bytes) => {
                let actual_byte_len = bytes.len() as u64;
                let actual_checksum = checksum_bytes(&bytes);
                let byte_len_matches = expected_byte_len == Some(actual_byte_len);
                let checksum_matches = expected_checksum == Some(actual_checksum);
                if !byte_len_matches {
                    errors.push(format!("staging artifact {kind} byte length mismatch"));
                }
                if !checksum_matches {
                    errors.push(format!("staging artifact {kind} checksum mismatch"));
                }
                match kind {
                    "manifest" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => manifest = Some(value),
                        Err(error) => {
                            errors.push(format!("invalid manifest artifact JSON: {error}"))
                        }
                    },
                    "graph_stream" => match String::from_utf8(bytes.clone()) {
                        Ok(value) => graph_stream = Some(value),
                        Err(error) => errors.push(format!("invalid GraphStream UTF-8: {error}")),
                    },
                    "bundle" => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => bundle = Some(value),
                        Err(error) => errors.push(format!("invalid bundle artifact JSON: {error}")),
                    },
                    _ => errors.push(format!("unknown staging artifact kind {kind}")),
                }
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": actual_byte_len,
                    "byte_len_matches": byte_len_matches,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": actual_checksum,
                    "checksum_matches": checksum_matches,
                }));
            }
            Err(error) => {
                errors.push(format!(
                    "missing staging artifact {kind} at {path}: {error}"
                ));
                artifact_reports.push(serde_json::json!({
                    "kind": kind,
                    "path": path,
                    "expected_byte_len": expected_byte_len,
                    "actual_byte_len": serde_json::Value::Null,
                    "byte_len_matches": false,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": serde_json::Value::Null,
                    "checksum_matches": false,
                }));
            }
        }
    }

    let graph_stream_validation = graph_stream
        .as_ref()
        .map(|encoded| skein::validate_graph_lightning_graph_stream(encoded, None));
    let graph_stream_validation_json = graph_stream_validation
        .as_ref()
        .map(graph_lightning_graph_stream_validation_json);
    let manifest_matches_graph_stream = match (&manifest, &graph_stream, &graph_stream_validation) {
        (Some(manifest), Some(graph_stream), Some(validation)) => {
            let matches = manifest
                .get("graph_stream_checksum")
                .and_then(serde_json::Value::as_u64)
                == validation.expected_stream_checksum
                && manifest
                    .get("graph_stream_byte_len")
                    .and_then(serde_json::Value::as_u64)
                    == Some(graph_stream.len() as u64)
                && manifest
                    .get("graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == validation.graph_commit_epoch
                && manifest
                    .get("logical_checksum")
                    .and_then(serde_json::Value::as_u64)
                    == validation.logical_checksum
                && manifest
                    .get("node_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.node_count as u64)
                && manifest
                    .get("relationship_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(validation.relationship_count as u64);
            if !matches {
                errors.push("manifest does not match GraphStream artifact".to_string());
            }
            matches
        }
        _ => {
            errors.push("manifest or GraphStream artifact missing".to_string());
            false
        }
    };
    let bundle_matches_artifacts = match (&bundle, &manifest, &graph_stream_validation_json) {
        (Some(bundle), Some(manifest), Some(validation)) => {
            let matches = bundle.get("manifest") == Some(manifest)
                && bundle.get("graph_stream_validation") == Some(validation)
                && bundle.get("export_gate") == catalog.get("export_gate");
            if !matches {
                errors.push("bundle does not match staged manifest, GraphStream validation, or catalog gate".to_string());
            }
            matches
        }
        _ => {
            errors.push("bundle artifact missing".to_string());
            false
        }
    };
    let artifact_integrity = artifact_reports.iter().all(|report| {
        report
            .get("byte_len_matches")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && report
                .get("checksum_matches")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    });
    let catalog_state_ready = catalog
        .get("stage_state")
        .and_then(serde_json::Value::as_str)
        == Some("READY")
        && catalog
            .get("export_gate")
            .and_then(|gate| gate.get("decision"))
            .and_then(serde_json::Value::as_str)
            == Some("ready");
    if !catalog_state_ready {
        errors.push("staging catalog is not READY".to_string());
    }
    let graph_stream_valid = graph_stream_validation
        .as_ref()
        .is_some_and(|validation| validation.is_valid);
    if !graph_stream_valid {
        errors.push("GraphStream validation failed".to_string());
    }
    let decision = if errors.is_empty()
        && artifact_integrity
        && manifest_matches_graph_stream
        && bundle_matches_artifacts
        && catalog_state_ready
        && graph_stream_valid
    {
        "ready"
    } else {
        "blocked"
    };
    Ok(serde_json::json!({
        "protocol": "graph-lightning-staging-verification",
        "protocol_version": 1,
        "catalog_path": "graph_lightning_staging_catalog.json",
        "artifact_integrity": artifact_integrity,
        "manifest_matches_graph_stream": manifest_matches_graph_stream,
        "bundle_matches_artifacts": bundle_matches_artifacts,
        "catalog_state_ready": catalog_state_ready,
        "graph_stream_validation": graph_stream_validation_json,
        "artifacts": artifact_reports,
        "validation_gate": {
            "decision": decision,
            "errors": errors,
        },
    }))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        SkeinError::Execution(format!("invalid JSON at {}: {error}", path.display()))
    })
}

fn write_staging_artifact(
    staging_dir: &Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<serde_json::Value> {
    let path = staging_dir.join(file_name);
    let tmp_path = staging_dir.join(format!("{file_name}.tmp"));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &path)?;
    sync_directory(staging_dir)?;
    Ok(serde_json::json!({
        "kind": graph_lightning_artifact_kind(file_name),
        "path": file_name,
        "byte_len": bytes.len(),
        "checksum": checksum_bytes(bytes),
    }))
}

fn graph_lightning_artifact_kind(file_name: &str) -> &'static str {
    match file_name {
        "graph_lightning_bootstrap_manifest.json" => "manifest",
        "graph_lightning_graph_stream.txt" => "graph_stream",
        "graph_lightning_bootstrap_bundle.json" => "bundle",
        "graph_lightning_staging_catalog.json" => "staging_catalog",
        _ => "unknown",
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn graph_lightning_graph_stream_validation_json(
    validation: &skein::GraphLightningGraphStreamValidation,
) -> serde_json::Value {
    serde_json::json!({
        "is_valid": validation.is_valid,
        "checksum_matches": validation.checksum_matches,
        "format_version_matches": validation.format_version_matches,
        "count_matches": validation.count_matches,
        "endpoint_integrity": validation.endpoint_integrity,
        "manifest_matches": validation.manifest_matches,
        "expected_stream_checksum": validation.expected_stream_checksum,
        "actual_stream_checksum": validation.actual_stream_checksum,
        "format_version": validation.format_version,
        "graph_commit_epoch": validation.graph_commit_epoch,
        "logical_checksum": validation.logical_checksum,
        "node_count": validation.node_count,
        "relationship_count": validation.relationship_count,
        "duplicate_node_ids": validation.duplicate_node_ids,
        "duplicate_relationship_ids": validation.duplicate_relationship_ids,
        "missing_sources": endpoint_violations_json(&validation.missing_sources),
        "missing_targets": endpoint_violations_json(&validation.missing_targets),
        "errors": validation.errors,
    })
}

fn stable_identity_audit_json(audit: &CanonicalSnapshotIdentityAudit) -> serde_json::Value {
    serde_json::json!({
        "requires_stable_id_mapping": audit.requires_stable_id_mapping,
        "nodes_without_stable_id": audit.nodes_without_stable_id,
        "relationships_without_stable_id": audit.relationships_without_stable_id,
        "duplicate_node_stable_ids": audit.duplicate_node_stable_ids.iter().map(value_json).collect::<Vec<_>>(),
        "duplicate_relationship_stable_ids": audit.duplicate_relationship_stable_ids.iter().map(value_json).collect::<Vec<_>>(),
    })
}

fn endpoint_violations_json(
    violations: &[skein::CanonicalSnapshotEndpointViolation],
) -> serde_json::Value {
    serde_json::Value::Array(
        violations
            .iter()
            .map(|violation| {
                serde_json::json!({
                    "relationship_id": violation.relationship_id,
                    "missing_node_id": violation.missing_node_id,
                })
            })
            .collect(),
    )
}

fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(value),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(value_json).collect::<Vec<_>>())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_shadow_ready_report, add_shadow_trace_report, canonical_snapshot_validation_json,
        graph_lightning_bootstrap_bundle_json, graph_lightning_bootstrap_bundle_usage,
        graph_lightning_bootstrap_manifest_json, graph_lightning_bootstrap_manifest_usage,
        graph_lightning_graph_stream_usage, graph_lightning_graph_stream_validation_json,
        graph_lightning_stage_bootstrap_usage, graph_lightning_verify_export_usage,
        graph_lightning_verify_staging_usage, is_self_shadow_command, parse_shadow_timeout_ms,
        should_run_shadow_ready, stable_identity_audit_json,
        stage_graph_lightning_bootstrap_export, validate_canonical_snapshot_usage, value_json,
        verify_graph_lightning_staging_catalog,
    };
    use skein::{
        CanonicalGraphSnapshotValidation, CanonicalSnapshotEndpointViolation,
        CanonicalSnapshotIdentityAudit, Database, ExternalShadowReady,
        GraphLightningBootstrapManifest, GraphLightningGraphStreamValidation, Value,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn detects_direct_self_shadow_binary() {
        assert!(is_self_shadow_command(
            "oracle",
            "target/debug/skein-shadow-self",
            &[]
        ));
    }

    #[test]
    fn detects_cargo_run_self_shadow() {
        assert!(is_self_shadow_command(
            "oracle",
            "cargo",
            &[
                "run".to_string(),
                "--quiet".to_string(),
                "--bin".to_string(),
                "skein-shadow-self".to_string(),
                "--".to_string(),
            ],
        ));
    }

    #[test]
    fn detects_self_shadow_name() {
        assert!(is_self_shadow_command(
            "self",
            "/usr/bin/legacy-wrapper",
            &[]
        ));
    }

    #[test]
    fn does_not_reject_named_external_shadow() {
        assert!(!is_self_shadow_command(
            "legacy-wrapper",
            "/usr/bin/nmem-graph-shadow",
            &[]
        ));
    }

    #[test]
    fn parses_shadow_timeout_ms() {
        assert_eq!(
            parse_shadow_timeout_ms("250").unwrap(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn rejects_zero_shadow_timeout_ms() {
        let error = parse_shadow_timeout_ms("0").unwrap_err();

        assert!(error
            .to_string()
            .contains("--shadow-timeout-ms must be greater than zero"));
    }

    #[test]
    fn require_ready_runs_shadow_ready_preflight() {
        assert!(should_run_shadow_ready(true, false));
    }

    #[test]
    fn shadow_ready_runs_preflight_without_requiring_ready_decision() {
        assert!(should_run_shadow_ready(false, true));
    }

    #[test]
    fn skips_shadow_ready_preflight_by_default() {
        assert!(!should_run_shadow_ready(false, false));
    }

    #[test]
    fn adds_shadow_ready_report_to_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
        };

        add_shadow_ready_report(&mut bundle, &ready).unwrap();

        assert_eq!(bundle["shadow_ready"]["protocol_version"], 1);
        assert_eq!(
            bundle["shadow_ready"]["capabilities"],
            serde_json::json!(["execute", "execute_session", "project_graph"])
        );
    }

    #[test]
    fn adds_shadow_trace_report_to_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready"
            }
        });

        add_shadow_trace_report(&mut bundle, "/tmp/skein-shadow.jsonl", 42).unwrap();

        assert_eq!(
            bundle["shadow_trace"]["path"],
            serde_json::json!("/tmp/skein-shadow.jsonl")
        );
        assert_eq!(bundle["shadow_trace"]["request_count"], 42);
    }

    #[test]
    fn renders_canonical_snapshot_validation_json() {
        let validation = CanonicalGraphSnapshotValidation {
            is_valid: false,
            is_import_ready: false,
            checksum_matches: false,
            expected_logical_checksum: 77,
            stable_identity_matches: false,
            stable_identity_ready: false,
            expected_stable_identity: CanonicalSnapshotIdentityAudit {
                requires_stable_id_mapping: true,
                nodes_without_stable_id: vec![1],
                relationships_without_stable_id: vec![2],
                duplicate_node_stable_ids: vec![Value::String("dup-node".to_string())],
                duplicate_relationship_stable_ids: vec![Value::String("dup-rel".to_string())],
            },
            duplicate_node_ids: vec![1],
            duplicate_relationship_ids: vec![2],
            missing_sources: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 2,
                missing_node_id: 10,
            }],
            missing_targets: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 3,
                missing_node_id: 11,
            }],
        };

        let json = canonical_snapshot_validation_json(5, 99, 3, 2, &validation);

        assert_eq!(json["graph_commit_epoch"], 5);
        assert_eq!(json["logical_checksum"], 99);
        assert_eq!(json["node_count"], 3);
        assert_eq!(json["relationship_count"], 2);
        assert_eq!(json["validation"]["is_valid"], false);
        assert_eq!(json["validation"]["is_import_ready"], false);
        assert_eq!(json["validation"]["expected_logical_checksum"], 77);
        assert_eq!(json["validation"]["stable_identity_ready"], false);
        assert_eq!(
            json["validation"]["expected_stable_identity"]["duplicate_node_stable_ids"],
            serde_json::json!(["dup-node"])
        );
        assert_eq!(
            json["validation"]["missing_sources"][0]["missing_node_id"],
            10
        );
        assert_eq!(
            json["validation"]["missing_targets"][0]["relationship_id"],
            3
        );
    }

    #[test]
    fn renders_graph_lightning_bootstrap_manifest_json() {
        let validation = CanonicalGraphSnapshotValidation {
            is_valid: true,
            is_import_ready: true,
            checksum_matches: true,
            expected_logical_checksum: 99,
            stable_identity_matches: true,
            stable_identity_ready: true,
            expected_stable_identity: CanonicalSnapshotIdentityAudit {
                requires_stable_id_mapping: false,
                nodes_without_stable_id: Vec::new(),
                relationships_without_stable_id: Vec::new(),
                duplicate_node_stable_ids: Vec::new(),
                duplicate_relationship_stable_ids: Vec::new(),
            },
            duplicate_node_ids: Vec::new(),
            duplicate_relationship_ids: Vec::new(),
            missing_sources: Vec::new(),
            missing_targets: Vec::new(),
        };
        let manifest = GraphLightningBootstrapManifest {
            protocol_version: 1,
            graph_commit_epoch: 5,
            logical_checksum: 99,
            graph_stream_checksum: 101,
            graph_stream_byte_len: 4096,
            schema_checksum: 77,
            node_count: 3,
            relationship_count: 2,
            label_count: 2,
            relationship_type_count: 1,
            node_property_count: 6,
            relationship_property_count: 2,
            validation,
        };

        let json = graph_lightning_bootstrap_manifest_json(&manifest);

        assert_eq!(json["protocol"], "graph-lightning-bootstrap");
        assert_eq!(json["protocol_version"], 1);
        assert_eq!(json["graph_commit_epoch"], 5);
        assert_eq!(json["logical_checksum"], 99);
        assert_eq!(json["graph_stream_checksum"], 101);
        assert_eq!(json["graph_stream_byte_len"], 4096);
        assert_eq!(json["schema_checksum"], 77);
        assert_eq!(json["node_count"], 3);
        assert_eq!(json["relationship_count"], 2);
        assert_eq!(json["validation"]["is_import_ready"], true);
    }

    #[test]
    fn renders_graph_lightning_graph_stream_validation_json() {
        let validation = GraphLightningGraphStreamValidation {
            is_valid: false,
            checksum_matches: false,
            format_version_matches: true,
            count_matches: true,
            endpoint_integrity: false,
            manifest_matches: false,
            expected_stream_checksum: Some(11),
            actual_stream_checksum: 22,
            format_version: Some(1),
            graph_commit_epoch: Some(5),
            logical_checksum: Some(99),
            node_count: 3,
            relationship_count: 2,
            duplicate_node_ids: vec![7],
            duplicate_relationship_ids: vec![8],
            missing_sources: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 2,
                missing_node_id: 10,
            }],
            missing_targets: vec![CanonicalSnapshotEndpointViolation {
                relationship_id: 3,
                missing_node_id: 11,
            }],
            errors: vec!["graph stream checksum mismatch".to_string()],
        };

        let json = graph_lightning_graph_stream_validation_json(&validation);

        assert_eq!(json["is_valid"], false);
        assert_eq!(json["checksum_matches"], false);
        assert_eq!(json["expected_stream_checksum"], 11);
        assert_eq!(json["actual_stream_checksum"], 22);
        assert_eq!(json["duplicate_node_ids"], serde_json::json!([7]));
        assert_eq!(json["missing_targets"][0]["missing_node_id"], 11);
        assert_eq!(
            json["errors"],
            serde_json::json!(["graph stream checksum mismatch"])
        );
    }

    #[test]
    fn renders_graph_lightning_bootstrap_bundle_json() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();

        let json = graph_lightning_bootstrap_bundle_json(&export);

        assert_eq!(json["protocol"], "graph-lightning-bootstrap-bundle");
        assert_eq!(json["manifest"]["protocol"], "graph-lightning-bootstrap");
        assert_eq!(json["manifest"]["validation"]["is_import_ready"], true);
        assert_eq!(json["graph_stream_validation"]["is_valid"], true);
        assert_eq!(json["export_gate"]["decision"], "ready");
        assert!(json["export_gate"]["blockers"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn stages_graph_lightning_bootstrap_export_artifacts() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_stage_bootstrap");

        let catalog = stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        assert_eq!(catalog["protocol"], "graph-lightning-staging-catalog");
        assert_eq!(catalog["stage_state"], "READY");
        assert_eq!(catalog["export_gate"]["decision"], "ready");
        assert_eq!(catalog["artifacts"].as_array().unwrap().len(), 3);
        assert!(staging_dir
            .join("graph_lightning_bootstrap_manifest.json")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_graph_stream.txt")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_bootstrap_bundle.json")
            .exists());
        assert!(staging_dir
            .join("graph_lightning_staging_catalog.json")
            .exists());
        let persisted_catalog =
            std::fs::read_to_string(staging_dir.join("graph_lightning_staging_catalog.json"))
                .unwrap();
        let persisted_catalog =
            serde_json::from_str::<serde_json::Value>(&persisted_catalog).unwrap();
        assert_eq!(persisted_catalog, catalog);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn verifies_graph_lightning_staging_catalog() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["protocol"], "graph-lightning-staging-verification");
        assert_eq!(report["validation_gate"]["decision"], "ready");
        assert_eq!(report["artifact_integrity"], true);
        assert_eq!(report["manifest_matches_graph_stream"], true);
        assert_eq!(report["bundle_matches_artifacts"], true);

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn staging_verification_reports_tampered_graph_stream() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let staging_dir = unique_main_test_dir("graph_lightning_verify_staging_tampered");
        stage_graph_lightning_bootstrap_export(&export, &staging_dir).unwrap();
        let graph_stream_path = staging_dir.join("graph_lightning_graph_stream.txt");
        let tampered = std::fs::read_to_string(&graph_stream_path)
            .unwrap()
            .replace("relationship\t0\t0\t1", "relationship\t0\t0\t99");
        std::fs::write(&graph_stream_path, tampered).unwrap();

        let report = verify_graph_lightning_staging_catalog(&staging_dir).unwrap();

        assert_eq!(report["validation_gate"]["decision"], "blocked");
        assert_eq!(report["artifact_integrity"], false);
        assert_eq!(report["manifest_matches_graph_stream"], false);
        assert!(report["validation_gate"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("graph_stream checksum mismatch")));

        std::fs::remove_dir_all(staging_dir).unwrap();
    }

    #[test]
    fn renders_stable_identity_audit_values() {
        let audit = CanonicalSnapshotIdentityAudit {
            requires_stable_id_mapping: true,
            nodes_without_stable_id: vec![7],
            relationships_without_stable_id: vec![9],
            duplicate_node_stable_ids: vec![Value::Int(42)],
            duplicate_relationship_stable_ids: vec![Value::Bool(true)],
        };

        let json = stable_identity_audit_json(&audit);

        assert_eq!(json["requires_stable_id_mapping"], true);
        assert_eq!(json["nodes_without_stable_id"], serde_json::json!([7]));
        assert_eq!(json["duplicate_node_stable_ids"], serde_json::json!([42]));
        assert_eq!(
            json["duplicate_relationship_stable_ids"],
            serde_json::json!([true])
        );
    }

    #[test]
    fn renders_nested_values_as_json() {
        let value = Value::Map(
            [(
                "items".to_string(),
                Value::List(vec![
                    Value::Null,
                    Value::Int(1),
                    Value::String("two".to_string()),
                ]),
            )]
            .into_iter()
            .collect(),
        );

        assert_eq!(
            value_json(&value),
            serde_json::json!({
                "items": [null, 1, "two"]
            })
        );
    }

    #[test]
    fn validates_canonical_snapshot_usage_text() {
        assert!(validate_canonical_snapshot_usage().contains("<database-path>"));
        assert!(validate_canonical_snapshot_usage().contains("--require-import-ready"));
    }

    #[test]
    fn validates_graph_lightning_bootstrap_manifest_usage_text() {
        assert!(graph_lightning_bootstrap_manifest_usage().contains("<database-path>"));
        assert!(graph_lightning_bootstrap_manifest_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_bootstrap_bundle_usage_text() {
        assert!(graph_lightning_bootstrap_bundle_usage().contains("<database-path>"));
        assert!(graph_lightning_bootstrap_bundle_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_stage_bootstrap_usage_text() {
        assert!(graph_lightning_stage_bootstrap_usage().contains("<database-path>"));
        assert!(graph_lightning_stage_bootstrap_usage().contains("<staging-dir>"));
        assert!(graph_lightning_stage_bootstrap_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_verify_staging_usage_text() {
        assert!(graph_lightning_verify_staging_usage().contains("<staging-dir>"));
        assert!(graph_lightning_verify_staging_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_graph_stream_usage_text() {
        assert!(graph_lightning_graph_stream_usage().contains("<database-path>"));
        assert!(graph_lightning_graph_stream_usage().contains("--require-ready"));
    }

    #[test]
    fn validates_graph_lightning_verify_export_usage_text() {
        assert!(graph_lightning_verify_export_usage().contains("<database-path>"));
        assert!(graph_lightning_verify_export_usage().contains("--require-valid"));
    }

    fn unique_main_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }
}
