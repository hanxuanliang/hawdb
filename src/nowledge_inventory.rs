use crate::api::{BackgroundMaintenanceSummary, Database};
use crate::compat::{
    assess_compatibility_cypher_migration_gate_bundle, assess_query_inventory_cypher_coverage,
    build_compatibility_query_inventory, compatibility_inventory_coverage_report_to_json,
    compatibility_migration_gate_bundle_to_json, nowledge_memory_core_fixture,
    run_compatibility_fixture_with_shadow, CompatibilityCutoverPolicy,
    CompatibilityInventoryCoveragePolicy, CompatibilityQueryCallSite, CompatibilityQueryInventory,
    CompatibilityShadowEngine, ExternalShadowReady,
};
use crate::error::{Result, SkeinError};
use crate::qos::{LocalQosPolicy, LocalQosState};
use crate::search::SearchIndex;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_INVENTORY_NAME: &str = "nowledge-scanned-inventory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeInventoryScanOptions {
    pub inventory_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeCypherMigrationGateJsonOptions {
    pub shadow_name: Option<String>,
    pub self_shadow: bool,
    pub shadow_ready: Option<ExternalShadowReady>,
    pub ready_preflight: bool,
    pub shadow_trace_path: Option<String>,
    pub shadow_request_count: Option<u64>,
    pub include_cutover_evidence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RustStringLiteral {
    value: String,
    line: usize,
}

impl Default for NowledgeInventoryScanOptions {
    fn default() -> Self {
        Self {
            inventory_name: DEFAULT_INVENTORY_NAME.to_string(),
        }
    }
}

pub fn scan_nowledge_query_inventory(
    root: impl AsRef<Path>,
) -> Result<CompatibilityQueryInventory> {
    scan_nowledge_query_inventory_with_options(root, NowledgeInventoryScanOptions::default())
}

pub fn scan_nowledge_query_inventory_with_options(
    root: impl AsRef<Path>,
    options: NowledgeInventoryScanOptions,
) -> Result<CompatibilityQueryInventory> {
    let root = root.as_ref();
    let mut files = Vec::new();
    collect_rust_files(root, &mut files)?;
    files.sort();

    let mut call_sites = Vec::new();
    for file in files {
        let content = fs::read_to_string(&file).map_err(|error| {
            SkeinError::Execution(format!("failed to read '{}': {error}", file.display()))
        })?;
        let relative = file.strip_prefix(root).unwrap_or(&file);
        let source_file = path_to_slash_string(relative);
        if !scan_source_file(&source_file) {
            continue;
        }
        let production_content = strip_cfg_test_modules(&content);
        for literal in extract_rust_string_literals(&production_content)? {
            let Some(cypher) = normalize_cypher_literal(&literal.value) else {
                continue;
            };
            let query_family = classify_query_family(&cypher);
            let name = format!(
                "{}:{}:{}",
                source_file,
                literal.line,
                stable_query_slug(&cypher)
            );
            call_sites.push(
                CompatibilityQueryCallSite::new(
                    name,
                    query_family,
                    format!("{source_file}:{}", literal.line),
                )
                .with_cypher(cypher),
            );
        }
    }

    build_compatibility_query_inventory(options.inventory_name, call_sites)
}

fn scan_source_file(source_file: &str) -> bool {
    if source_file.starts_with("crates/nmem-content/") {
        return false;
    }
    let parts = source_file.split('/').collect::<Vec<_>>();
    if parts
        .iter()
        .any(|part| *part == "tests" || *part == "benches")
    {
        return false;
    }
    if source_file.contains("/src/bin/") {
        return false;
    }
    true
}

pub fn scan_nowledge_query_inventory_to_json(root: impl AsRef<Path>) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    Ok(crate::compat::compatibility_query_inventory_to_json(
        &inventory,
    ))
}

pub fn scan_nowledge_query_inventory_cypher_coverage_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    Ok(compatibility_inventory_coverage_report_to_json(&coverage))
}

pub fn scan_nowledge_query_inventory_cypher_coverage_detail_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    let fixture_cypher_keys = fixture
        .checks
        .iter()
        .filter_map(fixture_check_cypher)
        .map(cypher_coverage_key)
        .collect::<std::collections::BTreeSet<_>>();
    let missing_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_none_or(|key| !fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();
    let covered_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_some_and(|key| fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();

    Ok(serde_json::json!({
        "coverage": compatibility_inventory_coverage_report_to_json(&coverage),
        "covered_items": covered_items,
        "missing_items": missing_items,
    }))
}

pub fn scan_nowledge_query_inventory_cypher_migration_gate_to_json(
    root: impl AsRef<Path>,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<serde_json::Value> {
    scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
        root,
        shadow,
        NowledgeCypherMigrationGateJsonOptions::default(),
    )
}

pub fn scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
    root: impl AsRef<Path>,
    shadow: &mut impl CompatibilityShadowEngine,
    options: NowledgeCypherMigrationGateJsonOptions,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let shadow_engine_name = shadow.name().to_string();
    let mut primary = Database::new();
    let shadow_report = run_compatibility_fixture_with_shadow(&mut primary, &fixture, shadow)?;
    let bundle = assess_compatibility_cypher_migration_gate_bundle(
        &fixture,
        &inventory,
        &shadow_report,
        CompatibilityInventoryCoveragePolicy::default(),
        CompatibilityCutoverPolicy::default(),
    );
    let mut json = compatibility_migration_gate_bundle_to_json(&bundle);
    insert_background_maintenance_summary_json(&mut json, &primary)?;
    add_shadow_metadata_to_migration_gate_json(&mut json, &shadow_engine_name, options)?;
    Ok(json)
}

fn add_shadow_metadata_to_migration_gate_json(
    bundle: &mut serde_json::Value,
    fallback_shadow_name: &str,
    options: NowledgeCypherMigrationGateJsonOptions,
) -> Result<()> {
    let shadow_name = options
        .shadow_name
        .as_deref()
        .unwrap_or(fallback_shadow_name);
    let ready_preflight = options.ready_preflight || options.shadow_ready.is_some();

    if options.shadow_name.is_some() || options.include_cutover_evidence {
        insert_shadow_run_json(bundle, shadow_name, options.self_shadow)?;
    }
    if let Some(ready) = options.shadow_ready.as_ref() {
        insert_shadow_ready_json(bundle, ready)?;
    }
    if let Some(trace_path) = options.shadow_trace_path.as_ref() {
        insert_shadow_trace_json(
            bundle,
            trace_path,
            options.shadow_request_count.unwrap_or_default(),
        )?;
    }
    if options.include_cutover_evidence {
        insert_cutover_evidence_json(bundle, options.self_shadow, ready_preflight)?;
    }
    Ok(())
}

fn migration_gate_json_object(
    bundle: &mut serde_json::Value,
) -> Result<&mut serde_json::Map<String, serde_json::Value>> {
    bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })
}

fn insert_shadow_run_json(
    bundle: &mut serde_json::Value,
    shadow_name: &str,
    self_shadow: bool,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_run".to_string(),
        serde_json::json!({
            "shadow_name": shadow_name,
            "self_shadow": self_shadow,
            "evidence_kind": if self_shadow {
                "protocol_smoke"
            } else {
                "previous_wrapper"
            },
        }),
    );
    Ok(())
}

fn insert_shadow_ready_json(
    bundle: &mut serde_json::Value,
    ready: &ExternalShadowReady,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_ready".to_string(),
        serde_json::json!({
            "protocol_version": ready.protocol_version,
            "capabilities": &ready.capabilities,
            "engine_kind": &ready.engine_kind,
        }),
    );
    Ok(())
}

fn insert_shadow_trace_json(
    bundle: &mut serde_json::Value,
    trace_path: &str,
    request_count: u64,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_trace".to_string(),
        serde_json::json!({
            "path": trace_path,
            "request_count": request_count,
        }),
    );
    Ok(())
}

fn insert_cutover_evidence_json(
    bundle: &mut serde_json::Value,
    self_shadow: bool,
    ready_preflight: bool,
) -> Result<()> {
    let migration_gate = bundle
        .get("migration_gate")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            SkeinError::Execution("migration gate bundle missing migration_gate".to_string())
        })?;
    let migration_gate_ready = migration_gate
        .get("decision")
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let shadow_evidence_present = migration_gate
        .get("shadow_evidence_present")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut blockers = Vec::new();
    if self_shadow {
        blockers.push("shadow run is protocol smoke, not previous-wrapper evidence");
    }
    if !ready_preflight {
        blockers.push("shadow ready preflight was not executed");
    }
    if !shadow_evidence_present {
        blockers.push("no matched shadow checks are present");
    }
    if !migration_gate_ready {
        blockers.push("migration gate decision is not ready");
    }

    migration_gate_json_object(bundle)?.insert(
        "cutover_evidence".to_string(),
        serde_json::json!({
            "eligible": blockers.is_empty(),
            "evidence_kind": if self_shadow {
                "protocol_smoke"
            } else {
                "previous_wrapper"
            },
            "requires_previous_wrapper": true,
            "requires_ready_preflight": true,
            "requires_shadow_evidence": true,
            "ready_preflight": ready_preflight,
            "shadow_evidence_present": shadow_evidence_present,
            "migration_gate_ready": migration_gate_ready,
            "blockers": blockers,
        }),
    );
    Ok(())
}

fn insert_background_maintenance_summary_json(
    bundle: &mut serde_json::Value,
    database: &Database,
) -> Result<()> {
    let search_index = SearchIndex::in_memory();
    let summary = database.background_maintenance_summary(
        Some(&search_index),
        &LocalQosPolicy::default(),
        &LocalQosState::default(),
        Default::default(),
    );
    migration_gate_json_object(bundle)?.insert(
        "background_maintenance".to_string(),
        background_maintenance_summary_to_json(&summary),
    );
    Ok(())
}

fn background_maintenance_summary_to_json(
    summary: &BackgroundMaintenanceSummary,
) -> serde_json::Value {
    serde_json::json!({
        "total_candidates": summary.total_candidates,
        "admitted_count": summary.admitted_count,
        "deferred_count": summary.deferred_count,
        "rejected_count": summary.rejected_count,
        "total_estimated_operations": summary.total_estimated_operations,
        "admitted_estimated_operations": summary.admitted_estimated_operations,
        "deferred_estimated_operations": summary.deferred_estimated_operations,
        "rejected_estimated_operations": summary.rejected_estimated_operations,
        "top_admitted_kind": summary.top_admitted_kind.map(|kind| kind.as_str()),
        "top_admitted_name": summary.top_admitted_name.as_deref(),
        "ranked": summary
            .ranked
            .iter()
            .map(|item| {
                serde_json::json!({
                    "kind": item.kind.as_str(),
                    "name": &item.name,
                    "work_class": &item.work_class_name,
                    "priority": &item.priority_name,
                    "estimated_operations": item.estimated_operations,
                    "admission": &item.admission_name,
                    "admission_code": &item.admission_code_name,
                    "score": item.score,
                    "reason_codes": &item.reason_code_names,
                    "reasons": &item.reasons,
                    "has_executable_search_projection_graph_delta": item.has_executable_search_projection_graph_delta,
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn fixture_check_cypher(check: &crate::compat::CompatibilityCheck) -> Option<&str> {
    match check {
        crate::compat::CompatibilityCheck::Cypher(check) => Some(check.statement.cypher.as_str()),
        crate::compat::CompatibilityCheck::ProjectedGraph(_) => None,
    }
}

fn inventory_item_detail_to_json(
    item: &crate::compat::CompatibilityQueryInventoryItem,
) -> serde_json::Value {
    serde_json::json!({
        "name": item.name,
        "query_family": item.query_family,
        "source": item.source,
        "cypher": item.cypher,
    })
}

fn cypher_coverage_key(cypher: &str) -> String {
    cypher.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn collect_rust_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = fs::metadata(root).map_err(|error| {
        SkeinError::Execution(format!("failed to stat '{}': {error}", root.display()))
    })?;
    if metadata.is_file() {
        if root.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            output.push(root.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(root).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read directory '{}': {error}",
            root.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            SkeinError::Execution(format!(
                "failed to read directory entry under '{}': {error}",
                root.display()
            ))
        })?;
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if file_name == "target" || file_name == ".git" {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            SkeinError::Execution(format!("failed to stat '{}': {error}", path.display()))
        })?;
        if metadata.is_dir() {
            collect_rust_files(&path, output)?;
        } else if metadata.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("rs")
        {
            output.push(path);
        }
    }
    Ok(())
}

fn extract_rust_string_literals(content: &str) -> Result<Vec<RustStringLiteral>> {
    let bytes = content.as_bytes();
    let mut literals = Vec::new();
    let mut index = 0;
    let mut line = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => {
                line += 1;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'\n' {
                        line += 1;
                    }
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b'\'' if looks_like_char_literal(bytes, index) => {
                let (next, newlines) = skip_char_literal(content, index)?;
                line += newlines;
                index = next;
            }
            b'\'' => {
                index += 1;
            }
            b'b' if bytes.get(index + 1) == Some(&b'"') => {
                let start_line = line;
                let (value, next, newlines) = parse_cooked_string(content, index + 1)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'b' if bytes.get(index + 1) == Some(&b'r')
                && raw_string_start(bytes, index + 1).is_some() =>
            {
                let start_line = line;
                let (value, next, newlines) = parse_raw_string(content, index + 1)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'"' => {
                let start_line = line;
                let (value, next, newlines) = parse_cooked_string(content, index)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'r' if raw_string_start(bytes, index).is_some() => {
                let start_line = line;
                let (value, next, newlines) = parse_raw_string(content, index)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            _ => {
                index += 1;
            }
        }
    }
    Ok(literals)
}

fn strip_cfg_test_modules(content: &str) -> String {
    let mut output = content.as_bytes().to_vec();
    let mut search_start = 0;
    while let Some(relative_start) = content[search_start..].find("#[cfg(test)]") {
        let attribute_start = search_start + relative_start;
        let after_attribute = attribute_start + "#[cfg(test)]".len();
        let Some(module_start) = cfg_test_module_start(content, after_attribute) else {
            search_start = after_attribute;
            continue;
        };
        let Some(module_end) = find_matching_rust_brace(content, module_start) else {
            break;
        };
        for byte in &mut output[attribute_start..=module_end] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
        search_start = module_end + 1;
    }
    String::from_utf8(output).expect("ASCII masking preserves valid UTF-8")
}

fn cfg_test_module_start(content: &str, after_attribute: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut index = skip_ascii_whitespace(bytes, after_attribute);
    if !bytes.get(index..)?.starts_with(b"mod") {
        return None;
    }
    index += b"mod".len();
    if !bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        return None;
    }
    index = skip_ascii_whitespace(bytes, index);
    let ident_start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    if index == ident_start {
        return None;
    }
    index = skip_ascii_whitespace(bytes, index);
    if bytes.get(index) == Some(&b'{') {
        Some(index)
    } else {
        None
    }
}

fn skip_ascii_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    index
}

fn find_matching_rust_brace(content: &str, open_brace: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut index = open_brace;
    let mut depth = 0_usize;
    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b'\'' if looks_like_char_literal(bytes, index) => {
                let (next, _) = skip_char_literal(content, index).ok()?;
                index = next;
            }
            b'\'' => {
                index += 1;
            }
            b'b' if bytes.get(index + 1) == Some(&b'"') => {
                let (_, next, _) = parse_cooked_string(content, index + 1).ok()?;
                index = next;
            }
            b'b' if bytes.get(index + 1) == Some(&b'r')
                && raw_string_start(bytes, index + 1).is_some() =>
            {
                let (_, next, _) = parse_raw_string(content, index + 1).ok()?;
                index = next;
            }
            b'"' => {
                let (_, next, _) = parse_cooked_string(content, index).ok()?;
                index = next;
            }
            b'r' if raw_string_start(bytes, index).is_some() => {
                let (_, next, _) = parse_raw_string(content, index).ok()?;
                index = next;
            }
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    None
}

fn skip_char_literal(content: &str, start: usize) -> Result<(usize, usize)> {
    let bytes = content.as_bytes();
    let mut index = start + 1;
    let mut newlines = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => return Ok((index + 1, newlines)),
            b'\\' => {
                index = (index + 2).min(bytes.len());
            }
            b'\n' => {
                newlines += 1;
                index += 1;
            }
            _ => index += 1,
        }
    }
    Err(SkeinError::Semantic(format!(
        "unterminated Rust char literal at byte {start}"
    )))
}

fn looks_like_char_literal(bytes: &[u8], start: usize) -> bool {
    let Some(next) = bytes.get(start + 1) else {
        return false;
    };
    if *next == b'\\' {
        return bytes[start + 2..].iter().take(8).any(|byte| *byte == b'\'');
    }
    bytes.get(start + 2) == Some(&b'\'')
}

fn parse_cooked_string(content: &str, start: usize) -> Result<(String, usize, usize)> {
    let bytes = content.as_bytes();
    let mut index = start + 1;
    let mut value = String::new();
    let mut newlines = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Ok((value, index + 1, newlines)),
            b'\\' => {
                index += 1;
                if index >= bytes.len() {
                    break;
                }
                match bytes[index] {
                    b'n' => value.push('\n'),
                    b'r' => value.push('\r'),
                    b't' => value.push('\t'),
                    b'\\' => value.push('\\'),
                    b'"' => value.push('"'),
                    b'\n' => newlines += 1,
                    other => value.push(other as char),
                }
                index += 1;
            }
            b'\n' => {
                value.push('\n');
                newlines += 1;
                index += 1;
            }
            byte => {
                value.push(byte as char);
                index += 1;
            }
        }
    }
    Err(SkeinError::Semantic(format!(
        "unterminated Rust string literal at byte {start}"
    )))
}

fn parse_raw_string(content: &str, start: usize) -> Result<(String, usize, usize)> {
    let bytes = content.as_bytes();
    let hashes = raw_string_start(bytes, start).expect("caller checked raw string start");
    let body_start = start + 2 + hashes;
    let terminator = format!("\"{}", "#".repeat(hashes));
    let rest = &content[body_start..];
    let Some(offset) = rest.find(&terminator) else {
        return Err(SkeinError::Semantic(format!(
            "unterminated Rust raw string literal at byte {start}"
        )));
    };
    let value = rest[..offset].to_string();
    let newlines = value.bytes().filter(|byte| *byte == b'\n').count();
    Ok((value, body_start + offset + terminator.len(), newlines))
}

fn raw_string_start(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'r') {
        return None;
    }
    let mut index = start + 1;
    let mut hashes = 0;
    while bytes.get(index) == Some(&b'#') {
        hashes += 1;
        index += 1;
    }
    if bytes.get(index) == Some(&b'"') {
        Some(hashes)
    } else {
        None
    }
}

fn normalize_cypher_literal(value: &str) -> Option<String> {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .trim()
        .to_string();
    if normalized.is_empty() || !looks_like_cypher(&normalized) {
        return None;
    }
    if looks_like_incomplete_match_fragment(&normalized) {
        return None;
    }
    if contains_unresolved_rust_format_placeholder(&normalized) {
        return None;
    }
    Some(normalized)
}

fn looks_like_incomplete_match_fragment(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    if !upper.starts_with("MATCH ") {
        return false;
    }
    ![
        " RETURN ",
        " WITH ",
        " SET ",
        " CREATE ",
        " MERGE ",
        " DELETE ",
        " DETACH DELETE ",
        " CALL ",
    ]
    .iter()
    .any(|marker| upper.contains(marker))
}

fn contains_unresolved_rust_format_placeholder(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut index = 0;
    let mut in_single_quote = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_single_quote && bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                } else {
                    in_single_quote = !in_single_quote;
                    index += 1;
                }
            }
            b'{' if bytes.get(index + 1) == Some(&b'{') => {
                index += 2;
            }
            b'}' if bytes.get(index + 1) == Some(&b'}') => {
                index += 2;
            }
            b'{' => {
                let content_start = index + 1;
                let Some(close_offset) = query[content_start..].find('}') else {
                    return true;
                };
                let content = query[content_start..content_start + close_offset].trim();
                if in_single_quote && content.is_empty() {
                    index = content_start + close_offset + 1;
                    continue;
                }
                if looks_like_rust_format_placeholder(content) {
                    return true;
                }
                index = content_start + close_offset + 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    false
}

fn looks_like_rust_format_placeholder(content: &str) -> bool {
    if content.is_empty() {
        return true;
    }
    let (head, format_spec) = content
        .split_once(':')
        .map(|(head, spec)| (head.trim(), Some(spec.trim_start())))
        .unwrap_or((content, None));
    let mut chars = head.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return false;
    }
    let Some(format_spec) = format_spec else {
        return true;
    };
    format_spec
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, '?' | '#' | '<' | '>' | '^' | '0' | '.' | '1'..='9'))
}

fn looks_like_cypher(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let starts_like_cypher = upper.starts_with("MATCH ")
        || upper.starts_with("MERGE (")
        || upper.starts_with("CREATE (")
        || upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE RELATIONSHIP ")
        || graph_index_ddl(&upper)
        || graph_procedure_call(&upper)
        || is_transaction_control(query);
    if !starts_like_cypher {
        return false;
    }
    upper.contains('(') || graph_procedure_call(&upper) || is_transaction_control(query)
}

fn classify_query_family(query: &str) -> &'static str {
    let upper = query.to_ascii_uppercase();
    if graph_procedure_call(&upper) {
        "procedure"
    } else if upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE RELATIONSHIP ")
        || graph_index_ddl(&upper)
    {
        "schema"
    } else if is_transaction_control(query) {
        "transaction_control"
    } else if upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || upper.contains(" CREATE ")
        || upper.contains(" MERGE ")
        || upper.contains(" SET ")
        || upper.contains(" DELETE ")
        || upper.contains(" DETACH DELETE ")
    {
        "mutation"
    } else {
        "read"
    }
}

fn is_transaction_control(query: &str) -> bool {
    matches!(
        query,
        "BEGIN TRANSACTION" | "COMMIT" | "ROLLBACK" | "CHECKPOINT"
    )
}

fn graph_index_ddl(upper: &str) -> bool {
    (upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE RANGE INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX "))
        && upper.contains(" ON :")
}

fn graph_procedure_call(upper: &str) -> bool {
    matches!(
        procedure_name(upper).as_deref(),
        Some("PROJECT_GRAPH" | "PAGE_RANK" | "PAGERANK" | "LOUVAIN")
    )
}

fn procedure_name(upper: &str) -> Option<String> {
    let rest = upper.strip_prefix("CALL ")?;
    let name = rest
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn stable_query_slug(query: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in query.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::{
        classify_query_family, extract_rust_string_literals, normalize_cypher_literal,
        scan_nowledge_query_inventory,
        scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
        scan_nowledge_query_inventory_cypher_coverage_to_json,
        scan_nowledge_query_inventory_cypher_migration_gate_to_json,
        scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json, scan_source_file,
        strip_cfg_test_modules, NowledgeCypherMigrationGateJsonOptions,
    };
    use crate::compat::{
        CompatibilityShadowEngine, ExternalShadowReady, ProjectedGraphFixtureCheck,
        ProjectedGraphShadowOutput,
    };
    use crate::{Database, QueryOutput, Result};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn extracts_cooked_and_raw_rust_cypher_literals() {
        let source = r##"
            let read = "MATCH (m:Memory {id: $id})\nRETURN m.id";
            let ignored = "https://example.test/query";
            let raw = r#"MATCH (j:AugmentationJob)
                         WHERE j.status = 'pending'
                         SET j.status = 'failed'"#;
        "##;

        let queries = extract_rust_string_literals(source)
            .unwrap()
            .into_iter()
            .filter_map(|literal| normalize_cypher_literal(&literal.value))
            .collect::<Vec<_>>();

        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "MATCH (m:Memory {id: $id}) RETURN m.id");
        assert_eq!(
            queries[1],
            "MATCH (j:AugmentationJob) WHERE j.status = 'pending' SET j.status = 'failed'"
        );
    }

    #[test]
    fn classifies_scanned_query_families() {
        assert_eq!(
            classify_query_family("MATCH (m:Memory) RETURN m.id"),
            "read"
        );
        assert_eq!(
            classify_query_family("MATCH (m:Memory) SET m.seen = true"),
            "mutation"
        );
        assert_eq!(
            classify_query_family("CREATE RANGE INDEX ON :Memory(created_at)"),
            "schema"
        );
        assert_eq!(
            classify_query_family("MATCH (a:Memory), (b:Memory) CREATE (a)-[:R]->(b)"),
            "mutation"
        );
        assert_eq!(classify_query_family("CALL page_rank('g')"), "procedure");
    }

    #[test]
    fn rejects_non_cypher_text_that_starts_with_create() {
        assert_eq!(
            normalize_cypher_literal("Create a crystal (knowledge synthesis)"),
            None
        );
    }

    #[test]
    fn rejects_sql_and_prompt_text_from_inventory() {
        assert_eq!(
            normalize_cypher_literal(
                "CREATE TABLE IF NOT EXISTS content_documents (id TEXT PRIMARY KEY)"
            ),
            None
        );
        assert_eq!(normalize_cypher_literal("BEGIN IMMEDIATE"), None);
        assert_eq!(normalize_cypher_literal("Call me Wey"), None);
        assert_eq!(
            normalize_cypher_literal("CALL knowledge_search('graph')"),
            None
        );
    }

    #[test]
    fn accepts_graph_inventory_literals() {
        assert_eq!(
            normalize_cypher_literal("CREATE INDEX ON :Memory(id)"),
            Some("CREATE INDEX ON :Memory(id)".to_string())
        );
        assert_eq!(
            normalize_cypher_literal("CALL PROJECT_GRAPH('UnifiedGraph', ['Entity'], ['LINKS'])"),
            Some("CALL PROJECT_GRAPH('UnifiedGraph', ['Entity'], ['LINKS'])".to_string())
        );
        assert_eq!(
            normalize_cypher_literal("BEGIN TRANSACTION"),
            Some("BEGIN TRANSACTION".to_string())
        );
        assert_eq!(
            normalize_cypher_literal("CHECKPOINT;"),
            Some("CHECKPOINT".to_string())
        );
        assert_eq!(normalize_cypher_literal("checkpoint"), None);
    }

    #[test]
    fn strips_cfg_test_modules_before_scanning_literals() {
        let source = r#"
            pub fn before() -> &'static str {
                "MATCH (m:Memory) RETURN m.id"
            }

            #[cfg(test)]
            mod tests {
                #[test]
                fn ignored() {
                    let query = "MATCH (t:TestOnly {shape: '{not a brace}'}) RETURN t.id";
                    assert_eq!(query.len(), 1);
                }
            }

            pub fn after() -> &'static str {
                "MATCH (e:Entity) RETURN e.id"
            }
        "#;

        let stripped = strip_cfg_test_modules(source);
        let queries = extract_rust_string_literals(&stripped)
            .unwrap()
            .into_iter()
            .filter_map(|literal| normalize_cypher_literal(&literal.value))
            .collect::<Vec<_>>();

        assert_eq!(
            queries,
            vec![
                "MATCH (m:Memory) RETURN m.id".to_string(),
                "MATCH (e:Entity) RETURN e.id".to_string()
            ]
        );
    }

    #[test]
    fn accepts_cypher_maps_but_skips_rust_format_templates() {
        assert_eq!(
            normalize_cypher_literal("MATCH (m:Memory {id: $id}) RETURN m.id"),
            Some("MATCH (m:Memory {id: $id}) RETURN m.id".to_string())
        );
        assert_eq!(
            normalize_cypher_literal("MATCH (m:Memory) WHERE m.id = $id{space_clause} RETURN m.id"),
            None
        );
        assert_eq!(
            normalize_cypher_literal(
                "MATCH p = (a)-[e* ALL SHORTEST 1..{max_depth}]-(b) RETURN length(p)"
            ),
            None
        );
        assert_eq!(
            normalize_cypher_literal(
                "CREATE (j:AugmentationJob {result: '{}', error_message: ''})"
            ),
            Some("CREATE (j:AugmentationJob {result: '{}', error_message: ''})".to_string())
        );
        assert_eq!(
            normalize_cypher_literal(
                "CALL PROJECT_GRAPH('{name}', {'Entity': ''}, {'RELATES_TO': ''})"
            ),
            None
        );
    }

    #[test]
    fn skips_incomplete_match_fragments_used_for_formatting() {
        assert_eq!(
            normalize_cypher_literal("MATCH (e:Entity {name: $name, entity_type: $entity_type})"),
            None
        );
        assert_eq!(
            normalize_cypher_literal(
                "MATCH (e:Entity) WHERE e.entity_type = $entity_type AND LOWER(e.name) = LOWER($name)"
            ),
            None
        );
        assert_eq!(
            normalize_cypher_literal("MATCH (e:Entity {id: $id}) RETURN e.id"),
            Some("MATCH (e:Entity {id: $id}) RETURN e.id".to_string())
        );
        assert_eq!(
            normalize_cypher_literal("MATCH (e:Entity {id: $id}) SET e.name = $name"),
            Some("MATCH (e:Entity {id: $id}) SET e.name = $name".to_string())
        );
    }

    #[test]
    fn skips_non_production_graph_sources() {
        assert!(!scan_source_file("crates/nmem-content/src/lib.rs"));
        assert!(!scan_source_file("crates/nmem-server/tests/okf_smoke.rs"));
        assert!(!scan_source_file(
            "crates/nmem-graph/src/bin/community_smoke.rs"
        ));
        assert!(scan_source_file("crates/nmem-graph/src/community.rs"));
        assert!(scan_source_file("crates/nmem-server/src/rest_fs.rs"));
    }

    #[test]
    fn scanned_cypher_coverage_reports_fixture_matches() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-inventory-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn query() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }
            "#,
        )
        .unwrap();

        let coverage = scan_nowledge_query_inventory_cypher_coverage_to_json(&root).unwrap();

        assert_eq!(coverage["fixture"], "nowledge-memory-core");
        assert_eq!(coverage["required_checks"], 1);
        assert_eq!(coverage["covered_checks"], 1);
        assert_eq!(coverage["missing_checks"].as_array().unwrap().len(), 0);
        assert_eq!(
            coverage["extra_fixture_checks"].as_array().unwrap().len(),
            641
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_reports_ready_bundle() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn query() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }
            "#,
        )
        .unwrap();

        let mut shadow = TestShadowEngine::default();
        let bundle =
            scan_nowledge_query_inventory_cypher_migration_gate_to_json(&root, &mut shadow)
                .unwrap();

        assert_eq!(bundle["coverage"]["required_checks"], 1);
        assert_eq!(bundle["coverage"]["covered_checks"], 1);
        assert_eq!(bundle["inventory_gate"]["decision"], "ready");
        assert_eq!(bundle["cutover"]["decision"], "ready");
        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["migration_gate"]["shadow_decision"], "ready");
        assert_eq!(
            bundle["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap(),
            bundle["background_maintenance"]["ranked"]
                .as_array()
                .unwrap()
                .len() as u64
        );
        assert!(
            bundle["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(bundle["background_maintenance"]["ranked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["work_class"] == "projection"
                && item["priority"] == "background"
                && item["admission"] == "admit"));
        assert_eq!(
            bundle["migration_gate"]["blockers"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(bundle.get("shadow_run").is_none());
        assert!(bundle.get("cutover_evidence").is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_can_include_shadow_wiring_metadata() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-metadata-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn query() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }
            "#,
        )
        .unwrap();

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_name: Some("previous-wrapper".to_string()),
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec!["execute".to_string(), "project_graph".to_string()],
                    engine_kind: Some("previous_wrapper".to_string()),
                }),
                shadow_trace_path: Some("/tmp/skein-shadow.jsonl".to_string()),
                shadow_request_count: Some(42),
                include_cutover_evidence: true,
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["shadow_run"]["shadow_name"], "previous-wrapper");
        assert_eq!(bundle["shadow_run"]["self_shadow"], false);
        assert_eq!(bundle["shadow_run"]["evidence_kind"], "previous_wrapper");
        assert_eq!(
            bundle["shadow_ready"]["protocol_version"],
            crate::EXTERNAL_SHADOW_PROTOCOL_VERSION
        );
        assert_eq!(bundle["shadow_trace"]["request_count"], 42);
        assert_eq!(bundle["cutover_evidence"]["eligible"], true);
        assert_eq!(bundle["cutover_evidence"]["ready_preflight"], true);
        assert_eq!(bundle["cutover_evidence"]["shadow_evidence_present"], true);
        assert!(
            bundle["background_maintenance"]["admitted_count"]
                .as_u64()
                .unwrap()
                > 0
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_coverage_detail_reports_missing_item_metadata() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-inventory-detail-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn covered() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }

                pub fn missing() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
                }
            "#,
        )
        .unwrap();

        let detail = scan_nowledge_query_inventory_cypher_coverage_detail_to_json(&root).unwrap();
        let missing_items = detail["missing_items"].as_array().unwrap();
        let covered_items = detail["covered_items"].as_array().unwrap();

        assert_eq!(detail["coverage"]["required_checks"], 2);
        assert_eq!(detail["coverage"]["covered_checks"], 1);
        assert_eq!(covered_items.len(), 1);
        assert_eq!(missing_items.len(), 1);
        assert_eq!(
            missing_items[0]["cypher"],
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
        );
        assert_eq!(missing_items[0]["query_family"], "read");
        assert_eq!(
            missing_items[0]["source"],
            "crates/nmem-graph/src/repo.rs:7"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[derive(Default)]
    struct TestShadowEngine {
        db: Database,
    }

    impl CompatibilityShadowEngine for TestShadowEngine {
        fn name(&self) -> &str {
            "test-shadow"
        }

        fn execute(
            &mut self,
            statement: &crate::compat::CypherFixtureStatement,
        ) -> Result<QueryOutput> {
            self.db
                .query_with_params(&statement.cypher, &statement.parameters)
        }

        fn execute_session(
            &mut self,
            statements: &[crate::compat::CypherFixtureStatement],
        ) -> Result<Vec<QueryOutput>> {
            let mut session = self.db.session();
            statements
                .iter()
                .map(|statement| {
                    session.query_with_params(&statement.cypher, &statement.parameters)
                })
                .collect()
        }

        fn project_graph(
            &mut self,
            check: &ProjectedGraphFixtureCheck,
        ) -> Result<Option<ProjectedGraphShadowOutput>> {
            let graph = self.db.project_graph(check.rel_type.as_deref());
            let page_rank_scores = graph
                .page_rank(Default::default())
                .into_iter()
                .map(|score| (score.node.0, score.score))
                .collect::<Vec<_>>();
            Ok(Some(ProjectedGraphShadowOutput {
                node_count: graph.node_count(),
                edge_count: graph.edge_count(),
                incoming: check
                    .expected_incoming
                    .iter()
                    .map(|(node, _)| {
                        let sources = graph
                            .incoming_sources(crate::store::NodeId(*node))
                            .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                            .unwrap_or_default();
                        (*node, sources)
                    })
                    .collect(),
                communities: if check.expected_communities.is_empty() {
                    Vec::new()
                } else {
                    graph
                        .louvain_communities(Default::default())
                        .into_iter()
                        .map(|assignment| (assignment.node.0, assignment.community.0))
                        .collect()
                },
                hierarchical_communities: if check.expected_hierarchical_communities.is_empty() {
                    Vec::new()
                } else {
                    graph
                        .hierarchical_louvain_communities(Default::default())
                        .into_iter()
                        .map(|assignment| {
                            (assignment.level, assignment.node.0, assignment.community.0)
                        })
                        .collect()
                },
                page_rank_top_node: page_rank_scores.first().map(|(node, _)| *node),
                page_rank_scores,
            }))
        }
    }

    #[test]
    fn scanned_inventory_skips_cfg_test_module_literals() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-inventory-cfg-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("client.rs"),
            r#"
                pub fn production() -> &'static str {
                    "MATCH (m:Memory) RETURN m.id"
                }

                #[cfg(test)]
                mod tests {
                    #[test]
                    fn ignored() {
                        let query = "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id));";
                    }
                }
            "#,
        )
        .unwrap();

        let inventory = scan_nowledge_query_inventory(&root).unwrap();

        assert_eq!(inventory.required_checks.len(), 1);
        assert_eq!(
            inventory.required_checks[0].cypher.as_deref(),
            Some("MATCH (m:Memory) RETURN m.id")
        );

        fs::remove_dir_all(root).unwrap();
    }
}
