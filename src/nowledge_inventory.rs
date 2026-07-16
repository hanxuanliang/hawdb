use crate::compat::{
    assess_query_inventory_cypher_coverage, build_compatibility_query_inventory,
    compatibility_inventory_coverage_report_to_json, nowledge_memory_core_fixture,
    CompatibilityQueryCallSite, CompatibilityQueryInventory,
};
use crate::error::{Result, SkeinError};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_INVENTORY_NAME: &str = "nowledge-scanned-inventory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeInventoryScanOptions {
    pub inventory_name: String,
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
        scan_nowledge_query_inventory_cypher_coverage_to_json, scan_source_file,
        strip_cfg_test_modules,
    };
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
            318
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
