use crate::compat::{
    build_compatibility_query_inventory, CompatibilityQueryCallSite, CompatibilityQueryInventory,
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
        for literal in extract_rust_string_literals(&content)? {
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

pub fn scan_nowledge_query_inventory_to_json(root: impl AsRef<Path>) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    Ok(crate::compat::compatibility_query_inventory_to_json(
        &inventory,
    ))
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
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() || !looks_like_cypher(&normalized) {
        return None;
    }
    Some(normalized.trim_end_matches(';').trim().to_string())
}

fn looks_like_cypher(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let starts_like_cypher = upper.starts_with("MATCH ")
        || upper.starts_with("MERGE (")
        || upper.starts_with("CREATE (")
        || upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE REL ")
        || upper.starts_with("CREATE TABLE ")
        || upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE RANGE INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX ")
        || upper.starts_with("CALL ")
        || upper.starts_with("BEGIN ")
        || upper == "COMMIT"
        || upper == "ROLLBACK"
        || upper == "CHECKPOINT";
    if !starts_like_cypher {
        return false;
    }
    upper.contains('(')
        || upper.starts_with("CALL ")
        || upper == "COMMIT"
        || upper == "ROLLBACK"
        || upper == "CHECKPOINT"
        || upper.starts_with("BEGIN ")
}

fn classify_query_family(query: &str) -> &'static str {
    let upper = query.to_ascii_uppercase();
    if upper.starts_with("CALL ") {
        "procedure"
    } else if upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE RELATIONSHIP ")
        || upper.starts_with("CREATE REL TABLE ")
        || upper.starts_with("CREATE TABLE ")
        || upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE RANGE INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX ")
    {
        "schema"
    } else if upper == "BEGIN TRANSACTION"
        || upper == "COMMIT"
        || upper == "ROLLBACK"
        || upper == "CHECKPOINT"
    {
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
    use super::{classify_query_family, extract_rust_string_literals, normalize_cypher_literal};

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
}
