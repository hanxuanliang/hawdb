use crate::error::{Result, SkeinError};
use crate::schema::Catalog;
use crate::store::{GraphStore, NodeRecord};
use crate::value::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

const SEARCH_SNAPSHOT_FILE: &str = "search_projection.skein";
pub const FULL_REINDEX_MARKER: &str = ".reindex_needed";
pub const METADATA_REPAIR_MARKER: &str = ".projection_metadata_repair_needed";

#[derive(Debug, Clone, PartialEq)]
pub struct SearchDocument {
    pub id: String,
    pub title: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionKind {
    Memory,
    Message,
    Entity,
    Source,
    SourceChunk,
    Community,
}

impl SearchProjectionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchProjectionKind::Memory => "memory",
            SearchProjectionKind::Message => "message",
            SearchProjectionKind::Entity => "entity",
            SearchProjectionKind::Source => "source",
            SearchProjectionKind::SourceChunk => "source_chunk",
            SearchProjectionKind::Community => "community",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchProjectionRow {
    pub kind: SearchProjectionKind,
    pub external_id: String,
    pub title: String,
    pub body: String,
    pub embedding: Option<Vec<f32>>,
    pub source_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl SearchProjectionRow {
    pub fn into_document(self) -> SearchDocument {
        let kind = self.kind.as_str();
        let mut metadata = self.metadata;
        metadata.insert("kind".to_string(), kind.to_string());
        metadata.insert("external_id".to_string(), self.external_id.clone());
        if let Some(source_id) = self.source_id {
            metadata.insert("source_id".to_string(), source_id);
        }
        SearchDocument {
            id: format!("{kind}:{}", self.external_id),
            title: self.title,
            content: self.body,
            embedding: self.embedding,
            metadata,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Hybrid,
    Vector,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: String,
    pub score: f64,
    pub vector_score: f64,
    pub text_score: f64,
    pub fallback_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchRebuildOptions {
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRebuildSummary {
    pub scanned_nodes: usize,
    pub indexed_documents: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataRepairOptions {
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRepairSummary {
    pub scanned_nodes: usize,
    pub repaired_documents: usize,
    pub missing_documents: usize,
}

#[derive(Debug, Default)]
pub struct SearchIndex {
    documents: BTreeMap<String, SearchDocument>,
    path: Option<PathBuf>,
    embedding_dimension: Option<usize>,
}

impl SearchIndex {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(path.as_ref())?;
        let mut index = Self {
            documents: BTreeMap::new(),
            path: Some(path.as_ref().to_path_buf()),
            embedding_dimension: None,
        };
        index.load_snapshot()?;
        Ok(index)
    }

    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        if let Some(embedding) = &document.embedding {
            self.validate_or_set_dimension(embedding.len())?;
        }
        self.documents.insert(document.id.clone(), document);
        Ok(())
    }

    pub fn upsert_projection_row(&mut self, row: SearchProjectionRow) -> Result<()> {
        self.upsert(row.into_document())
    }

    pub fn delete(&mut self, id: &str) {
        self.documents.remove(id);
    }

    pub fn document(&self, id: &str) -> Option<&SearchDocument> {
        self.documents.get(id)
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn rebuild_from_graph(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        let mut next_documents = BTreeMap::new();
        let mut scanned_nodes = 0;

        for node in store.scan_nodes(None) {
            scanned_nodes += 1;
            let Some(row) = projection_row_from_node(catalog, node) else {
                continue;
            };
            if options
                .max_rows
                .map(|limit| next_documents.len() >= limit)
                .unwrap_or(false)
            {
                self.mark_full_reindex_needed("full rebuild exceeded configured row limit")?;
                return Err(SkeinError::Storage(format!(
                    "full rebuild exceeded configured row limit after {} documents",
                    next_documents.len()
                )));
            }
            let document = row.into_document();
            next_documents.insert(document.id.clone(), document);
        }

        self.documents = next_documents;
        self.embedding_dimension = None;
        self.clear_marker(FULL_REINDEX_MARKER)?;
        self.clear_marker(METADATA_REPAIR_MARKER)?;
        Ok(SearchRebuildSummary {
            scanned_nodes,
            indexed_documents: self.documents.len(),
        })
    }

    pub fn repair_metadata_from_graph(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        let mut repairs = Vec::new();
        let mut scanned_nodes = 0;
        let mut missing_documents = 0;

        for node in store.scan_nodes(None) {
            scanned_nodes += 1;
            let Some(row) = projection_row_from_node(catalog, node) else {
                continue;
            };
            let document = row.into_document();
            if !self.documents.contains_key(&document.id) {
                missing_documents += 1;
                continue;
            }
            if options
                .max_rows
                .map(|limit| repairs.len() >= limit)
                .unwrap_or(false)
            {
                self.mark_metadata_repair_needed("metadata repair exceeded configured row limit")?;
                return Err(SkeinError::Storage(format!(
                    "metadata repair exceeded configured row limit after {} documents",
                    repairs.len()
                )));
            }
            repairs.push((document.id, document.metadata));
        }

        let repaired_documents = repairs.len();
        for (id, metadata) in repairs {
            if let Some(existing) = self.documents.get_mut(&id) {
                existing.metadata = metadata;
            }
        }
        if missing_documents > 0 {
            self.mark_full_reindex_needed("metadata repair found missing projection rows")?;
        }
        self.clear_marker(METADATA_REPAIR_MARKER)?;
        Ok(MetadataRepairSummary {
            scanned_nodes,
            repaired_documents,
            missing_documents,
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot_path = path.join(SEARCH_SNAPSHOT_FILE);
        let mut body = String::new();
        body.push_str("SKEIN_SEARCH_PROJECTION_V1\n");
        if let Some(dimension) = self.embedding_dimension {
            body.push_str(&format!("embedding_dimension\t{dimension}\n"));
        }
        for document in self.documents.values() {
            body.push_str(&format!(
                "doc\t{}\t{}\t{}\t{}\t{}\n",
                encode_string(&document.id),
                encode_string(&document.title),
                encode_string(&document.content),
                encode_embedding(document.embedding.as_deref()),
                encode_metadata(&document.metadata),
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = snapshot_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, snapshot_path)?;
        Ok(())
    }

    pub fn search(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        limit: usize,
    ) -> Vec<SearchHit> {
        let query_terms = tokenize(query_text);
        let mut fallback_reasons = Vec::new();
        let vector_available = match (query_embedding, self.embedding_dimension) {
            (Some(vector), Some(dimension)) if vector.len() == dimension => true,
            (Some(vector), Some(dimension)) => {
                fallback_reasons.push(format!(
                    "query embedding dimension {} does not match index dimension {dimension}",
                    vector.len()
                ));
                false
            }
            (Some(_), None) => {
                fallback_reasons.push("index has no vector rows".to_string());
                false
            }
            (None, _) => false,
        };
        let text_available = !query_terms.is_empty();

        let mut hits = Vec::new();
        for document in self.documents.values() {
            let vector_score = if vector_available && mode != SearchMode::Text {
                query_embedding
                    .zip(document.embedding.as_deref())
                    .and_then(|(query, document)| cosine_similarity(query, document))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let text_score = if text_available && mode != SearchMode::Vector {
                text_score(&query_terms, document)
            } else {
                0.0
            };
            let score = match mode {
                SearchMode::Hybrid => fuse_score(vector_score, text_score, text_available),
                SearchMode::Vector => vector_score,
                SearchMode::Text => text_score,
            };
            if score > 0.0 {
                hits.push(SearchHit {
                    id: document.id.clone(),
                    score,
                    vector_score,
                    text_score,
                    fallback_reasons: fallback_reasons.clone(),
                });
            }
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        hits.truncate(limit);
        hits
    }

    pub fn mark_full_reindex_needed(&self, reason: &str) -> Result<()> {
        self.append_marker(FULL_REINDEX_MARKER, reason)
    }

    pub fn full_reindex_needed(&self) -> bool {
        self.read_marker_lines(FULL_REINDEX_MARKER)
            .map(|lines| !lines.is_empty())
            .unwrap_or(false)
    }

    pub fn mark_metadata_repair_needed(&self, reason: &str) -> Result<()> {
        self.write_marker(METADATA_REPAIR_MARKER, reason)
    }

    pub fn metadata_repair_needed(&self) -> bool {
        self.marker_path(METADATA_REPAIR_MARKER)
            .map(|path| path.exists())
            .unwrap_or(false)
    }

    fn validate_or_set_dimension(&mut self, dimension: usize) -> Result<()> {
        match self.embedding_dimension {
            Some(existing) if existing != dimension => Err(SkeinError::Storage(format!(
                "embedding dimension mismatch: index has {existing}, row has {dimension}"
            ))),
            Some(_) => Ok(()),
            None => {
                self.embedding_dimension = Some(dimension);
                Ok(())
            }
        }
    }

    fn load_snapshot(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot_path = path.join(SEARCH_SNAPSHOT_FILE);
        if !snapshot_path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(snapshot_path)?;
        let (body, checksum) = split_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "search projection checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        for line in body.lines() {
            if line == "SKEIN_SEARCH_PROJECTION_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["embedding_dimension", raw] => {
                    self.embedding_dimension = Some(parse_usize(raw, "embedding dimension")?);
                }
                ["doc", raw_id, raw_title, raw_content, raw_embedding, raw_metadata] => {
                    let embedding = decode_embedding(raw_embedding)?;
                    if let Some(embedding) = &embedding {
                        self.validate_or_set_dimension(embedding.len())?;
                    }
                    let document = SearchDocument {
                        id: decode_string(raw_id)?,
                        title: decode_string(raw_title)?,
                        content: decode_string(raw_content)?,
                        embedding,
                        metadata: decode_metadata(raw_metadata)?,
                    };
                    self.documents.insert(document.id.clone(), document);
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid search projection line: {line}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn marker_path(&self, name: &str) -> Option<PathBuf> {
        self.path.as_ref().map(|path| path.join(name))
    }

    fn append_marker(&self, name: &str, reason: &str) -> Result<()> {
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        let existing = fs::read_to_string(&path).unwrap_or_default();
        if existing.lines().any(|line| line == reason) {
            return Ok(());
        }
        let next = if existing.trim().is_empty() {
            reason.to_string()
        } else {
            format!("{}\n{reason}", existing.trim())
        };
        fs::write(path, next)?;
        Ok(())
    }

    fn write_marker(&self, name: &str, reason: &str) -> Result<()> {
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        fs::write(path, reason)?;
        Ok(())
    }

    fn clear_marker(&self, name: &str) -> Result<()> {
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn read_marker_lines(&self, name: &str) -> Result<Vec<String>> {
        let Some(path) = self.marker_path(name) else {
            return Ok(Vec::new());
        };
        let content = fs::read_to_string(path).unwrap_or_default();
        Ok(content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }
}

fn projection_row_from_node(catalog: &Catalog, node: &NodeRecord) -> Option<SearchProjectionRow> {
    let kind = node.labels.iter().find_map(|label_id| {
        catalog
            .label_name(*label_id)
            .and_then(search_projection_kind_from_label)
    })?;
    let external_id = string_property(node, "id").unwrap_or_else(|| node.id.0.to_string());
    let title = first_string_property(node, &["title", "name", "summary", "id"])
        .unwrap_or_else(|| external_id.clone());
    let body = first_string_property(
        node,
        &["content", "body", "text", "summary", "title", "name"],
    )
    .unwrap_or_else(|| title.clone());
    let source_id = first_string_property(node, &["source_id", "thread_id", "source"]);
    let mut metadata = BTreeMap::new();
    for (key, value) in &node.properties {
        if matches!(key.as_str(), "kind" | "external_id" | "source_id") {
            continue;
        }
        metadata.insert(key.clone(), value_to_projection_string(value));
    }
    Some(SearchProjectionRow {
        kind,
        external_id,
        title,
        body,
        embedding: None,
        source_id,
        metadata,
    })
}

fn search_projection_kind_from_label(label: &str) -> Option<SearchProjectionKind> {
    match label {
        "Memory" | "memory" => Some(SearchProjectionKind::Memory),
        "Message" | "message" => Some(SearchProjectionKind::Message),
        "Entity" | "entity" => Some(SearchProjectionKind::Entity),
        "Source" | "source" => Some(SearchProjectionKind::Source),
        "SourceChunk" | "source_chunk" | "chunk" => Some(SearchProjectionKind::SourceChunk),
        "Community" | "community" => Some(SearchProjectionKind::Community),
        _ => None,
    }
}

fn first_string_property(node: &NodeRecord, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| string_property(node, key))
}

fn string_property(node: &NodeRecord, key: &str) -> Option<String> {
    node.properties.get(key).map(value_to_projection_string)
}

fn value_to_projection_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::String(value) => value.clone(),
    }
}

fn fuse_score(vector_score: f64, text_score: f64, text_available: bool) -> f64 {
    let score = if vector_score > 0.0 && text_score > 0.0 {
        (vector_score * 0.6 + text_score * 0.4) * 1.1
    } else if vector_score > 0.0 {
        if text_available {
            vector_score * 0.95
        } else {
            vector_score
        }
    } else {
        text_score * 0.7
    };
    score.min(0.99)
}

fn text_score(query_terms: &BTreeSet<String>, document: &SearchDocument) -> f64 {
    let haystack = tokenize(&format!("{} {}", document.title, document.content));
    if haystack.is_empty() {
        return 0.0;
    }
    let matches = query_terms
        .iter()
        .filter(|term| haystack.contains(*term))
        .count();
    if matches == 0 {
        return 0.0;
    }
    matches as f64 / query_terms.len() as f64
}

fn tokenize(text: &str) -> BTreeSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| token.to_lowercase())
        .collect()
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (l, r) in left.iter().zip(right.iter()) {
        let l = f64::from(*l);
        let r = f64::from(*r);
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some((dot / (left_norm.sqrt() * right_norm.sqrt())).max(0.0))
}

fn encode_embedding(embedding: Option<&[f32]>) -> String {
    embedding
        .map(|values| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn decode_embedding(input: &str) -> Result<Option<Vec<f32>>> {
    if input.is_empty() {
        return Ok(None);
    }
    input
        .split(',')
        .map(|raw| {
            raw.parse::<f32>()
                .map_err(|_| SkeinError::Storage(format!("invalid embedding value: {raw}")))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn encode_metadata(metadata: &BTreeMap<String, String>) -> String {
    metadata
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_string(value)))
        .collect::<Vec<_>>()
        .join(";")
}

fn decode_metadata(input: &str) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    if input.is_empty() {
        return Ok(metadata);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid metadata pair: {pair}"
            )));
        };
        metadata.insert(decode_string(key)?, decode_string(value)?);
    }
    Ok(metadata)
}

fn encode_string(input: &str) -> String {
    input
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_string(input: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = u8::from_str_radix(&input[offset..offset + 2], 16)
            .map_err(|_| SkeinError::Storage(format!("invalid hex string: {input}")))?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

fn split_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "search projection missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "search projection checksum")?;
    Ok((body, checksum))
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use crate::store::GraphStore;

    #[test]
    fn hybrid_search_combines_vector_and_text() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        index
            .upsert(doc("b", "Embedding model", "Vector search", [0.0, 1.0]))
            .unwrap();

        let hits = index.search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits[0].id, "a");
        assert!(hits[0].vector_score > 0.99);
        assert!(hits[0].text_score > 0.0);
    }

    #[test]
    fn vector_dimension_mismatch_degrades_to_text() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();

        let hits = index.search("graph", Some(&[1.0, 0.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[0].vector_score, 0.0);
        assert!(hits[0].text_score > 0.0);
        assert!(hits[0].fallback_reasons[0].contains("dimension"));
    }

    #[test]
    fn projection_snapshot_round_trips() {
        let path = unique_test_dir("search_snapshot");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
                .unwrap();
            index.checkpoint().unwrap();
        }
        {
            let index = SearchIndex::open(&path).unwrap();
            let hits = index.search("adjacency", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);
            assert_eq!(hits[0].id, "a");
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_markers_round_trip() {
        let path = unique_test_dir("search_markers");
        let index = SearchIndex::open(&path).unwrap();
        index
            .mark_full_reindex_needed("embedding model changed")
            .unwrap();
        index
            .mark_full_reindex_needed("embedding model changed")
            .unwrap();
        index
            .mark_metadata_repair_needed("missing metadata columns")
            .unwrap();

        assert!(index.full_reindex_needed());
        assert!(index.metadata_repair_needed());
        assert_eq!(
            std::fs::read_to_string(path.join(FULL_REINDEX_MARKER)).unwrap(),
            "embedding model changed"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_rows_encode_nowledge_shapes() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem_1".to_string(),
                title: "Graph storage".to_string(),
                body: "Native adjacency and WAL".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("thread_1".to_string()),
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            })
            .unwrap();

        let hits = index.search("wal", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:mem_1");
        let document = index.document("memory:mem_1").unwrap();
        assert_eq!(
            document.metadata.get("kind").map(String::as_str),
            Some("memory")
        );
        assert_eq!(
            document.metadata.get("external_id").map(String::as_str),
            Some("mem_1")
        );
        assert_eq!(
            document.metadata.get("source_id").map(String::as_str),
            Some("thread_1")
        );
    }

    #[test]
    fn full_rebuild_projects_graph_nodes() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    (
                        "content".to_string(),
                        Value::String("Native adjacency and WAL".to_string()),
                    ),
                    (
                        "source_id".to_string(),
                        Value::String("thread_1".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String("entity_1".to_string())),
                    ("name".to_string(), Value::String("Skein".to_string())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        let summary = index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert_eq!(summary.scanned_nodes, 2);
        assert_eq!(summary.indexed_documents, 2);
        assert!(index.document("memory:mem_1").is_some());
        assert_eq!(
            index
                .document("entity:entity_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("entity")
        );
        let hits = index.search("adjacency", None, SearchMode::Text, 10);
        assert_eq!(hits[0].id, "memory:mem_1");
    }

    #[test]
    fn full_rebuild_limit_failure_keeps_existing_projection() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String("entity_1".to_string())),
                    ("name".to_string(), Value::String("Skein".to_string())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
            .unwrap();
        let error = index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions { max_rows: Some(1) })
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("old").is_some());
    }

    #[test]
    fn full_rebuild_clears_projection_markers_on_success() {
        let path = unique_test_dir("search_rebuild_markers");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::open(&path).unwrap();
        index.mark_full_reindex_needed("stale projection").unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert!(!index.full_reindex_needed());
        assert!(!index.metadata_repair_needed());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn metadata_repair_updates_only_metadata() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    ("space_id".to_string(), Value::String("default".to_string())),
                    (
                        "source_id".to_string(),
                        Value::String("thread_1".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Old title".to_string(),
                content: "Old body should stay".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        let summary = index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();

        assert_eq!(summary.repaired_documents, 1);
        let document = index.document("memory:mem_1").unwrap();
        assert_eq!(document.title, "Old title");
        assert_eq!(document.content, "Old body should stay");
        assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
        assert_eq!(
            document.metadata.get("kind").map(String::as_str),
            Some("memory")
        );
        assert_eq!(
            document.metadata.get("source_id").map(String::as_str),
            Some("thread_1")
        );
        assert_eq!(
            document.metadata.get("space_id").map(String::as_str),
            Some("default")
        );
    }

    #[test]
    fn metadata_repair_limit_failure_keeps_existing_metadata() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }

        let mut index = SearchIndex::in_memory();
        for id in ["mem_1", "mem_2"] {
            index
                .upsert(SearchDocument {
                    id: format!("memory:{id}"),
                    title: id.to_string(),
                    content: id.to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
                })
                .unwrap();
        }
        let error = index
            .repair_metadata_from_graph(
                &catalog,
                &store,
                MetadataRepairOptions { max_rows: Some(1) },
            )
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("stale")
        );
    }

    #[test]
    fn metadata_repair_marks_full_reindex_when_rows_are_missing() {
        let path = unique_test_dir("metadata_repair_missing_rows");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::open(&path).unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();
        let summary = index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();

        assert_eq!(summary.missing_documents, 1);
        assert!(index.full_reindex_needed());
        assert!(!index.metadata_repair_needed());
        std::fs::remove_dir_all(path).unwrap();
    }

    fn doc<const N: usize>(
        id: &str,
        title: &str,
        content: &str,
        embedding: [f32; N],
    ) -> SearchDocument {
        SearchDocument {
            id: id.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            embedding: Some(embedding.to_vec()),
            metadata: BTreeMap::new(),
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_search_{name}_{nanos}"))
    }
}
