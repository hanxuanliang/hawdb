use crate::error::{Result, SkeinError};
use crate::schema::{Catalog, LabelId, RelTypeId};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const STORAGE_VERSION: &str = "skein-storage-v1";
const CHECKPOINT_FILE: &str = "checkpoint.skein";
const WAL_FILE: &str = "wal.skein";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityPolicy {
    #[default]
    SyncOnCheckpoint,
    SyncOnEveryWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub labels: BTreeSet<LabelId>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelRecord {
    pub id: RelId,
    pub source: NodeId,
    pub target: NodeId,
    pub rel_type: RelTypeId,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedNodesCreate {
    pub source_label: String,
    pub source_properties: BTreeMap<String, Value>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutation {
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    CreateConnectedNodes(ConnectedNodesCreate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationSummary {
    pub rows: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Default)]
pub struct GraphStore {
    next_node_id: u64,
    next_rel_id: u64,
    nodes: BTreeMap<NodeId, NodeRecord>,
    relationships: BTreeMap<RelId, RelRecord>,
    outgoing: BTreeMap<(NodeId, RelTypeId), BTreeSet<RelId>>,
    incoming: BTreeMap<(NodeId, RelTypeId), BTreeSet<RelId>>,
    property_index: BTreeMap<(LabelId, String, Value), BTreeSet<NodeId>>,
    durable: Option<DurableStore>,
}

impl GraphStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>, catalog: &mut Catalog) -> Result<Self> {
        Self::open_with_durability(path, catalog, DurabilityPolicy::default())
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        let durable = DurableStore::open(path.as_ref(), durability)?;
        let mut store = Self {
            next_node_id: 0,
            next_rel_id: 0,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            property_index: BTreeMap::new(),
            durable: Some(durable),
        };
        store.load_checkpoint(catalog)?;
        store.replay_wal(catalog)?;
        Ok(store)
    }

    pub fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<NodeId> {
        let label_id = catalog.get_or_create_label(label);
        let id = NodeId(self.next_node_id);
        if let Some(durable) = &mut self.durable {
            durable.append_create_node(id, label, &properties)?;
        }
        self.apply_create_node(id, label_id, properties);
        Ok(id)
    }

    pub fn create_relationship(
        &mut self,
        catalog: &mut Catalog,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<RelId> {
        if !self.nodes.contains_key(&source) {
            return Err(SkeinError::Storage(format!(
                "source node {} does not exist",
                source.0
            )));
        }
        if !self.nodes.contains_key(&target) {
            return Err(SkeinError::Storage(format!(
                "target node {} does not exist",
                target.0
            )));
        }
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        let id = RelId(self.next_rel_id);
        if let Some(durable) = &mut self.durable {
            durable.append_create_relationship(id, source, target, rel_type, &properties)?;
        }
        self.apply_create_relationship(id, source, target, rel_type_id, properties);
        Ok(id)
    }

    pub fn create_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source = NodeId(self.next_node_id);
        let target = NodeId(self.next_node_id + 1);
        let relationship = RelId(self.next_rel_id);
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![
                WalOp::CreateNode {
                    id: source,
                    label: request.source_label.clone(),
                    properties: request.source_properties.clone(),
                },
                WalOp::CreateNode {
                    id: target,
                    label: request.target_label.clone(),
                    properties: request.target_properties.clone(),
                },
                WalOp::CreateRelationship {
                    id: relationship,
                    source,
                    target,
                    rel_type: request.rel_type.clone(),
                    properties: request.rel_properties.clone(),
                },
            ])?;
        }
        self.apply_create_node(source, source_label_id, request.source_properties);
        self.apply_create_node(target, target_label_id, request.target_properties);
        self.apply_create_relationship(
            relationship,
            source,
            target,
            rel_type_id,
            request.rel_properties,
        );
        Ok((source, relationship, target))
    }

    pub fn commit_mutations(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
    ) -> Result<MutationSummary> {
        let mut next_node_id = self.next_node_id;
        let mut next_rel_id = self.next_rel_id;
        let mut ops = Vec::new();
        let mut rows = Vec::new();

        for mutation in mutations {
            match mutation {
                GraphMutation::CreateNode { label, properties } => {
                    let id = NodeId(next_node_id);
                    next_node_id += 1;
                    ops.push(WalOp::CreateNode {
                        id,
                        label,
                        properties,
                    });
                    rows.push(BTreeMap::from([(
                        "node_id".to_string(),
                        Value::Int(id.0 as i64),
                    )]));
                }
                GraphMutation::CreateConnectedNodes(request) => {
                    let source = NodeId(next_node_id);
                    let target = NodeId(next_node_id + 1);
                    let relationship = RelId(next_rel_id);
                    next_node_id += 2;
                    next_rel_id += 1;
                    ops.push(WalOp::CreateNode {
                        id: source,
                        label: request.source_label,
                        properties: request.source_properties,
                    });
                    ops.push(WalOp::CreateNode {
                        id: target,
                        label: request.target_label,
                        properties: request.target_properties,
                    });
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type,
                        properties: request.rel_properties,
                    });
                    rows.push(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                    ]));
                }
            }
        }

        if ops.is_empty() {
            return Ok(MutationSummary { rows });
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        Ok(MutationSummary { rows })
    }

    pub fn checkpoint(&mut self, catalog: &Catalog) -> Result<()> {
        let Some(durable) = &mut self.durable else {
            return Ok(());
        };
        durable.write_checkpoint(
            catalog,
            self.next_node_id,
            self.next_rel_id,
            &self.nodes,
            &self.relationships,
        )?;
        durable.truncate_wal()?;
        Ok(())
    }

    pub fn storage_version(&self) -> &'static str {
        STORAGE_VERSION
    }

    fn apply_create_node(
        &mut self,
        id: NodeId,
        label_id: LabelId,
        properties: BTreeMap<String, Value>,
    ) {
        self.apply_create_node_with_labels(id, BTreeSet::from([label_id]), properties);
    }

    fn apply_create_node_with_labels(
        &mut self,
        id: NodeId,
        labels: BTreeSet<LabelId>,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_node_id = self.next_node_id.max(id.0 + 1);
        self.nodes.insert(
            id,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
        if let Some(node) = self.nodes.get(&id) {
            for label_id in &node.labels {
                for (property, value) in &node.properties {
                    self.property_index
                        .entry((*label_id, property.clone(), value.clone()))
                        .or_default()
                        .insert(id);
                }
            }
        }
    }

    fn apply_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: RelTypeId,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_rel_id = self.next_rel_id.max(id.0 + 1);
        self.relationships.insert(
            id,
            RelRecord {
                id,
                source,
                target,
                rel_type,
                properties,
            },
        );
        self.outgoing
            .entry((source, rel_type))
            .or_default()
            .insert(id);
        self.incoming
            .entry((target, rel_type))
            .or_default()
            .insert(id);
    }

    pub fn scan_nodes<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.nodes
            .values()
            .filter(move |node| label_id.map(|id| node.labels.contains(&id)).unwrap_or(true))
    }

    pub fn seek_nodes_by_property<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        value: &Value,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.property_index
            .get(&(label_id, property.to_string(), value.clone()))
            .into_iter()
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
    }

    pub fn outgoing_relationships<'a>(
        &'a self,
        source: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.outgoing
            .get(&(source, rel_type))
            .into_iter()
            .flat_map(|rel_ids| rel_ids.iter())
            .filter_map(|rel_id| self.relationships.get(rel_id))
    }

    pub fn relationship(&self, id: RelId) -> Option<&RelRecord> {
        self.relationships.get(&id)
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&id)
    }

    fn load_checkpoint(&mut self, catalog: &mut Catalog) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        if !durable.checkpoint_path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(&durable.checkpoint_path)?;
        let (body, checksum) = split_checkpoint_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "checkpoint checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        for line in body.lines() {
            if line == "SKEIN_CHECKPOINT_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["version", STORAGE_VERSION] => {}
                ["next_node_id", raw] => {
                    self.next_node_id = parse_u64(raw, "next_node_id")?;
                }
                ["next_rel_id", raw] => {
                    self.next_rel_id = parse_u64(raw, "next_rel_id")?;
                }
                ["label", raw_id, raw_name] => {
                    let id = LabelId(parse_u32(raw_id, "label id")?);
                    catalog.import_label(id, decode_string(raw_name)?);
                }
                ["rel_type", raw_id, raw_name] => {
                    let id = RelTypeId(parse_u32(raw_id, "rel type id")?);
                    catalog.import_rel_type(id, decode_string(raw_name)?);
                }
                ["node", raw_id, raw_labels, raw_properties] => {
                    let id = NodeId(parse_u64(raw_id, "node id")?);
                    let labels = parse_label_set(raw_labels)?;
                    let properties = decode_properties(raw_properties)?;
                    self.apply_create_node_with_labels(id, labels, properties);
                }
                ["rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                    self.apply_create_relationship(
                        RelId(parse_u64(raw_id, "rel id")?),
                        NodeId(parse_u64(raw_source, "rel source")?),
                        NodeId(parse_u64(raw_target, "rel target")?),
                        RelTypeId(parse_u32(raw_type, "rel type")?),
                        decode_properties(raw_properties)?,
                    );
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid checkpoint line: {line}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn replay_wal(&mut self, catalog: &mut Catalog) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        let wal_path = durable.wal_path.clone();
        let mut next_lsn = durable.next_lsn;
        if !wal_path.exists() {
            return Ok(());
        }
        let file = File::open(&wal_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let Some(entry) = WalEntry::decode(&line)? else {
                break;
            };
            next_lsn = next_lsn.max(entry.lsn + 1);
            match entry.op {
                WalOp::Batch(ops) => {
                    for op in ops {
                        self.apply_wal_op(catalog, op);
                    }
                }
                op => {
                    self.apply_wal_op(catalog, op);
                }
            }
        }
        if let Some(durable) = &mut self.durable {
            durable.next_lsn = next_lsn;
        }
        Ok(())
    }

    fn apply_wal_op(&mut self, catalog: &mut Catalog, op: WalOp) {
        match op {
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let label_id = catalog.get_or_create_label(&label);
                self.apply_create_node(id, label_id, properties);
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                self.apply_create_relationship(id, source, target, rel_type_id, properties);
            }
            WalOp::Batch(ops) => {
                for op in ops {
                    self.apply_wal_op(catalog, op);
                }
            }
        }
    }
}

#[derive(Debug)]
struct DurableStore {
    checkpoint_path: PathBuf,
    wal_path: PathBuf,
    next_lsn: u64,
    durability: DurabilityPolicy,
}

impl DurableStore {
    fn open(path: &Path, durability: DurabilityPolicy) -> Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            checkpoint_path: path.join(CHECKPOINT_FILE),
            wal_path: path.join(WAL_FILE),
            next_lsn: 1,
            durability,
        })
    }

    fn append_create_node(
        &mut self,
        id: NodeId,
        label: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::CreateNode {
                id,
                label: label.to_string(),
                properties: properties.clone(),
            },
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn append_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type: rel_type.to_string(),
                properties: properties.clone(),
            },
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn append_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::Batch(ops),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn finish_wal_append(&self, file: &mut File) -> Result<()> {
        file.flush()?;
        if self.durability == DurabilityPolicy::SyncOnEveryWrite {
            file.sync_data()?;
        }
        Ok(())
    }

    fn write_checkpoint(
        &self,
        catalog: &Catalog,
        next_node_id: u64,
        next_rel_id: u64,
        nodes: &BTreeMap<NodeId, NodeRecord>,
        relationships: &BTreeMap<RelId, RelRecord>,
    ) -> Result<()> {
        let mut body = String::new();
        body.push_str("SKEIN_CHECKPOINT_V1\n");
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!("next_node_id\t{next_node_id}\n"));
        body.push_str(&format!("next_rel_id\t{next_rel_id}\n"));
        for label in catalog.labels() {
            if !label.name.is_empty() {
                body.push_str(&format!(
                    "label\t{}\t{}\n",
                    label.id.0,
                    encode_string(&label.name)
                ));
            }
        }
        for rel_type in catalog.rel_types() {
            if !rel_type.name.is_empty() {
                body.push_str(&format!(
                    "rel_type\t{}\t{}\n",
                    rel_type.id.0,
                    encode_string(&rel_type.name)
                ));
            }
        }
        for node in nodes.values() {
            body.push_str(&format!(
                "node\t{}\t{}\t{}\n",
                node.id.0,
                encode_label_set(&node.labels),
                encode_properties(&node.properties)
            ));
        }
        for relationship in relationships.values() {
            body.push_str(&format!(
                "rel\t{}\t{}\t{}\t{}\t{}\n",
                relationship.id.0,
                relationship.source.0,
                relationship.target.0,
                relationship.rel_type.0,
                encode_properties(&relationship.properties)
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = self.checkpoint_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &self.checkpoint_path)?;
        Ok(())
    }

    fn truncate_wal(&mut self) -> Result<()> {
        File::create(&self.wal_path)?.sync_all()?;
        self.next_lsn = 1;
        Ok(())
    }
}

#[derive(Debug)]
struct WalEntry {
    lsn: u64,
    op: WalOp,
}

#[derive(Debug, Clone)]
enum WalOp {
    CreateNode {
        id: NodeId,
        label: String,
        properties: BTreeMap<String, Value>,
    },
    CreateRelationship {
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: String,
        properties: BTreeMap<String, Value>,
    },
    Batch(Vec<WalOp>),
}

impl WalEntry {
    fn encode(&self) -> String {
        let payload = match &self.op {
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => format!(
                "create_node\t{}\t{}\t{}",
                id.0,
                encode_string(label),
                encode_properties(properties)
            ),
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => format!(
                "create_rel\t{}\t{}\t{}\t{}\t{}",
                id.0,
                source.0,
                target.0,
                encode_string(rel_type),
                encode_properties(properties)
            ),
            WalOp::Batch(ops) => format!(
                "batch\t{}",
                ops.iter()
                    .map(encode_wal_op_for_batch)
                    .collect::<Vec<_>>()
                    .join("|")
            ),
        };
        let body = format!("{}\t{payload}", self.lsn);
        let checksum = checksum_bytes(body.as_bytes());
        format!("{body}\t{checksum}")
    }

    fn decode(line: &str) -> Result<Option<Self>> {
        let Some((body, raw_checksum)) = line.rsplit_once('\t') else {
            return Ok(None);
        };
        let Ok(expected) = raw_checksum.parse::<u64>() else {
            return Ok(None);
        };
        let actual = checksum_bytes(body.as_bytes());
        if expected != actual {
            return Ok(None);
        }
        let fields = body.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            [raw_lsn, "create_node", raw_id, raw_label, raw_properties] => Ok(Some(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateNode {
                    id: NodeId(parse_u64(raw_id, "wal node id")?),
                    label: decode_string(raw_label)?,
                    properties: decode_properties(raw_properties)?,
                },
            })),
            [raw_lsn, "create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                Ok(Some(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationship {
                        id: RelId(parse_u64(raw_id, "wal rel id")?),
                        source: NodeId(parse_u64(raw_source, "wal rel source")?),
                        target: NodeId(parse_u64(raw_target, "wal rel target")?),
                        rel_type: decode_string(raw_type)?,
                        properties: decode_properties(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "batch", raw_ops] => Ok(Some(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::Batch(decode_wal_batch(raw_ops)?),
            })),
            _ => Err(SkeinError::Storage(format!("invalid wal entry: {line}"))),
        }
    }
}

fn encode_wal_op_for_batch(op: &WalOp) -> String {
    match op {
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => format!(
            "create_node,{},{},{}",
            id.0,
            encode_string(label),
            encode_properties(properties)
        ),
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => format!(
            "create_rel,{},{},{},{},{}",
            id.0,
            source.0,
            target.0,
            encode_string(rel_type),
            encode_properties(properties)
        ),
        WalOp::Batch(_) => unreachable!("nested wal batches are not encoded"),
    }
}

fn decode_wal_batch(input: &str) -> Result<Vec<WalOp>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split('|').map(decode_wal_op_from_batch).collect()
}

fn decode_wal_op_from_batch(input: &str) -> Result<WalOp> {
    let fields = input.split(',').collect::<Vec<_>>();
    match fields.as_slice() {
        ["create_node", raw_id, raw_label, raw_properties] => Ok(WalOp::CreateNode {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
            label: decode_string(raw_label)?,
            properties: decode_properties(raw_properties)?,
        }),
        ["create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
            Ok(WalOp::CreateRelationship {
                id: RelId(parse_u64(raw_id, "batch rel id")?),
                source: NodeId(parse_u64(raw_source, "batch rel source")?),
                target: NodeId(parse_u64(raw_target, "batch rel target")?),
                rel_type: decode_string(raw_type)?,
                properties: decode_properties(raw_properties)?,
            })
        }
        _ => Err(SkeinError::Storage(format!(
            "invalid batch wal op: {input}"
        ))),
    }
}

fn split_checkpoint_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "checkpoint missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "checkpoint checksum")?;
    Ok((body, checksum))
}

fn encode_label_set(labels: &BTreeSet<LabelId>) -> String {
    labels
        .iter()
        .map(|label| label.0.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_label_set(input: &str) -> Result<BTreeSet<LabelId>> {
    if input.is_empty() {
        return Ok(BTreeSet::new());
    }
    input
        .split(',')
        .map(|raw| parse_u32(raw, "label id").map(LabelId))
        .collect()
}

fn encode_properties(properties: &BTreeMap<String, Value>) -> String {
    properties
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_value(value)))
        .collect::<Vec<_>>()
        .join(";")
}

fn decode_properties(input: &str) -> Result<BTreeMap<String, Value>> {
    let mut properties = BTreeMap::new();
    if input.is_empty() {
        return Ok(properties);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid property pair: {pair}"
            )));
        };
        properties.insert(decode_string(key)?, decode_value(value)?);
    }
    Ok(properties)
}

fn encode_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(false) => "b0".to_string(),
        Value::Bool(true) => "b1".to_string(),
        Value::Int(value) => format!("i{value}"),
        Value::String(value) => format!("s{}", encode_string(value)),
    }
}

fn decode_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Err(SkeinError::Storage("empty encoded value".to_string()));
    }
    let (kind, rest) = input.split_at(1);
    match kind {
        "n" if rest.is_empty() => Ok(Value::Null),
        "b" => match rest {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(SkeinError::Storage(format!("invalid bool value: {input}"))),
        },
        "i" => parse_i64(rest, "integer value").map(Value::Int),
        "s" => decode_string(rest).map(Value::String),
        _ => Err(SkeinError::Storage(format!(
            "invalid encoded value: {input}"
        ))),
    }
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

fn parse_u32(input: &str, name: &str) -> Result<u32> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
mod tests {
    use super::{ConnectedNodesCreate, GraphStore, NodeId};
    use crate::schema::Catalog;
    use crate::value::Value;
    use std::collections::BTreeMap;
    use std::io::Write;

    #[test]
    fn replays_relationships_from_wal_and_rebuilds_adjacency() {
        let path = unique_test_dir("rel_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "RELATES_TO",
                    properties([("weight", Value::Int(7))]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].target, NodeId(1));
            assert_eq!(rels[0].properties.get("weight"), Some(&Value::Int(7)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoints_relationships_and_truncates_wal() {
        let path = unique_test_dir("rel_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].source, NodeId(0));
            assert_eq!(rels[0].target, NodeId(1));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn replays_property_index_from_wal() {
        let path = unique_test_dir("property_index_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(label, "id", &Value::Int(1))
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(
                nodes[0].properties.get("title"),
                Some(&Value::String("Graph foundations".to_string()))
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuilds_property_index_from_checkpoint() {
        let path = unique_test_dir("property_index_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(
                    label,
                    "title",
                    &Value::String("Graph foundations".to_string()),
                )
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].properties.get("id"), Some(&Value::Int(1)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn stops_replay_at_torn_wal_tail() {
        let path = unique_test_dir("torn_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(path.join("wal.skein"))
            .unwrap()
            .write_all(b"torn-entry-without-checksum")
            .unwrap();

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
        assert_eq!(nodes.len(), 1);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn skips_torn_batch_wal_without_partial_path_recovery() {
        let path = unique_test_dir("torn_batch_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".to_string(),
                        source_properties: properties([("id", Value::Int(1))]),
                        rel_type: "MENTIONS".to_string(),
                        rel_properties: BTreeMap::new(),
                        target_label: "Entity".to_string(),
                        target_properties: properties([("id", Value::Int(10))]),
                    },
                )
                .unwrap();
        }
        let wal_path = path.join("wal.skein");
        let wal = std::fs::read_to_string(&wal_path).unwrap();
        let torn = wal.rsplit_once('\t').unwrap().0;
        std::fs::write(&wal_path, torn).unwrap();

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(store.scan_nodes(None).next().is_none());
        assert!(catalog.rel_type_id("MENTIONS").is_none());
        std::fs::remove_dir_all(path).unwrap();
    }

    fn properties<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_store_{name}_{nanos}"))
    }
}
