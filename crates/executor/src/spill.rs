use crate::binding::{binding_payload_bytes, Binding};
use skein_core::{LabelId, RelTypeId, Result, SkeinError, Value};
use skein_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_SPILL_RECORD_BYTES: usize = 1024 * 1024 * 1024;
const MAX_VALUE_DEPTH: usize = 64;
static NEXT_SPILL_ID: AtomicU64 = AtomicU64::new(0);

pub struct SpillRun {
    path: PathBuf,
}

impl SpillRun {
    pub fn create(directory: &Path, operator: &str) -> Result<(Self, SpillWriter)> {
        std::fs::create_dir_all(directory).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to create spill directory '{}': {error}",
                directory.display()
            ))
        })?;
        for _ in 0..32 {
            let id = NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!(
                "skein-{operator}-{}-{id}.spill",
                std::process::id()
            ));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => {
                    return Ok((
                        Self { path },
                        SpillWriter {
                            writer: BufWriter::new(file),
                        },
                    ));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(SkeinError::Execution(format!(
                        "failed to create spill run '{}': {error}",
                        path.display()
                    )));
                }
            }
        }
        Err(SkeinError::Execution(
            "failed to allocate a unique spill run path".to_string(),
        ))
    }

    pub fn reader(&self) -> Result<SpillReader> {
        let file = File::open(&self.path).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to open spill run '{}': {error}",
                self.path.display()
            ))
        })?;
        Ok(SpillReader {
            reader: BufReader::new(file),
        })
    }
}

impl Drop for SpillRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct SpillWriter {
    writer: BufWriter<File>,
}

impl SpillWriter {
    pub fn write(&mut self, ordinal: u64, binding: &Binding, max_record_bytes: u64) -> Result<u64> {
        let mut payload = Vec::with_capacity(binding_payload_bytes(binding));
        write_u64(&mut payload, ordinal)?;
        write_binding(&mut payload, binding)?;
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            SkeinError::Execution("spill record exceeds the supported size".to_string())
        })?;
        let record_bytes = payload_len.saturating_add(8);
        if record_bytes > max_record_bytes {
            return Err(SkeinError::Execution(format!(
                "spill record uses {record_bytes} bytes, exceeding the remaining spill budget {max_record_bytes}"
            )));
        }
        self.writer
            .write_all(&payload_len.to_le_bytes())
            .and_then(|_| self.writer.write_all(&payload))
            .map_err(|error| {
                SkeinError::Execution(format!("failed to write spill run: {error}"))
            })?;
        Ok(record_bytes)
    }

    pub fn finish(mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|error| SkeinError::Execution(format!("failed to flush spill run: {error}")))
    }
}

pub struct SpillReader {
    reader: BufReader<File>,
}

impl SpillReader {
    pub fn read(&mut self, max_record_bytes: usize) -> Result<Option<(u64, Binding)>> {
        let mut encoded_len = [0u8; 8];
        let bytes_read = self.reader.read(&mut encoded_len).map_err(|error| {
            SkeinError::Execution(format!("failed to read spill record length: {error}"))
        })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut encoded_len[bytes_read..])
            .map_err(|error| {
                SkeinError::Execution(format!("truncated spill record length: {error}"))
            })?;
        let payload_len = usize::try_from(u64::from_le_bytes(encoded_len)).map_err(|_| {
            SkeinError::Execution("spill record length does not fit in memory".to_string())
        })?;
        let safety_limit = MAX_SPILL_RECORD_BYTES.min(max_record_bytes);
        if payload_len > safety_limit {
            return Err(SkeinError::Execution(format!(
                "spill record length {payload_len} exceeds the admitted limit {safety_limit}"
            )));
        }
        let mut payload = vec![0; payload_len];
        self.reader.read_exact(&mut payload).map_err(|error| {
            SkeinError::Execution(format!("truncated spill record payload: {error}"))
        })?;
        let mut cursor = Cursor::new(payload.as_slice());
        let ordinal = read_u64(&mut cursor)?;
        let binding = read_binding(&mut cursor)?;
        if cursor.position() != payload.len() as u64 {
            return Err(SkeinError::Execution(
                "spill record contains trailing bytes".to_string(),
            ));
        }
        Ok(Some((ordinal, binding)))
    }
}

fn write_binding(output: &mut Vec<u8>, binding: &Binding) -> Result<()> {
    write_value_map(output, &binding.values, 0)?;
    write_len(output, binding.nodes.len())?;
    for (name, node) in &binding.nodes {
        write_string(output, name)?;
        write_u64(output, node.id.0)?;
        write_len(output, node.labels.len())?;
        for label in &node.labels {
            write_u64(output, label.0 as u64)?;
        }
        write_value_map(output, &node.properties, 0)?;
    }
    write_len(output, binding.relationships.len())?;
    for (name, relationship) in &binding.relationships {
        write_string(output, name)?;
        write_u64(output, relationship.id.0)?;
        write_u64(output, relationship.source.0)?;
        write_u64(output, relationship.target.0)?;
        write_u64(output, relationship.rel_type.0 as u64)?;
        write_value_map(output, &relationship.properties, 0)?;
    }
    Ok(())
}

fn read_binding(input: &mut Cursor<&[u8]>) -> Result<Binding> {
    let values = read_value_map(input, 0)?;
    let mut nodes = BTreeMap::new();
    for _ in 0..read_len(input)? {
        let name = read_string(input)?;
        let id = NodeId(read_u64(input)?);
        let mut labels = BTreeSet::new();
        for _ in 0..read_len(input)? {
            labels.insert(LabelId(read_u32(input)?));
        }
        let properties = read_value_map(input, 0)?;
        nodes.insert(
            name,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
    }
    let mut relationships = BTreeMap::new();
    for _ in 0..read_len(input)? {
        let name = read_string(input)?;
        relationships.insert(
            name,
            RelRecord {
                id: RelId(read_u64(input)?),
                source: NodeId(read_u64(input)?),
                target: NodeId(read_u64(input)?),
                rel_type: RelTypeId(read_u32(input)?),
                properties: read_value_map(input, 0)?,
            },
        );
    }
    Ok(Binding {
        values,
        nodes,
        relationships,
    })
}

fn write_value_map(
    output: &mut Vec<u8>,
    values: &BTreeMap<String, Value>,
    depth: usize,
) -> Result<()> {
    check_depth(depth)?;
    write_len(output, values.len())?;
    for (name, value) in values {
        write_string(output, name)?;
        write_value(output, value, depth + 1)?;
    }
    Ok(())
}

fn read_value_map(input: &mut Cursor<&[u8]>, depth: usize) -> Result<BTreeMap<String, Value>> {
    check_depth(depth)?;
    let mut values = BTreeMap::new();
    for _ in 0..read_len(input)? {
        values.insert(read_string(input)?, read_value(input, depth + 1)?);
    }
    Ok(values)
}

fn write_value(output: &mut Vec<u8>, value: &Value, depth: usize) -> Result<()> {
    check_depth(depth)?;
    match value {
        Value::Null => output.push(0),
        Value::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        Value::Int(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            output.push(4);
            write_string(output, value)?;
        }
        Value::List(values) => {
            output.push(5);
            write_len(output, values.len())?;
            for value in values {
                write_value(output, value, depth + 1)?;
            }
        }
        Value::Map(values) => {
            output.push(6);
            write_value_map(output, values, depth + 1)?;
        }
    }
    Ok(())
}

fn read_value(input: &mut Cursor<&[u8]>, depth: usize) -> Result<Value> {
    check_depth(depth)?;
    Ok(match read_u8(input)? {
        0 => Value::Null,
        1 => Value::Bool(match read_u8(input)? {
            0 => false,
            1 => true,
            value => {
                return Err(SkeinError::Execution(format!(
                    "invalid boolean tag in spill record: {value}"
                )));
            }
        }),
        2 => Value::Int(read_i64(input)?),
        3 => Value::Float(f64::from_bits(read_u64(input)?)),
        4 => Value::String(read_string(input)?),
        5 => {
            let mut values = Vec::new();
            for _ in 0..read_len(input)? {
                values.push(read_value(input, depth + 1)?);
            }
            Value::List(values)
        }
        6 => Value::Map(read_value_map(input, depth + 1)?),
        tag => {
            return Err(SkeinError::Execution(format!(
                "invalid value tag in spill record: {tag}"
            )));
        }
    })
}

fn check_depth(depth: usize) -> Result<()> {
    if depth > MAX_VALUE_DEPTH {
        return Err(SkeinError::Execution(
            "spill value nesting exceeds the safety limit".to_string(),
        ));
    }
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    write_len(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(input: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_len(input)?;
    let mut bytes = vec![0; len];
    input.read_exact(&mut bytes).map_err(|error| {
        SkeinError::Execution(format!("truncated string in spill record: {error}"))
    })?;
    String::from_utf8(bytes)
        .map_err(|error| SkeinError::Execution(format!("invalid spill string: {error}")))
}

fn write_len(output: &mut Vec<u8>, value: usize) -> Result<()> {
    write_u64(
        output,
        u64::try_from(value).map_err(|_| {
            SkeinError::Execution("spill collection length is too large".to_string())
        })?,
    )
}

fn read_len(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let value = usize::try_from(read_u64(input)?).map_err(|_| {
        SkeinError::Execution("spill collection length does not fit in memory".to_string())
    })?;
    let remaining = input
        .get_ref()
        .len()
        .saturating_sub(input.position() as usize);
    if value > remaining {
        return Err(SkeinError::Execution(format!(
            "spill collection length {value} exceeds remaining payload {remaining}"
        )));
    }
    Ok(value)
}

fn write_u64(output: &mut Vec<u8>, value: u64) -> Result<()> {
    output
        .write_all(&value.to_le_bytes())
        .map_err(|error| SkeinError::Execution(format!("failed to encode spill integer: {error}")))
}

fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut bytes = [0; 1];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill tag: {error}")))?;
    Ok(bytes[0])
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    u32::try_from(read_u64(input)?)
        .map_err(|_| SkeinError::Execution("spill identifier exceeds u32".to_string()))
}

fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0; 8];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill integer: {error}")))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64(input: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut bytes = [0; 8];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill integer: {error}")))?;
    Ok(i64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_round_trip_preserves_bindings() {
        let binding = Binding {
            values: BTreeMap::from([(
                "nested".to_string(),
                Value::Map(BTreeMap::from([(
                    "items".to_string(),
                    Value::List(vec![Value::Int(1), Value::String("two".to_string())]),
                )])),
            )]),
            nodes: BTreeMap::from([(
                "n".to_string(),
                NodeRecord {
                    id: NodeId(7),
                    labels: BTreeSet::from([LabelId(3)]),
                    properties: BTreeMap::from([("score".to_string(), Value::Float(1.5))]),
                },
            )]),
            relationships: BTreeMap::from([(
                "r".to_string(),
                RelRecord {
                    id: RelId(11),
                    source: NodeId(7),
                    target: NodeId(8),
                    rel_type: RelTypeId(4),
                    properties: BTreeMap::from([("active".to_string(), Value::Bool(true))]),
                },
            )]),
        };
        let directory = std::env::temp_dir();
        let (run, mut writer) = SpillRun::create(&directory, "codec-test").unwrap();
        writer.write(42, &binding, u64::MAX).unwrap();
        writer.finish().unwrap();
        let mut reader = run.reader().unwrap();
        assert_eq!(reader.read(usize::MAX).unwrap(), Some((42, binding)));
        assert_eq!(reader.read(usize::MAX).unwrap(), None);
    }
}
