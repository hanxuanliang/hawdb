use sha2::{Digest, Sha256};
use skein::{Row, Value};

pub(crate) fn rows_sha256(rows: &[Row]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((rows.len() as u64).to_le_bytes());
    for row in rows {
        hasher.update((row.len() as u64).to_le_bytes());
        for (name, value) in row {
            hash_bytes(&mut hasher, name.as_bytes());
            hash_value(&mut hasher, value);
        }
    }
    format!("{:x}", hasher.finalize())
}

pub(crate) fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update([0]),
        Value::Bool(value) => {
            hasher.update([1]);
            hasher.update([u8::from(*value)]);
        }
        Value::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_le_bytes());
        }
        Value::Float(value) => {
            hasher.update([3]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            hasher.update([4]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::List(values) => {
            hasher.update([5]);
            hasher.update((values.len() as u64).to_le_bytes());
            for value in values {
                hash_value(hasher, value);
            }
        }
        Value::Map(values) => {
            hasher.update([6]);
            hasher.update((values.len() as u64).to_le_bytes());
            for (name, value) in values {
                hash_bytes(hasher, name.as_bytes());
                hash_value(hasher, value);
            }
        }
    }
}

pub(crate) fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
