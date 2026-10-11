// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compact binary result envelope, measured against the JSON envelope
//! for issue #976.
//!
//! All integers are little-endian; lengths are u64 unless noted. Layout:
//!
//! ```text
//! "HDB1" magic (4 bytes)
//! u32 column count; per column: u32 len + UTF-8 name
//! u64 row count; per row, per column: tagged value
//! ```
//!
//! Value tags:
//!
//! ```text
//! 0 null · 1 bool u8 · 2 i64 · 3 f64 · 4 string (u64 len + bytes)
//! 5 binary (u64 len + raw bytes) · 6 uuid (16 raw bytes)
//! 7 list (u64 count + items) · 8 map (u64 count; per entry:
//! u32 key len + key bytes + value)
//! ```

use hawdb::{QueryOutput, Value};

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_binary_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(0),
        Value::Bool(item) => out.extend_from_slice(&[1, u8::from(*item)]),
        Value::Int(item) => {
            out.push(2);
            out.extend_from_slice(&item.to_le_bytes());
        }
        // Non-finite floats carry no bits the envelope can express;
        // they degrade to null the same way the JSON envelope does.
        Value::Float(item) if item.is_finite() => {
            out.push(3);
            out.extend_from_slice(&item.to_le_bytes());
        }
        Value::Float(_) => out.push(0),
        Value::String(item) => {
            out.push(4);
            put_u64(out, item.len() as u64);
            out.extend_from_slice(item.as_bytes());
        }
        Value::Binary(item) => {
            out.push(5);
            put_u64(out, item.len() as u64);
            out.extend_from_slice(item);
        }
        Value::Uuid(item) => {
            out.push(6);
            out.extend_from_slice(item.as_bytes());
        }
        Value::List(items) => {
            out.push(7);
            put_u64(out, items.len() as u64);
            for item in items {
                write_binary_value(out, item);
            }
        }
        Value::Map(items) => {
            out.push(8);
            put_u64(out, items.len() as u64);
            for (key, item) in items {
                put_u32(out, key.len() as u32);
                out.extend_from_slice(key.as_bytes());
                write_binary_value(out, item);
            }
        }
    }
}

/// Serialize `output` into the binary result envelope.
pub(crate) fn encode_output_binary(output: &QueryOutput) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"HDB1");
    let columns = output.schema().columns();
    put_u32(&mut out, columns.len() as u32);
    for column in columns {
        put_u32(&mut out, column.len() as u32);
        out.extend_from_slice(column.as_bytes());
    }
    let rows = output.value_rows();
    put_u64(&mut out, rows.len() as u64);
    for row in rows {
        for value in row {
            write_binary_value(&mut out, value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::Uuid;
    use std::collections::BTreeMap;

    fn encode(value: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        write_binary_value(&mut out, value);
        out
    }

    #[test]
    fn scalars_have_exact_layout() {
        assert_eq!(encode(&Value::Null), [0]);
        assert_eq!(encode(&Value::Bool(true)), [1, 1]);
        assert_eq!(encode(&Value::Bool(false)), [1, 0]);
        assert_eq!(encode(&Value::Int(42)), {
            let mut v = vec![2];
            v.extend_from_slice(&42i64.to_le_bytes());
            v
        });
        assert_eq!(encode(&Value::Float(1.5)), {
            let mut v = vec![3];
            v.extend_from_slice(&1.5f64.to_le_bytes());
            v
        });
        // Non-finite floats degrade to null, mirroring the JSON path.
        assert_eq!(encode(&Value::Float(f64::NAN)), [0]);
    }

    #[test]
    fn strings_and_blobs_are_len_prefixed() {
        assert_eq!(encode(&Value::String("ab".into())), {
            let mut v = vec![4];
            v.extend_from_slice(&2u64.to_le_bytes());
            v.extend_from_slice(b"ab");
            v
        });
        // Binary payloads keep interior NULs raw — no base64, no escape.
        assert_eq!(encode(&Value::Binary(vec![0, 1])), {
            let mut v = vec![5];
            v.extend_from_slice(&2u64.to_le_bytes());
            v.extend_from_slice(&[0, 1]);
            v
        });
    }

    #[test]
    fn uuid_is_sixteen_raw_bytes() {
        let uuid = Uuid::nil();
        let encoded = encode(&Value::Uuid(uuid));
        assert_eq!(encoded.len(), 17);
        assert_eq!(encoded[0], 6);
        assert_eq!(&encoded[1..], uuid.as_bytes().as_slice());
    }

    #[test]
    fn containers_recurse_in_order() {
        let list = Value::List(vec![Value::Int(1), Value::Null]);
        assert_eq!(encode(&list), {
            let mut v = vec![7];
            v.extend_from_slice(&2u64.to_le_bytes());
            v.push(2);
            v.extend_from_slice(&1i64.to_le_bytes());
            v.push(0);
            v
        });

        let mut map = BTreeMap::new();
        map.insert("k".to_string(), Value::Bool(true));
        assert_eq!(encode(&Value::Map(map)), {
            let mut v = vec![8];
            v.extend_from_slice(&1u64.to_le_bytes());
            v.extend_from_slice(&1u32.to_le_bytes());
            v.push(b'k');
            v.extend_from_slice(&[1, 1]);
            v
        });
    }
}
