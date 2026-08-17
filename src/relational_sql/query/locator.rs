use crate::error::{Result, SkeinError};
use crate::value::Value;
use skein_storage::{RelationalKey, RelationalScalarType, RelationalTableSchema, RelationalValue};

pub(super) struct RelationalLocatorLayout<'a> {
    pub(super) bindings: Vec<RelationalLocatorBindingLayout<'a>>,
}

pub(super) struct RelationalLocatorBindingLayout<'a> {
    pub(super) table: &'a str,
    pub(super) qualifier: &'a str,
    pub(super) schema: &'a RelationalTableSchema,
    primary_key_types: Vec<RelationalScalarType>,
}

impl<'a> RelationalLocatorLayout<'a> {
    pub(super) fn from_bindings(
        bindings: impl IntoIterator<Item = (&'a str, &'a str, &'a RelationalTableSchema)>,
    ) -> Result<Self> {
        bindings
            .into_iter()
            .map(|(table, qualifier, schema)| {
                RelationalLocatorBindingLayout::new(table, qualifier, schema)
            })
            .collect::<Result<Vec<_>>>()
            .map(|bindings| Self { bindings })
    }
}

impl<'a> RelationalLocatorBindingLayout<'a> {
    fn new(table: &'a str, qualifier: &'a str, schema: &'a RelationalTableSchema) -> Result<Self> {
        if schema.primary_key.is_empty() {
            return Err(SkeinError::Storage(format!(
                "relational locator layout requires a primary key for table {table}"
            )));
        }
        let primary_key_types = schema
            .primary_key
            .iter()
            .map(|column| {
                schema
                    .column_position(column)
                    .map(|position| schema.columns[position].scalar_type)
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relational locator layout references unknown primary-key column {column} in table {table}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            table,
            qualifier,
            schema,
            primary_key_types,
        })
    }
}

pub(super) fn encode_locator_key(key: &RelationalKey) -> Result<Value> {
    key.0
        .iter()
        .map(|value| match value {
            RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
            RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
            RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
            RelationalValue::Text(value) => Ok(Value::String(value.clone())),
            RelationalValue::Bytea(value) => Ok(Value::String(hex_encode(value))),
            RelationalValue::Null => Err(SkeinError::Storage(
                "relational locator contains a null primary-key value".to_string(),
            )),
            RelationalValue::Overflow(_) => Err(SkeinError::Storage(
                "relational locator contains an externalized primary-key value".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::List)
}

pub(super) fn decode_locator(
    value: &Value,
    locator_layout: &RelationalLocatorLayout<'_>,
) -> Result<Vec<Option<RelationalKey>>> {
    let Value::List(bindings) = value else {
        return Err(SkeinError::Execution(
            "relational spill locator is not a list".to_string(),
        ));
    };
    if bindings.len() != locator_layout.bindings.len() {
        return Err(SkeinError::Execution(format!(
            "relational spill locator has {} bindings but the query layout requires {}",
            bindings.len(),
            locator_layout.bindings.len()
        )));
    }
    bindings
        .iter()
        .zip(&locator_layout.bindings)
        .map(|(binding, layout)| match binding {
            Value::Null => Ok(None),
            Value::List(values) => decode_locator_key(values, layout).map(Some),
            _ => Err(SkeinError::Execution(
                "relational spill binding locator is neither null nor a primary-key list"
                    .to_string(),
            )),
        })
        .collect()
}

fn decode_locator_key(
    values: &[Value],
    layout: &RelationalLocatorBindingLayout<'_>,
) -> Result<RelationalKey> {
    if values.len() != layout.primary_key_types.len() {
        return Err(SkeinError::Execution(format!(
            "relational spill locator for table {} has {} key values but the schema requires {}",
            layout.table,
            values.len(),
            layout.primary_key_types.len()
        )));
    }
    values
        .iter()
        .zip(&layout.primary_key_types)
        .map(|(value, scalar_type)| decode_locator_value(value, *scalar_type, layout.table))
        .collect::<Result<Vec<_>>>()
        .map(RelationalKey)
}

fn decode_locator_value(
    value: &Value,
    scalar_type: RelationalScalarType,
    table: &str,
) -> Result<RelationalValue> {
    match (scalar_type, value) {
        (RelationalScalarType::Boolean, Value::Bool(value)) => {
            Ok(RelationalValue::Boolean(*value))
        }
        (RelationalScalarType::BigInt, Value::Int(value)) => Ok(RelationalValue::BigInt(*value)),
        (RelationalScalarType::DoublePrecision, Value::Float(value)) => {
            Ok(RelationalValue::DoublePrecision(*value))
        }
        (RelationalScalarType::Text, Value::String(value)) => {
            Ok(RelationalValue::Text(value.clone()))
        }
        (RelationalScalarType::Bytea, Value::String(value)) => {
            Ok(RelationalValue::Bytea(hex_decode(value)?))
        }
        _ => Err(SkeinError::Execution(format!(
            "relational spill locator key for table {table} does not match schema type {scalar_type:?}"
        ))),
    }
}

pub(super) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(SkeinError::Execution(
            "relational spill byte string has an odd length".to_string(),
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = hex_digit(digits[0])?;
            let low = hex_digit(digits[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(SkeinError::Execution(
            "relational spill byte string contains invalid hex".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_executor::binding::value_memory_bytes;
    use skein_storage::RelationalColumnSchema;
    use std::collections::BTreeMap;

    #[test]
    fn positional_locator_round_trips_schema_typed_composite_keys() {
        let primary_schema = locator_schema(
            "records",
            &[
                ("active", RelationalScalarType::Boolean),
                ("sequence", RelationalScalarType::BigInt),
                ("score", RelationalScalarType::DoublePrecision),
                ("name", RelationalScalarType::Text),
                ("digest", RelationalScalarType::Bytea),
            ],
        );
        let optional_schema =
            locator_schema("optional_records", &[("id", RelationalScalarType::Text)]);
        let layout = RelationalLocatorLayout::from_bindings([
            ("records", "r", &primary_schema),
            ("optional_records", "optional", &optional_schema),
        ])
        .expect("locator layout");
        let key = RelationalKey(vec![
            RelationalValue::Boolean(true),
            RelationalValue::BigInt(42),
            RelationalValue::DoublePrecision(3.5),
            RelationalValue::Text("record-42".to_string()),
            RelationalValue::Bytea(vec![0, 1, 127, 255]),
        ]);
        let locator = Value::List(vec![
            encode_locator_key(&key).expect("encode compact locator key"),
            Value::Null,
        ]);

        assert!(!contains_map(&locator));
        assert!(!contains_string(&locator, "records"));
        assert!(!contains_string(&locator, "optional"));
        assert!(
            value_memory_bytes(&locator) < value_memory_bytes(&legacy_locator_value(&key)),
            "positional locator must retain fewer estimated bytes than self-describing rows"
        );
        assert_eq!(
            decode_locator(&locator, &layout).expect("decode compact locator"),
            vec![Some(key), None]
        );
    }

    #[test]
    fn positional_locator_rejects_layout_and_type_drift() {
        let schema = locator_schema("records", &[("id", RelationalScalarType::BigInt)]);
        let layout = RelationalLocatorLayout::from_bindings([("records", "r", &schema)])
            .expect("locator layout");

        let error = decode_locator(&Value::List(Vec::new()), &layout)
            .expect_err("binding count drift must fail closed");
        assert!(error.to_string().contains("requires 1"));

        let error = decode_locator(
            &Value::List(vec![Value::List(vec![Value::String(
                "wrong-type".to_string(),
            )])]),
            &layout,
        )
        .expect_err("key type drift must fail closed");
        assert!(error
            .to_string()
            .contains("does not match schema type BigInt"));
    }

    fn locator_schema(
        table: &str,
        columns: &[(&str, RelationalScalarType)],
    ) -> RelationalTableSchema {
        RelationalTableSchema {
            name: table.to_string(),
            columns: columns
                .iter()
                .map(|(name, scalar_type)| RelationalColumnSchema {
                    name: (*name).to_string(),
                    scalar_type: *scalar_type,
                    nullable: false,
                    default: None,
                })
                .collect(),
            primary_key: columns
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn legacy_locator_value(key: &RelationalKey) -> Value {
        Value::List(vec![
            Value::Map(BTreeMap::from([
                ("table".to_string(), Value::String("records".to_string())),
                ("qualifier".to_string(), Value::String("r".to_string())),
                (
                    "key".to_string(),
                    Value::List(key.0.iter().map(legacy_key_value).collect()),
                ),
            ])),
            Value::Map(BTreeMap::from([
                (
                    "table".to_string(),
                    Value::String("optional_records".to_string()),
                ),
                (
                    "qualifier".to_string(),
                    Value::String("optional".to_string()),
                ),
                ("key".to_string(), Value::Null),
            ])),
        ])
    }

    fn legacy_key_value(value: &RelationalValue) -> Value {
        let (kind, value) = match value {
            RelationalValue::Boolean(value) => ("bool", Value::Bool(*value)),
            RelationalValue::BigInt(value) => ("int", Value::Int(*value)),
            RelationalValue::DoublePrecision(value) => ("float", Value::Float(*value)),
            RelationalValue::Text(value) => ("text", Value::String(value.clone())),
            RelationalValue::Bytea(value) => ("bytea", Value::String(hex_encode(value))),
            RelationalValue::Null | RelationalValue::Overflow(_) => {
                panic!("test key must be inline and non-null")
            }
        };
        Value::Map(BTreeMap::from([
            ("kind".to_string(), Value::String(kind.to_string())),
            ("value".to_string(), value),
        ]))
    }

    fn contains_map(value: &Value) -> bool {
        match value {
            Value::Map(_) => true,
            Value::List(values) => values.iter().any(contains_map),
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::String(_) => {
                false
            }
        }
    }

    fn contains_string(value: &Value, expected: &str) -> bool {
        match value {
            Value::String(value) => value == expected,
            Value::List(values) => values.iter().any(|value| contains_string(value, expected)),
            Value::Map(values) => values
                .iter()
                .any(|(name, value)| name == expected || contains_string(value, expected)),
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => false,
        }
    }
}
