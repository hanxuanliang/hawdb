use super::*;

pub(super) fn encode_row(
    row: &RelationalRow,
    column_count: usize,
    limits: RelationalRowPageLimits,
) -> Result<Vec<u8>, RelationalRowPageError> {
    if row.values().len() != column_count {
        return Err(RelationalRowPageError::Admission(format!(
            "row contains {} values, expected {column_count}",
            row.values().len()
        )));
    }
    let directory_len = column_count.checked_mul(VALUE_SLOT_BYTES).ok_or_else(|| {
        RelationalRowPageError::Admission("value slot directory length overflow".to_string())
    })?;
    let header_len = 4usize.checked_add(directory_len).ok_or_else(|| {
        RelationalRowPageError::Admission("encoded row header length overflow".to_string())
    })?;
    if header_len > limits.max_row_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "row directory contains {header_len} bytes, exceeding row limit {}",
            limits.max_row_bytes
        )));
    }
    let mut slots = Vec::with_capacity(column_count);
    let mut payload = Vec::new();
    for value in row.values() {
        let offset = u32_len(payload.len(), "value offset")?;
        let before = payload.len();
        encode_value(&mut payload, value, limits)?;
        let length = payload.len().checked_sub(before).ok_or_else(|| {
            RelationalRowPageError::Admission("value length underflow".to_string())
        })?;
        slots.push((offset, u32_len(length, "value length")?));
        let projected_len = header_len.checked_add(payload.len()).ok_or_else(|| {
            RelationalRowPageError::Admission("encoded row length overflow".to_string())
        })?;
        if projected_len > limits.max_row_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded row would contain {projected_len} bytes, exceeding limit {}",
                limits.max_row_bytes
            )));
        }
    }
    let mut encoded = Vec::with_capacity(header_len + payload.len());
    encoded.extend_from_slice(&u32_len(column_count, "row value count")?.to_le_bytes());
    for (offset, length) in slots {
        encoded.extend_from_slice(&offset.to_le_bytes());
        encoded.extend_from_slice(&length.to_le_bytes());
    }
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn encode_value(
    encoded: &mut Vec<u8>,
    value: &RelationalValue,
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    match value {
        RelationalValue::Null => encoded.push(0),
        RelationalValue::Boolean(value) => {
            encoded.push(1);
            encoded.push(u8::from(*value));
        }
        RelationalValue::BigInt(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        RelationalValue::Text(value) => {
            validate_inline_value_len(value.len(), limits)?;
            encoded.push(4);
            encoded.extend_from_slice(&u32_len(value.len(), "TEXT length")?.to_le_bytes());
            encoded.extend_from_slice(value.as_bytes());
        }
        RelationalValue::Bytea(value) => {
            validate_inline_value_len(value.len(), limits)?;
            encoded.push(5);
            encoded.extend_from_slice(&u32_len(value.len(), "BYTEA length")?.to_le_bytes());
            encoded.extend_from_slice(value);
        }
        RelationalValue::Overflow(reference) => {
            validate_overflow_shape(reference, limits, ErrorClass::Admission)?;
            let digest: Sha256Digest = reference.digest.parse().map_err(|error| {
                RelationalRowPageError::Admission(format!(
                    "overflow digest is not canonical SHA-256: {error}"
                ))
            })?;
            encoded.push(6);
            encoded.push(scalar_type_tag(reference.scalar_type));
            encoded.extend_from_slice(
                &u64_len(reference.compressed_bytes, "overflow compressed length")?.to_le_bytes(),
            );
            encoded.extend_from_slice(
                &u64_len(reference.uncompressed_bytes, "overflow uncompressed length")?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(digest.as_bytes());
        }
    }
    Ok(())
}

pub(super) fn decode_row_fields(
    encoded: &[u8],
    column_count: usize,
    requested_fields: Option<&[usize]>,
    limits: RelationalRowPageLimits,
) -> Result<Vec<RelationalProjectedField>, RelationalRowPageError> {
    if encoded.len() < 4 {
        return Err(RelationalRowPageError::Corrupt(
            "row is smaller than its value-count header".to_string(),
        ));
    }
    let declared_columns = read_u32(&encoded[..4]) as usize;
    if declared_columns != column_count {
        return Err(RelationalRowPageError::Corrupt(format!(
            "row declares {declared_columns} values, expected {column_count}"
        )));
    }
    let directory_len = column_count.checked_mul(VALUE_SLOT_BYTES).ok_or_else(|| {
        RelationalRowPageError::Corrupt("value slot directory length overflow".to_string())
    })?;
    let payload_start = 4usize.checked_add(directory_len).ok_or_else(|| {
        RelationalRowPageError::Corrupt("row payload offset overflow".to_string())
    })?;
    if payload_start > encoded.len() {
        return Err(RelationalRowPageError::Corrupt(
            "row value directory exceeds its payload".to_string(),
        ));
    }
    let directory = &encoded[4..payload_start];
    let payload = &encoded[payload_start..];
    let mut fields = Vec::with_capacity(requested_fields.map_or(column_count, <[usize]>::len));
    let mut requested_index = 0usize;
    let mut next_offset = 0usize;
    for ordinal in 0..column_count {
        let slot_offset = ordinal * VALUE_SLOT_BYTES;
        let value_offset = read_u32(&directory[slot_offset..slot_offset + 4]);
        let value_len = read_u32(&directory[slot_offset + 4..slot_offset + 8]);
        if value_offset as usize != next_offset {
            return Err(RelationalRowPageError::Corrupt(format!(
                "column {ordinal} value offset is not contiguous"
            )));
        }
        let value = bounded_slice(payload, value_offset, value_len, "row value")?;
        validate_value(value, limits)?;
        if requested_fields.is_none()
            || requested_fields
                .and_then(|fields| fields.get(requested_index))
                .copied()
                == Some(ordinal)
        {
            fields.push(RelationalProjectedField {
                ordinal,
                value: decode_validated_value(value)?,
            });
            if requested_fields.is_some() {
                requested_index += 1;
            }
        }
        next_offset = next_offset.checked_add(value.len()).ok_or_else(|| {
            RelationalRowPageError::Corrupt("row value offset overflow".to_string())
        })?;
    }
    if next_offset != payload.len() {
        return Err(RelationalRowPageError::Corrupt(
            "value slot directory does not cover the exact row payload".to_string(),
        ));
    }
    Ok(fields)
}

fn validate_value(
    encoded: &[u8],
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    let Some(tag) = encoded.first().copied() else {
        return Err(RelationalRowPageError::Corrupt(
            "encoded value is empty".to_string(),
        ));
    };
    match tag {
        0 if encoded.len() == 1 => Ok(()),
        1 if encoded.len() == 2 && matches!(encoded[1], 0 | 1) => Ok(()),
        2 | 3 if encoded.len() == 9 => Ok(()),
        4 | 5 => {
            if encoded.len() < 5 {
                return Err(RelationalRowPageError::Corrupt(
                    "inline value is smaller than its length header".to_string(),
                ));
            }
            let length = read_u32(&encoded[1..5]) as usize;
            validate_inline_value_len_decode(length, limits)?;
            if encoded.len() != 5usize.saturating_add(length) {
                return Err(RelationalRowPageError::Corrupt(
                    "inline value length does not match its slot".to_string(),
                ));
            }
            if tag == 4 {
                std::str::from_utf8(&encoded[5..]).map_err(|error| {
                    RelationalRowPageError::Corrupt(format!(
                        "inline TEXT value is not valid UTF-8: {error}"
                    ))
                })?;
            }
            Ok(())
        }
        6 => {
            if encoded.len() != 50 {
                return Err(RelationalRowPageError::Corrupt(format!(
                    "overflow descriptor contains {} bytes, expected 50",
                    encoded.len()
                )));
            }
            let scalar_type = scalar_type_from_tag(encoded[1])?;
            let reference = RelationalOverflowRef {
                digest: Sha256Digest::from_bytes(
                    encoded[18..50]
                        .try_into()
                        .expect("overflow digest has a fixed length"),
                )
                .to_string(),
                scalar_type,
                compressed_bytes: usize_from_u64(
                    read_u64(&encoded[2..10]),
                    "overflow compressed length",
                )?,
                uncompressed_bytes: usize_from_u64(
                    read_u64(&encoded[10..18]),
                    "overflow uncompressed length",
                )?,
            };
            validate_overflow_shape(&reference, limits, ErrorClass::Corrupt)
        }
        1 => Err(RelationalRowPageError::Corrupt(
            "invalid BOOLEAN value encoding".to_string(),
        )),
        0 | 2 | 3 => Err(RelationalRowPageError::Corrupt(format!(
            "value tag {tag} has an invalid encoded length {}",
            encoded.len()
        ))),
        tag => Err(RelationalRowPageError::Corrupt(format!(
            "invalid relational row value tag {tag}"
        ))),
    }
}

fn decode_validated_value(encoded: &[u8]) -> Result<RelationalValue, RelationalRowPageError> {
    match encoded[0] {
        0 => Ok(RelationalValue::Null),
        1 => Ok(RelationalValue::Boolean(encoded[1] == 1)),
        2 => Ok(RelationalValue::BigInt(i64::from_le_bytes(
            encoded[1..9]
                .try_into()
                .expect("BIGINT value has a fixed length"),
        ))),
        3 => Ok(RelationalValue::DoublePrecision(f64::from_bits(read_u64(
            &encoded[1..9],
        )))),
        4 => Ok(RelationalValue::Text(
            std::str::from_utf8(&encoded[5..])
                .expect("validated TEXT value is UTF-8")
                .to_string(),
        )),
        5 => Ok(RelationalValue::Bytea(encoded[5..].to_vec())),
        6 => Ok(RelationalValue::Overflow(RelationalOverflowRef {
            digest: Sha256Digest::from_bytes(
                encoded[18..50]
                    .try_into()
                    .expect("overflow digest has a fixed length"),
            )
            .to_string(),
            scalar_type: scalar_type_from_tag(encoded[1])?,
            compressed_bytes: usize_from_u64(
                read_u64(&encoded[2..10]),
                "overflow compressed length",
            )?,
            uncompressed_bytes: usize_from_u64(
                read_u64(&encoded[10..18]),
                "overflow uncompressed length",
            )?,
        })),
        _ => unreachable!("validated row value tag"),
    }
}

pub(super) fn validate_requested_fields(
    requested_fields: &[usize],
    column_count: usize,
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    if requested_fields.len() > limits.max_requested_fields.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "requested {} fields, exceeding limit {}",
            requested_fields.len(),
            limits.max_requested_fields
        )));
    }
    let mut previous = None;
    for field in requested_fields {
        if *field >= column_count {
            return Err(RelationalRowPageError::Admission(format!(
                "requested field {field} is outside column count {column_count}"
            )));
        }
        if previous.is_some_and(|previous| previous >= *field) {
            return Err(RelationalRowPageError::Admission(
                "requested fields must be strictly increasing".to_string(),
            ));
        }
        previous = Some(*field);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

fn validate_overflow_shape(
    reference: &RelationalOverflowRef,
    limits: RelationalRowPageLimits,
    error_class: ErrorClass,
) -> Result<(), RelationalRowPageError> {
    if !matches!(
        reference.scalar_type,
        RelationalScalarType::Text | RelationalScalarType::Bytea
    ) {
        return Err(classify(
            error_class,
            "overflow descriptor has a non-payload scalar type".to_string(),
        ));
    }
    if reference.compressed_bytes > limits.max_value_bytes.get()
        || reference.uncompressed_bytes > limits.max_value_bytes.get()
    {
        return Err(classify(
            error_class,
            format!(
                "overflow descriptor contains {} compressed and {} uncompressed bytes, exceeding value limit {}",
                reference.compressed_bytes,
                reference.uncompressed_bytes,
                limits.max_value_bytes
            ),
        ));
    }
    Ok(())
}

fn validate_inline_value_len(
    length: usize,
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    if length > limits.max_inline_value_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "inline value contains {length} bytes, exceeding limit {}",
            limits.max_inline_value_bytes
        )));
    }
    Ok(())
}

fn validate_inline_value_len_decode(
    length: usize,
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    if length > limits.max_inline_value_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "inline value declares {length} bytes, exceeding limit {}",
            limits.max_inline_value_bytes
        )));
    }
    Ok(())
}

fn scalar_type_tag(scalar_type: RelationalScalarType) -> u8 {
    match scalar_type {
        RelationalScalarType::Boolean => 1,
        RelationalScalarType::BigInt => 2,
        RelationalScalarType::DoublePrecision => 3,
        RelationalScalarType::Text => 4,
        RelationalScalarType::Bytea => 5,
    }
}

fn scalar_type_from_tag(tag: u8) -> Result<RelationalScalarType, RelationalRowPageError> {
    match tag {
        4 => Ok(RelationalScalarType::Text),
        5 => Ok(RelationalScalarType::Bytea),
        tag => Err(RelationalRowPageError::Corrupt(format!(
            "invalid overflow scalar type tag {tag}"
        ))),
    }
}

fn classify(error_class: ErrorClass, message: String) -> RelationalRowPageError {
    match error_class {
        ErrorClass::Admission => RelationalRowPageError::Admission(message),
        ErrorClass::Corrupt => RelationalRowPageError::Corrupt(message),
    }
}
