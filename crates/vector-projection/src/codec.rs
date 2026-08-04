use crate::error::{ProjectionError, Result};
use crate::quantizer::TurboQuantCodebook;
use crate::transform::normalize_and_transform;

pub(crate) fn bytes_per_vector(dimension: usize) -> usize {
    dimension.div_ceil(2)
}

pub(crate) fn encode_vector(
    vector: &[f32],
    transform_seed: u64,
    codebook: &TurboQuantCodebook,
    transformed: &mut [f32],
    packed: &mut Vec<u8>,
) -> Result<f32> {
    normalize_and_transform(vector, transform_seed, transformed)?;
    let encoded_len = bytes_per_vector(transformed.len());
    packed.clear();
    packed.resize(encoded_len, 0);

    let max_absolute = transformed
        .iter()
        .copied()
        .map(f32::abs)
        .fold(0.0f32, f32::max);
    if max_absolute <= f32::EPSILON {
        for dimension in 0..transformed.len() {
            set_code(packed, dimension, codebook.zero_code());
        }
        return Ok(0.0);
    }

    let mut alignment = 0.0f64;
    for (dimension, value) in transformed.iter().copied().enumerate() {
        let code = codebook.code(value);
        set_code(packed, dimension, code);
        alignment += f64::from(value) * f64::from(codebook.centroid(code));
    }
    if !alignment.is_finite() || alignment <= f64::EPSILON {
        return Err(ProjectionError::InvalidVector(
            "quantized reconstruction has no positive alignment".to_string(),
        ));
    }
    Ok(alignment.recip() as f32)
}

#[inline]
pub(crate) fn code_at(packed: &[u8], dimension: usize) -> u8 {
    let byte = packed[dimension / 2];
    if dimension & 1 == 0 {
        byte & 0x0f
    } else {
        byte >> 4
    }
}

fn set_code(packed: &mut [u8], dimension: usize, code: u8) {
    let slot = &mut packed[dimension / 2];
    if dimension & 1 == 0 {
        *slot = (*slot & 0xf0) | (code & 0x0f);
    } else {
        *slot = (*slot & 0x0f) | ((code & 0x0f) << 4);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn odd_dimensions_round_trip_nibbles() {
        let mut packed = vec![0; bytes_per_vector(5)];
        for (dimension, code) in [1, 3, 5, 7, 9].into_iter().enumerate() {
            set_code(&mut packed, dimension, code);
        }
        assert_eq!(
            (0..5)
                .map(|dimension| code_at(&packed, dimension))
                .collect::<Vec<_>>(),
            vec![1, 3, 5, 7, 9]
        );
    }
}
