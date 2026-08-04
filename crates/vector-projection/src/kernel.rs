use crate::codec::code_at;
use crate::error::{ProjectionError, Result};
#[cfg(test)]
use crate::quantizer::TurboQuantCodebook;
use crate::quantizer::TURBOQUANT_LEVELS;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanKernel {
    Scalar,
    Avx2,
    Neon,
}

impl ScanKernel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Neon => "neon",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KernelPreference {
    #[default]
    Auto,
    Scalar,
    Avx2,
    Neon,
}

pub(crate) fn select_kernel(preference: KernelPreference) -> Result<ScanKernel> {
    match preference {
        KernelPreference::Scalar => Ok(ScanKernel::Scalar),
        KernelPreference::Avx2 if avx2_available() => Ok(ScanKernel::Avx2),
        KernelPreference::Neon if neon_available() => Ok(ScanKernel::Neon),
        KernelPreference::Avx2 => Err(ProjectionError::UnsupportedKernel("avx2")),
        KernelPreference::Neon => Err(ProjectionError::UnsupportedKernel("neon")),
        KernelPreference::Auto => {
            if avx2_available() {
                Ok(ScanKernel::Avx2)
            } else if neon_available() {
                Ok(ScanKernel::Neon)
            } else {
                Ok(ScanKernel::Scalar)
            }
        }
    }
}

pub(crate) fn score_codes(
    kernel: ScanKernel,
    codes: &[u8],
    query: &[f32],
    centroids: &[f32; TURBOQUANT_LEVELS],
) -> f32 {
    match kernel {
        ScanKernel::Scalar => score_scalar(codes, query, centroids),
        #[cfg(target_arch = "x86_64")]
        ScanKernel::Avx2 => unsafe { score_avx2(codes, query, centroids) },
        #[cfg(not(target_arch = "x86_64"))]
        ScanKernel::Avx2 => unreachable!("AVX2 is selected only on x86_64"),
        #[cfg(target_arch = "aarch64")]
        ScanKernel::Neon => unsafe { score_neon(codes, query, centroids) },
        #[cfg(not(target_arch = "aarch64"))]
        ScanKernel::Neon => unreachable!("NEON is selected only on aarch64"),
    }
}

fn score_scalar(codes: &[u8], query: &[f32], centroids: &[f32; TURBOQUANT_LEVELS]) -> f32 {
    query
        .iter()
        .enumerate()
        .map(|(dimension, value)| *value * centroids[usize::from(code_at(codes, dimension))])
        .sum()
}

#[cfg(target_arch = "x86_64")]
fn avx2_available() -> bool {
    std::arch::is_x86_feature_detected!("avx2")
}

#[cfg(not(target_arch = "x86_64"))]
const fn avx2_available() -> bool {
    false
}

#[cfg(target_arch = "aarch64")]
const fn neon_available() -> bool {
    true
}

#[cfg(not(target_arch = "aarch64"))]
const fn neon_available() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn score_avx2(codes: &[u8], query: &[f32], centroids: &[f32; TURBOQUANT_LEVELS]) -> f32 {
    use std::arch::x86_64::*;

    let mut accumulator = _mm256_setzero_ps();
    let mask = _mm_set1_epi8(0x0f);
    let mut dimension = 0;
    let mut byte_offset = 0;
    while dimension + 32 <= query.len() {
        let packed = unsafe { _mm_loadu_si128(codes.as_ptr().add(byte_offset).cast()) };
        let low = _mm_and_si128(packed, mask);
        let high = _mm_and_si128(_mm_srli_epi16(packed, 4), mask);
        let first = _mm_unpacklo_epi8(low, high);
        let second = _mm_unpackhi_epi8(low, high);
        for (chunk, source) in [first, second].into_iter().enumerate() {
            let source_low = source;
            let source_high = _mm_srli_si128(source, 8);
            for (half, bytes) in [source_low, source_high].into_iter().enumerate() {
                let integers = _mm256_cvtepu8_epi32(bytes);
                let centers = unsafe { _mm256_i32gather_ps(centroids.as_ptr(), integers, 4) };
                let query_offset = dimension + chunk * 16 + half * 8;
                let query_values = unsafe { _mm256_loadu_ps(query.as_ptr().add(query_offset)) };
                accumulator = _mm256_add_ps(accumulator, _mm256_mul_ps(centers, query_values));
            }
        }
        dimension += 32;
        byte_offset += 16;
    }

    let mut lanes = [0.0f32; 8];
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), accumulator) };
    let mut score = lanes.into_iter().sum::<f32>();
    for (index, value) in query.iter().copied().enumerate().skip(dimension) {
        score += value * centroids[usize::from(code_at(codes, index))];
    }
    score
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn score_neon(codes: &[u8], query: &[f32], centroids: &[f32; TURBOQUANT_LEVELS]) -> f32 {
    use std::arch::aarch64::*;

    let mask = vdupq_n_u8(0x0f);
    let mut accumulator = vdupq_n_f32(0.0);
    let mut dimension = 0;
    let mut byte_offset = 0;
    while dimension + 32 <= query.len() {
        let packed = unsafe { vld1q_u8(codes.as_ptr().add(byte_offset)) };
        let low = vandq_u8(packed, mask);
        let high = vandq_u8(vshrq_n_u8(packed, 4), mask);
        let zipped = vzipq_u8(low, high);
        for (chunk, source) in [zipped.0, zipped.1].into_iter().enumerate() {
            let widened_low = vmovl_u8(vget_low_u8(source));
            let widened_high = vmovl_u8(vget_high_u8(source));
            for (half, values) in [widened_low, widened_high].into_iter().enumerate() {
                let mut indexes = [0u16; 8];
                unsafe { vst1q_u16(indexes.as_mut_ptr(), values) };
                let centers = indexes.map(|index| centroids[usize::from(index)]);
                let lower = unsafe { vld1q_f32(centers.as_ptr()) };
                let upper = unsafe { vld1q_f32(centers.as_ptr().add(4)) };
                let query_offset = dimension + chunk * 16 + half * 8;
                let query_lower = unsafe { vld1q_f32(query.as_ptr().add(query_offset)) };
                let query_upper = unsafe { vld1q_f32(query.as_ptr().add(query_offset + 4)) };
                accumulator = vaddq_f32(accumulator, vmulq_f32(lower, query_lower));
                accumulator = vaddq_f32(accumulator, vmulq_f32(upper, query_upper));
            }
        }
        dimension += 32;
        byte_offset += 16;
    }

    let mut score = vaddvq_f32(accumulator);
    for (index, value) in query.iter().copied().enumerate().skip(dimension) {
        score += value * centroids[usize::from(code_at(codes, index))];
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_kernel_matches_scalar_reference() {
        let dimension: usize = 67;
        let codes = (0..dimension.div_ceil(2))
            .map(|index| ((index * 29 + 17) & 0xff) as u8)
            .collect::<Vec<_>>();
        let query = (0..dimension)
            .map(|index| (index as f32 * 0.17).sin())
            .collect::<Vec<_>>();
        let codebook = TurboQuantCodebook::for_dimension(dimension).unwrap();
        let scalar = score_codes(ScanKernel::Scalar, &codes, &query, codebook.centroids());
        let selected = select_kernel(KernelPreference::Auto).unwrap();
        let accelerated = score_codes(selected, &codes, &query, codebook.centroids());
        assert!(
            (scalar - accelerated).abs() < 1e-4,
            "{scalar} != {accelerated}"
        );
    }
}
