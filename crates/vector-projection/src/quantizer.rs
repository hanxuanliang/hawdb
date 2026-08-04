use crate::error::{ProjectionError, Result};

pub(crate) const TURBOQUANT_LEVELS: usize = 16;
pub(crate) const TURBOQUANT_CODEBOOK_BYTES: usize =
    (TURBOQUANT_LEVELS + TURBOQUANT_LEVELS - 1) * std::mem::size_of::<f32>();

#[derive(Debug, Clone)]
pub(crate) struct TurboQuantCodebook {
    boundaries: [f32; TURBOQUANT_LEVELS - 1],
    centroids: [f32; TURBOQUANT_LEVELS],
}

impl TurboQuantCodebook {
    pub fn for_dimension(dimension: usize) -> Result<Self> {
        if dimension == 0 {
            return Err(ProjectionError::InvalidConfiguration(
                "TurboQuant codebook dimension must be greater than zero".to_string(),
            ));
        }

        let mut centroids = [0.0f64; TURBOQUANT_LEVELS];
        for (index, centroid) in centroids.iter_mut().enumerate() {
            let probability = (index as f64 + 0.5) / TURBOQUANT_LEVELS as f64;
            *centroid = inverse_normal_cdf(probability);
        }

        let mut boundaries = [0.0f64; TURBOQUANT_LEVELS - 1];
        for _ in 0..128 {
            for index in 0..boundaries.len() {
                boundaries[index] = (centroids[index] + centroids[index + 1]) * 0.5;
            }
            let mut max_delta = 0.0f64;
            for index in 0..centroids.len() {
                let lower = if index == 0 {
                    f64::NEG_INFINITY
                } else {
                    boundaries[index - 1]
                };
                let upper = if index == boundaries.len() {
                    f64::INFINITY
                } else {
                    boundaries[index]
                };
                let probability = normal_cdf(upper) - normal_cdf(lower);
                if probability <= f64::EPSILON {
                    continue;
                }
                let next = (normal_pdf(lower) - normal_pdf(upper)) / probability;
                max_delta = max_delta.max((centroids[index] - next).abs());
                centroids[index] = next;
            }
            if max_delta < 1e-12 {
                break;
            }
        }

        let scale = (dimension as f64).sqrt().recip();
        Ok(Self {
            boundaries: boundaries.map(|value| (value * scale) as f32),
            centroids: centroids.map(|value| (value * scale) as f32),
        })
    }

    #[inline]
    pub fn code(&self, value: f32) -> u8 {
        self.boundaries
            .partition_point(|boundary| value > *boundary) as u8
    }

    #[inline]
    pub fn centroid(&self, code: u8) -> f32 {
        self.centroids[usize::from(code)]
    }

    pub fn centroids(&self) -> &[f32; TURBOQUANT_LEVELS] {
        &self.centroids
    }

    pub fn zero_code(&self) -> u8 {
        self.centroids
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.abs().total_cmp(&right.abs()))
            .map(|(index, _)| index as u8)
            .expect("TurboQuant codebook is non-empty")
    }
}

fn normal_pdf(value: f64) -> f64 {
    if value.is_infinite() {
        return 0.0;
    }
    const INVERSE_SQRT_TWO_PI: f64 = 0.398_942_280_401_432_7;
    INVERSE_SQRT_TWO_PI * (-0.5 * value * value).exp()
}

fn normal_cdf(value: f64) -> f64 {
    if value == f64::NEG_INFINITY {
        return 0.0;
    }
    if value == f64::INFINITY {
        return 1.0;
    }
    let absolute = value.abs();
    let t = 1.0 / (1.0 + 0.231_641_9 * absolute);
    let polynomial = t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    let upper_tail = normal_pdf(absolute) * polynomial;
    if value >= 0.0 {
        1.0 - upper_tail
    } else {
        upper_tail
    }
}

fn inverse_normal_cdf(probability: f64) -> f64 {
    debug_assert!(probability > 0.0 && probability < 1.0);
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOWER: f64 = 0.024_25;
    const UPPER: f64 = 1.0 - LOWER;

    if probability < LOWER {
        let q = (-2.0 * probability.ln()).sqrt();
        return (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }
    if probability > UPPER {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        return -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }
    let q = probability - 0.5;
    let r = q * q;
    (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
        / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codebook_is_symmetric_and_monotonic() {
        let codebook = TurboQuantCodebook::for_dimension(1_536).unwrap();
        assert!(codebook.boundaries.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(codebook.centroids.windows(2).all(|pair| pair[0] < pair[1]));
        for index in 0..TURBOQUANT_LEVELS {
            let opposite = TURBOQUANT_LEVELS - 1 - index;
            assert!((codebook.centroids[index] + codebook.centroids[opposite]).abs() < 1e-6);
        }
    }

    #[test]
    fn code_assignment_spans_all_levels() {
        let codebook = TurboQuantCodebook::for_dimension(64).unwrap();
        assert_eq!(codebook.code(f32::NEG_INFINITY), 0);
        assert_eq!(codebook.code(f32::INFINITY), 15);
        assert!(matches!(codebook.zero_code(), 7 | 8));
    }
}
