mod artifact;
mod build;
mod codec;
mod error;
mod kernel;
mod model;
mod quantizer;
mod scan;
mod transform;

pub use artifact::{FileProjection, ProjectionWriter};
pub use build::{source_digest, ProjectionBuilder};
pub use error::{ProjectionError, Result};
pub use kernel::{KernelPreference, ScanKernel};
pub use model::{
    InMemoryProjection, ProjectionBuildAdmission, ProjectionBuildConfig, ProjectionBuildReport,
    ProjectionIdentity, ProjectionManifest, ProjectionMetric, SegmentDescriptor,
    DEFAULT_BUILD_MEMORY_BYTES, DEFAULT_SEGMENT_ROWS, DEFAULT_TRANSFORM_SEED, PROJECTION_ALGORITHM,
    PROJECTION_BIT_WIDTH, PROJECTION_CALIBRATION, PROJECTION_FORMAT_VERSION, PROJECTION_PROTOCOL,
    PROJECTION_QUANTIZER, PROJECTION_TRANSFORM,
};
pub use scan::{
    ProjectionHit, ProjectionSearchOptions, ProjectionSearchOutput, ProjectionSearchReport,
};
