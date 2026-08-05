use super::{
    elapsed_micros, execute_out_of_core, ProductionSearchLifecycleConfig,
    ProductionSearchLifecycleReport, ProductionSearchQualificationError,
};
use skein::{
    SearchIndex, SearchOutOfCoreConfig, SearchOutOfCoreReader, SearchProjectionDelta,
    SearchProjectionQualificationIdentity, SearchResultSet,
};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const OUT_OF_CORE_MANIFEST_FILE: &str = "search_projection.out_of_core.manifest.skein";

pub(super) fn run_lifecycle_probes(
    config: &ProductionSearchLifecycleConfig,
    expected_projection_identity: &SearchProjectionQualificationIdentity,
    out_of_core_config: &SearchOutOfCoreConfig,
) -> Result<ProductionSearchLifecycleReport, ProductionSearchQualificationError> {
    let mut update_micros = Vec::with_capacity(config.replica_paths.len());
    let mut checkpoint_micros = Vec::with_capacity(config.replica_paths.len());
    let mut reopen_micros = Vec::with_capacity(config.replica_paths.len());
    let mut incremental_upsert_delete = true;
    let mut checkpoint_reopen = true;
    let mut stale_generation = true;
    let mut mixed_foreground_background = true;
    let mut checkpoint_write_amplification_per_million = 0;
    let logical_delta_bytes = delta_logical_bytes(&config.delta);

    for path in &config.replica_paths {
        let old_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        require_projection_identity(&old_reader, expected_projection_identity)?;
        let old_generation = old_reader.generation();
        let upsert_before = execute_out_of_core(&old_reader, &config.upsert_verification)?;
        let delete_before = execute_out_of_core(&old_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            !contains_hit(&upsert_before.result, &config.expected_upsert_document_id)
                && contains_hit(&delete_before.result, &config.expected_deleted_document_id);

        let bytes_before = directory_regular_file_bytes(path)?;
        let mut index =
            SearchIndex::open(path).map_err(ProductionSearchQualificationError::from_error)?;
        let update_started = Instant::now();
        index
            .apply_projection_delta(config.delta.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        update_micros.push(elapsed_micros(update_started));

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker_case = config.upsert_verification.clone();
        let worker_runs = config.mixed_load_probe_runs;
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            let mut succeeded = true;
            for _ in 0..worker_runs {
                succeeded &= execute_out_of_core(&old_reader, &worker_case).is_ok();
            }
            succeeded
        });
        barrier.wait();
        let checkpoint_started = Instant::now();
        index
            .checkpoint()
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_micros.push(elapsed_micros(checkpoint_started));
        mixed_foreground_background &= worker.join().map_err(|_| {
            ProductionSearchQualificationError::new(
                "production search mixed-load probe thread panicked",
            )
        })?;
        drop(index);

        let bytes_after = directory_regular_file_bytes(path)?;
        checkpoint_write_amplification_per_million = checkpoint_write_amplification_per_million
            .max(ratio_per_million(
                bytes_after.saturating_sub(bytes_before),
                logical_delta_bytes,
            ));
        let reopen_started = Instant::now();
        let new_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        reopen_micros.push(elapsed_micros(reopen_started));
        stale_generation &= new_reader.generation() > old_generation;
        let upsert_after = execute_out_of_core(&new_reader, &config.upsert_verification)?;
        let delete_after = execute_out_of_core(&new_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            contains_hit(&upsert_after.result, &config.expected_upsert_document_id)
                && !contains_hit(&delete_after.result, &config.expected_deleted_document_id);
        let expected_upsert_digest = super::result_digest(&upsert_after.result);
        let expected_delete_digest = super::result_digest(&delete_after.result);
        drop(new_reader);
        let reopened = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_reopen &= super::result_digest(
            &execute_out_of_core(&reopened, &config.upsert_verification)?.result,
        ) == expected_upsert_digest
            && super::result_digest(
                &execute_out_of_core(&reopened, &config.delete_verification)?.result,
            ) == expected_delete_digest;
    }

    let corrupt_reader = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .map_err(ProductionSearchQualificationError::from_error)?;
    require_projection_identity(&corrupt_reader, expected_projection_identity)?;
    drop(corrupt_reader);
    corrupt_out_of_core_manifest(&config.corruption_replica_path)?;
    let corrupt_artifact_rejected = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .is_err();

    Ok(ProductionSearchLifecycleReport {
        incremental_upsert_delete,
        checkpoint_reopen,
        stale_generation,
        corrupt_artifact_rejected,
        mixed_foreground_background,
        update_latency: crate::latency_percentiles(&update_micros),
        checkpoint_latency: crate::latency_percentiles(&checkpoint_micros),
        reopen_latency: crate::latency_percentiles(&reopen_micros),
        checkpoint_write_amplification_per_million,
    })
}

fn require_projection_identity(
    reader: &SearchOutOfCoreReader,
    expected: &SearchProjectionQualificationIdentity,
) -> Result<(), ProductionSearchQualificationError> {
    if reader.production_qualification_identity() == *expected {
        Ok(())
    } else {
        Err(ProductionSearchQualificationError::new(
            "production search lifecycle replica identity does not match the source projection",
        ))
    }
}

fn corrupt_out_of_core_manifest(path: &Path) -> Result<(), ProductionSearchQualificationError> {
    let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest_path)
        .map_err(ProductionSearchQualificationError::from_error)?;
    let length = file
        .metadata()
        .map_err(ProductionSearchQualificationError::from_error)?
        .len();
    if length == 0 {
        return Err(ProductionSearchQualificationError::new(
            "production search corruption replica has an empty manifest",
        ));
    }
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)
        .map_err(ProductionSearchQualificationError::from_error)?;
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    byte[0] ^= 0xff;
    file.write_all(&byte)
        .and_then(|_| file.sync_all())
        .map_err(ProductionSearchQualificationError::from_error)
}

fn contains_hit(result: &SearchResultSet, document_id: &str) -> bool {
    result.hits.iter().any(|hit| hit.id == document_id)
}

fn delta_logical_bytes(delta: &SearchProjectionDelta) -> u64 {
    let upserts = delta.upserts.iter().fold(0u64, |bytes, row| {
        bytes
            .saturating_add(row.external_id.len() as u64)
            .saturating_add(row.title.len() as u64)
            .saturating_add(row.body.len() as u64)
            .saturating_add(
                row.embedding
                    .as_ref()
                    .map(|embedding| {
                        (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
                    })
                    .unwrap_or_default(),
            )
            .saturating_add(
                row.metadata
                    .iter()
                    .map(|(name, value)| (name.len() + value.len()) as u64)
                    .fold(0u64, u64::saturating_add),
            )
    });
    delta
        .deletes
        .iter()
        .map(|id| id.len() as u64)
        .fold(upserts, u64::saturating_add)
        .max(1)
}

fn directory_regular_file_bytes(path: &Path) -> Result<u64, ProductionSearchQualificationError> {
    let mut total = 0u64;
    let entries =
        std::fs::read_dir(path).map_err(ProductionSearchQualificationError::from_error)?;
    for entry in entries {
        let entry = entry.map_err(ProductionSearchQualificationError::from_error)?;
        let metadata = entry
            .metadata()
            .map_err(ProductionSearchQualificationError::from_error)?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}
