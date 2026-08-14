use super::{
    durability, manifest, relational_row_page_artifact_file,
    relational_row_page_manifest_generation_file, relational_row_page_root_descriptor_file,
    relational_row_page_root_key_file, root, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPageRootDescriptor,
    RelationalRowPageRootManifest, RelationalRowPageTableRoot, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RelationalRowPageRootReader {
    directory: PathBuf,
    pub(super) manifest: Arc<RelationalRowPageRootManifest>,
    config: RelationalRowPagePublicationConfig,
}

impl RelationalRowPageRootReader {
    pub fn open_latest(
        directory: &Path,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Option<Self>, RelationalRowPagePublicationError> {
        let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        manifest::read_manifest_if_exists(&path, config)?
            .map(|manifest| Self::from_manifest(directory, manifest, config))
            .transpose()
    }

    pub fn open_generation(
        directory: &Path,
        generation: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        let path = directory.join(relational_row_page_manifest_generation_file(generation));
        let manifest = manifest::read_manifest(&path, config)?;
        if manifest.generation != generation {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "generation manifest {generation} identifies generation {}",
                manifest.generation
            )));
        }
        Self::from_manifest(directory, manifest, config)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalRowPageRootManifest,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        validate_artifact_length(
            &directory.join(relational_row_page_artifact_file(manifest.generation)),
            manifest.page_artifact.encoded_len,
            "row-page artifact",
        )?;
        validate_artifact_length(
            &directory.join(relational_row_page_root_descriptor_file(
                manifest.generation,
            )),
            manifest.root_descriptor_artifact.encoded_len,
            "row-page root descriptor artifact",
        )?;
        validate_artifact_length(
            &directory.join(relational_row_page_root_key_file(manifest.generation)),
            manifest.root_key_artifact.encoded_len,
            "row-page root key artifact",
        )?;
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest: Arc::new(manifest),
            config,
        })
    }

    pub fn manifest(&self) -> &RelationalRowPageRootManifest {
        &self.manifest
    }

    pub fn read_table_page_descriptor(
        &self,
        table: &str,
        ordinal: u64,
    ) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
        let table_root = self.table_root(table)?;
        if ordinal >= table_root.page_count {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "row-page ordinal {ordinal} exceeds table {table} page count {}",
                table_root.page_count
            )));
        }
        let descriptor_ordinal = table_root
            .first_descriptor
            .checked_add(ordinal)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Corrupt(
                    "row-page descriptor ordinal overflow".to_string(),
                )
            })?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        root::read_descriptor(
            &mut descriptors,
            &mut keys,
            descriptor_ordinal,
            &self.manifest,
            self.config,
        )
    }

    pub fn visit_table_pages<F>(
        &self,
        table: &str,
        mut visitor: F,
    ) -> Result<(), RelationalRowPagePublicationError>
    where
        F: FnMut(&RelationalRowPageRootDescriptor) -> Result<(), RelationalRowPagePublicationError>,
    {
        let table_root = self.table_root(table)?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        let mut previous_upper: Option<Vec<u8>> = None;
        for offset in 0..table_root.page_count {
            let ordinal = table_root
                .first_descriptor
                .checked_add(offset)
                .ok_or_else(|| {
                    RelationalRowPagePublicationError::Corrupt(
                        "row-page descriptor ordinal overflow".to_string(),
                    )
                })?;
            let descriptor = root::read_descriptor(
                &mut descriptors,
                &mut keys,
                ordinal,
                &self.manifest,
                self.config,
            )?;
            if previous_upper
                .as_ref()
                .is_some_and(|upper| upper.as_slice() >= descriptor.lower_bound.as_slice())
            {
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "table {table} row-page bounds overlap or are unordered"
                )));
            }
            previous_upper = Some(descriptor.upper_bound.clone());
            visitor(&descriptor)?;
        }
        Ok(())
    }

    fn table_root(
        &self,
        table: &str,
    ) -> Result<&RelationalRowPageTableRoot, RelationalRowPagePublicationError> {
        self.manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
            .map(|index| &self.manifest.tables[index])
            .map_err(|_| RelationalRowPagePublicationError::MissingTable(table.to_string()))
    }

    fn descriptor_path(&self) -> PathBuf {
        self.directory
            .join(relational_row_page_root_descriptor_file(
                self.manifest.generation,
            ))
    }

    fn key_path(&self) -> PathBuf {
        self.directory
            .join(relational_row_page_root_key_file(self.manifest.generation))
    }
}

fn validate_artifact_length(
    path: &Path,
    expected: u64,
    context: &str,
) -> Result<(), RelationalRowPagePublicationError> {
    let actual = fs::metadata(path)
        .map_err(durability("read row-page artifact metadata"))?
        .len();
    if actual != expected {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "{context} contains {actual} bytes, expected {expected}"
        )));
    }
    Ok(())
}
