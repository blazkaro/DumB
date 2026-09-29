use crate::ss_table::errors::SsTableIdGeneratorError;
use crate::ss_table::metadata::SsTableId;
use glommio_ng::io::Directory;
use std::cell::Cell;
use std::path::Path;

pub struct SsTableIdGenerator {
    next_id: Cell<SsTableId>,
}

impl SsTableIdGenerator {
    pub async fn init(dir: &Path, cpu_shard_id: u32) -> Result<Self, SsTableIdGeneratorError> {
        let directory = Directory::open(dir)
            .await
            .map_err(|e| SsTableIdGeneratorError::SsTablesListingFailed(e.into()))?;

        let mut ss_table_ids = Vec::new();
        let file_name_prefix = format!("ss_table_{}_", cpu_shard_id);
        for entry in directory
            .sync_read_dir()
            .map_err(|e| SsTableIdGeneratorError::SsTablesListingFailed(e.into()))?
        {
            let entry =
                entry.map_err(|e| SsTableIdGeneratorError::SsTablesListingFailed(e.into()))?;
            let path = entry.path();

            let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                panic!("Corrupted, non UTF-8 filename in ss tables directory");
            };

            if let Some(id_str) = file_name.strip_prefix(file_name_prefix.as_str()) {
                if let Ok(id) = id_str.parse::<SsTableId>() {
                    ss_table_ids.push(id);
                }
            }
        }

        directory
            .close()
            .await
            .map_err(|e| SsTableIdGeneratorError::SsTablesListingFailed(e.into()))?;

        let max_id = ss_table_ids.into_iter().max().unwrap_or(0);
        Ok(Self {
            next_id: Cell::new(max_id + 1),
        })
    }

    pub fn next_id(&self) -> SsTableId {
        let next_free = self.next_id.get();
        self.next_id.set(next_free + 1);
        next_free
    }
}
