use crate::commands::request::CommandRequest;
use crate::db_entry::{DbEntry, DbValue};
use crate::mem_table::mem_table::MemTable;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalRecoveringError;
use crate::wal::id_generator::{OrderedWalId, WalId, WalIdGenerator};
use crate::wal::reader::WalReader;
use glommio_ng::io::Directory;
use std::path::PathBuf;
use std::rc::Rc;

pub struct WalRecoveredState<MT: MemTable> {
    pub ordered_immutable_mem_tables: Vec<(Rc<MT>, OrderedWalId)>,
    pub id_generator: WalIdGenerator,
}

pub struct WalRecovery {
    cpu_shard_id: u32,
    dir: PathBuf,
    storage_config: Rc<StorageConfig>,
}

impl WalRecovery {
    pub fn new(cpu_shard_id: u32, dir: PathBuf, storage_config: Rc<StorageConfig>) -> Self {
        Self {
            cpu_shard_id,
            dir,
            storage_config,
        }
    }

    pub async fn recover<MT: MemTable + std::fmt::Debug>(
        &self,
    ) -> Result<Option<WalRecoveredState<MT>>, WalRecoveringError> {
        let segments = self.get_wal_segments().await?;
        if segments.len() == 0 {
            return Ok(None); // there were no WALs to recover from
        }

        let mut current_mem_table = MT::new(self.storage_config.clone());
        let mut mem_tables: Vec<(Rc<MT>, OrderedWalId)> = Vec::new();
        let mut in_use_ids: Vec<WalId> = Vec::new();
        for (wal_id, wal_path) in segments {
            in_use_ids.push(wal_id);

            let mut reader = WalReader::init(wal_path, Rc::clone(&self.storage_config))
                .await
                .map_err(WalRecoveringError::ReaderFailed)?;

            while let Some(command) = reader
                .next_command()
                .await
                .map_err(WalRecoveringError::ReaderFailed)?
            {
                let entry = match command {
                    CommandRequest::Set(cmd) => DbEntry {
                        key: cmd.key,
                        value: cmd.value,
                    },
                    CommandRequest::Remove(cmd) => DbEntry {
                        key: cmd.key,
                        value: DbValue::Tombstone,
                    },
                    _ => {
                        panic!("Corrupted WAL. Non-mutating or not supported operation was written")
                    }
                };

                current_mem_table.set(entry, false).expect(
                    "Mem table size exceeded while reconstructing from WAL. Corrupted WAL file",
                );
            }

            mem_tables.push((
                Rc::new(current_mem_table),
                OrderedWalId {
                    wal_id,
                    order: reader.get_order(),
                },
            ));
            current_mem_table = MT::new(self.storage_config.clone());
        }

        mem_tables.sort_by_key(|(_, id)| id.order); // sort by order - the greater, the newer the element
        let newest_mem_table = mem_tables.last().unwrap(); // won't panic, already checked if len > 0
        let next_free_order = &newest_mem_table.1.order + 1;

        let id_generator = WalIdGenerator::new(
            &in_use_ids,
            next_free_order,
            self.storage_config.wal_segments_pool_size as u64,
        );

        Ok(Some(WalRecoveredState {
            ordered_immutable_mem_tables: mem_tables,
            id_generator,
        }))
    }

    async fn get_wal_segments(&self) -> Result<Vec<(WalId, PathBuf)>, WalRecoveringError> {
        let directory = Directory::open(&self.dir)
            .await
            .map_err(|e| WalRecoveringError::OpenFailed(e.into()))?;

        let mut segments = Vec::new();
        let file_name_prefix = format!("WAL_{}_", self.cpu_shard_id);
        for entry in directory
            .sync_read_dir()
            .map_err(|e| WalRecoveringError::SegmentsListingFailed(e.into()))?
        {
            let entry = entry.map_err(|e| WalRecoveringError::SegmentsListingFailed(e.into()))?;
            let path = entry.path();

            let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                panic!("Corrupted, non UTF-8 filename in WAL directory");
            };

            if let Some(id_str) = file_name.strip_prefix(file_name_prefix.as_str()) {
                if let Ok(id) = id_str.parse::<WalId>() {
                    let metadata = std::fs::metadata(&path) // block but doesn't matter for recovery on startup
                        .map_err(WalRecoveringError::SegmentsListingFailed)?;

                    if metadata.len() == 0 {
                        continue; // empty, pre-allocated segment - skip
                    }

                    segments.push((id, path))
                }
            }
        }

        directory
            .close()
            .await
            .map_err(|e| WalRecoveringError::SegmentsListingFailed(e.into()))?;
        Ok(segments)
    }
}
