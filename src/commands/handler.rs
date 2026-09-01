use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::manifest::handler::ManifestHandler;
use crate::manifest::snapshot_lookup::SnapshotLookup;
use crate::mem_table::flusher::MemTableFlushHandler;
use crate::mem_table::immutable::ImmutableMemTables;
use crate::mem_table::mem_table::{MemTable, MemTableError};
use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use crate::ss_table::reader::SsTableReader;
use crate::storage_config::StorageConfig;
use glommio_ng::GlommioError;
use glommio_ng::io::DmaFile;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

pub struct CommandHandler<MT: MemTable + 'static> {
    mem_table: Rc<RefCell<MT>>, // NEVER HELD MID-AWAIT
    mem_table_flusher: MemTableFlushHandler<MT>,
    storage_config: Rc<StorageConfig>,
    immutable_mem_tables: ImmutableMemTables<MT>,
    manifest: ManifestHandler,
    ss_tables_dir: PathBuf,
    cpu_shard_id: u32,
}

impl<MT: MemTable + 'static> CommandHandler<MT> {
    pub fn new(
        flusher: MemTableFlushHandler<MT>,
        manifest: ManifestHandler,
        ss_tables_dir: PathBuf,
        cpu_shard_id: u32,
        storage_config: Rc<StorageConfig>,
    ) -> Self {
        Self {
            mem_table: Rc::new(RefCell::new(MT::new(Rc::clone(&storage_config)))),
            mem_table_flusher: flusher,
            storage_config: Rc::clone(&storage_config),
            immutable_mem_tables: ImmutableMemTables::<MT>::new(),
            manifest,
            ss_tables_dir,
            cpu_shard_id,
        }
    }

    pub fn set(&self, key: DbKey, value: DbValue) -> Result<(), GlommioError<()>> {
        let entry = DbEntry { key, value };

        let entry = match self.mem_table.borrow_mut().set(entry) {
            Ok(()) => return Ok(()),
            Err(MemTableError::SizeExceeded(entry)) => entry,
        };

        let full = {
            let mut guard = self.mem_table.borrow_mut();
            std::mem::replace(&mut *guard, MT::new(Rc::clone(&self.storage_config)))
        };

        let full = Rc::new(full);

        self.immutable_mem_tables.push(Rc::clone(&full));
        self.mem_table_flusher.flush(full).map_err(|_| {
            GlommioError::IoError(std::io::Error::other("flusher task is no longer running"))
        })?;

        let _ = self.mem_table.borrow_mut().set(entry); // fresh mem table isn't going to overflow

        Ok(())
    }

    pub async fn get(&self, key: &DbKey) -> Result<Option<Rc<DbValue>>, GlommioError<()>> {
        if let Some(value) = self.mem_table.borrow().get(key) {
            return Ok(Some(value));
        }

        if let Some(value) = self.immutable_mem_tables.get(key) {
            return Ok(Some(value));
        }

        self.search_ss_tables(key).await // No mem table borrow is held here - ALL MEM TABLE OPS ARE SYNC, NO ASYNC RACES
    }

    pub fn remove(&self, key: DbKey) -> Result<(), GlommioError<()>> {
        self.set(key, DbValue::Tombstone)
    }

    async fn search_ss_tables(&self, key: &DbKey) -> Result<Option<Rc<DbValue>>, GlommioError<()>> {
        let snapshot = self.manifest.snapshot();

        if let Some(level_0) = snapshot.get_level(0) {
            let mut candidates: Vec<_> = level_0.iter().cloned().collect();
            candidates.sort_by(|a, b| b.id.cmp(&a.id)); // newest id first

            for ss_table in candidates {
                if let Some(value) = self.search_ss_table(key, ss_table).await? {
                    return Ok(Some(value));
                }
            }
        }

        for level_idx in 1..snapshot.levels_count() {
            let level_idx = level_idx as SsTableLevel;

            if let Some(level) = snapshot.get_level(level_idx) {
                let overlapping =
                    SnapshotLookup::get_overlap(level_idx, key.clone(), key, &snapshot);
                if overlapping.is_empty() {
                    continue;
                }

                for ss_table in overlapping {
                    if let Some(value) = self.search_ss_table(key, ss_table).await? {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn search_ss_table(
        &self,
        key: &DbKey,
        ss_table: Rc<SsTableMetadata>,
    ) -> Result<Option<Rc<DbValue>>, GlommioError<()>> {
        let path = self
            .ss_tables_dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, ss_table.id));

        let file = DmaFile::open(&path).await?;
        let mut reader = SsTableReader::init(file, Rc::clone(&self.storage_config)).await?;

        let result = async {
            while !reader.is_eof() {
                let entry = reader.next_entry().await?;
                if entry.key == *key {
                    return Ok(Some(Rc::new(entry.value)));
                }
            }
            Ok(None)
        }
        .await;

        reader.close().await?;
        result
    }
}
