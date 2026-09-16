use crate::commands::errors::{GetError, RemoveError, SetError};
use crate::commands::remove::RemoveCommand;
use crate::commands::request::CommandRequest;
use crate::commands::set::SetCommand;
use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::manifest::handler::ManifestHandler;
use crate::manifest::snapshot_lookup::SnapshotLookup;
use crate::mem_table::flusher::MemTableFlushHandler;
use crate::mem_table::immutable::ImmutableMemTables;
use crate::mem_table::mem_table::{MemTable, MemTableError};
use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use crate::ss_table::reader::SsTableReader;
use crate::storage_config::StorageConfig;
use crate::wal::log::Wal;
use glommio_ng::io::OpenOptions;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

pub struct CommandHandler<MT: MemTable + 'static> {
    mem_table: Rc<RefCell<MT>>, // NEVER HELD MID-AWAIT
    mem_table_flusher: MemTableFlushHandler<MT>,
    storage_config: Rc<StorageConfig>,
    immutable_mem_tables: ImmutableMemTables<MT>,
    manifest: ManifestHandler,
    wal: Wal,
    ss_tables_dir: PathBuf,
    cpu_shard_id: u32,
}

impl<MT: MemTable + 'static> CommandHandler<MT> {
    pub fn new(
        flusher: MemTableFlushHandler<MT>,
        manifest: ManifestHandler,
        wal: Wal,
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
            wal,
            ss_tables_dir,
            cpu_shard_id,
        }
    }

    pub async fn set(&self, key: DbKey, value: DbValue) -> Result<(), SetError> {
        let entry = DbEntry {
            key: key.clone(),
            value: value.clone(),
        };

        let req = match value {
            DbValue::Value(_) => CommandRequest::Set(SetCommand { key, value }),
            DbValue::Tombstone => CommandRequest::Remove(RemoveCommand { key }),
        };
        self.wal.append(req).await.map_err(SetError::WalFailure)?;

        let entry = match self.mem_table.borrow_mut().set(entry) {
            Ok(()) => return Ok(()),
            Err(MemTableError::SizeExceeded(entry)) => entry,
        };

        let full = Rc::new({
            let mut guard = self.mem_table.borrow_mut();
            std::mem::replace(&mut *guard, MT::new(Rc::clone(&self.storage_config)))
        });

        self.immutable_mem_tables.push(Rc::clone(&full));
        self.mem_table_flusher
            .flush(full)
            .map_err(SetError::FlusherFailure)?;

        let _ = self.mem_table.borrow_mut().set(entry); // fresh mem table isn't going to overflow

        Ok(())
    }

    pub async fn get(&self, key: &DbKey) -> Result<Option<Rc<DbValue>>, GetError> {
        if let Some(value) = self.mem_table.borrow().get(key) {
            return Ok(Some(value));
        }

        if let Some(value) = self.immutable_mem_tables.get(key) {
            return Ok(Some(value));
        }

        self.search_ss_tables(key).await // No mem table borrow is held here - ALL MEM TABLE OPS ARE SYNC, NO ASYNC RACES
    }

    pub async fn remove(&self, key: DbKey) -> Result<(), RemoveError> {
        self.set(key, DbValue::Tombstone).await
    }

    async fn search_ss_tables(&self, key: &DbKey) -> Result<Option<Rc<DbValue>>, GetError> {
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

            let overlapping = SnapshotLookup::get_overlap(level_idx, key.clone(), key, &snapshot);
            if overlapping.is_empty() {
                continue;
            }

            for ss_table in overlapping {
                if let Some(value) = self.search_ss_table(key, ss_table).await? {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    async fn search_ss_table(
        &self,
        key: &DbKey,
        ss_table: Rc<SsTableMetadata>,
    ) -> Result<Option<Rc<DbValue>>, GetError> {
        let path = self
            .ss_tables_dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, ss_table.id));

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .dma_open(&path)
            .await
            .map_err(|e| GetError::OpenFailure(e.into()))?;
        let mut reader = SsTableReader::init(file, Rc::clone(&self.storage_config))
            .await
            .map_err(GetError::ReadFailure)?;

        let result = async {
            while !reader.is_eof() {
                let entry = reader.next_entry().await.map_err(GetError::ReadFailure)?;
                if entry.key == *key {
                    return Ok(Some(Rc::new(entry.value)));
                }
            }
            Ok(None)
        }
        .await;

        reader.close().await.map_err(GetError::ReadFailure)?;
        result
    }
}
