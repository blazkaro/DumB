use crate::commands::errors::{GetError, RemoveError, SetError};
use crate::commands::remove::RemoveCommand;
use crate::commands::request::CommandRequest;
use crate::commands::set::SetCommand;
use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::manifest::handler::ManifestHandler;
use crate::manifest::snapshot_lookup::SnapshotLookup;
use crate::mem_table::errors::MemTablePreallocationError;
use crate::mem_table::flusher::MemTableFlushHandler;
use crate::mem_table::immutable::ImmutableMemTables;
use crate::mem_table::mem_table::MemTable;
use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use crate::ss_table::reader::SsTableReader;
use crate::storage_config::StorageConfig;
use crate::wal::log::Wal;
use glommio_ng::io::OpenOptions;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

struct WaitingMemTable<MT: MemTable> {
    mem_table: Rc<MT>,
    sync_id: u32,
}

pub struct CommandHandler<MT: MemTable + 'static> {
    mem_table: RefCell<Rc<RefCell<MT>>>,
    gate: RefCell<Option<glommio_ng::sync::Gate>>,
    mem_table_flusher: MemTableFlushHandler<MT>,
    storage_config: Rc<StorageConfig>,
    immutable_mem_tables: Rc<ImmutableMemTables<MT>>,
    manifest: ManifestHandler,
    wal: Rc<Wal>,
    ss_tables_dir: PathBuf,
    cpu_shard_id: u32,
}

impl<MT: MemTable + 'static> CommandHandler<MT> {
    pub fn new(
        flusher: MemTableFlushHandler<MT>,
        manifest: ManifestHandler,
        wal: Rc<Wal>,
        ss_tables_dir: PathBuf,
        immutable_mem_tables: Rc<ImmutableMemTables<MT>>,
        cpu_shard_id: u32,
        storage_config: Rc<StorageConfig>,
    ) -> Self {
        Self {
            mem_table: RefCell::new(Rc::new(RefCell::new(MT::new(Rc::clone(&storage_config))))),
            gate: RefCell::new(Some(glommio_ng::sync::Gate::new())),
            mem_table_flusher: flusher,
            storage_config: Rc::clone(&storage_config),
            immutable_mem_tables,
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

        let (mem_table, pass) = loop {
            // Take clone for this attempt
            let mem_table = Rc::clone(&self.mem_table.borrow());

            let preallocation = mem_table.borrow_mut().preallocate(&entry);
            match preallocation {
                Ok(()) => {
                    // pre-allocated, fine
                    // current gate still belongs to this mem table, no await between here and preallocation
                    let pass = self
                        .gate
                        .borrow()
                        .as_ref()
                        .expect("Gate must always be present")
                        .enter()
                        .expect("Gate was closed, although it has to be always open");

                    break (mem_table, pass);
                }
                Err(MemTablePreallocationError::SizeExceeded) => {
                    // rotate

                    let taken = self.gate.borrow_mut().take();
                    match taken {
                        Some(prev_gate) => {
                            // We are the only task that will rotate

                            // Rotate immediately for future calls. WAL uses actor loop so ordering is preserved
                            let prev_wal_id =
                                match self.wal.rotate().map_err(SetError::WalRotationFailure) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        // Restore the previous, valid state
                                        *self.gate.borrow_mut() = Some(prev_gate);
                                        return Err(e);
                                    }
                                };

                            // Swap both mem table and gate synchronously, no window for invalid state
                            let full = std::mem::replace(
                                &mut *self.mem_table.borrow_mut(),
                                Rc::new(RefCell::new(MT::new(Rc::clone(&self.storage_config)))),
                            );
                            *self.gate.borrow_mut() = Some(glommio_ng::sync::Gate::new());

                            // we don't need that clone anymore
                            // don't keep untracked Rc while draining
                            drop(mem_table);

                            // Drain in flight commands against old mem table.
                            // We can do it after rotation, because we took clone of old mem table in the beginning
                            // Also, we have to do it after rotation, because future calls need to have new gate, mem table etc. immediately, without waiting for drain
                            // (or panicking without proper, lock-like handling)
                            prev_gate.close().await.ok();

                            // Drained, no task is holding that old mem table right now, so we can convert into inner
                            let full = Rc::into_inner(full).expect("Mem table was still being processed by some task despite draining in flight commands");
                            let full = Rc::new(full.into_inner());

                            self.immutable_mem_tables.push(Rc::clone(&full));
                            self.mem_table_flusher
                                .flush(full, prev_wal_id)
                                .map_err(SetError::FlusherFailure)?;

                            // Loop back and retry
                        }
                        None => {
                            // don't keep Rc while yielding
                            drop(mem_table);
                            glommio_ng::executor().yield_now().await;
                            // Loop back and retry
                        }
                    }
                }
            }
        };

        let req = match &value {
            DbValue::Value(_) => CommandRequest::Set(SetCommand { key, value }),
            DbValue::Tombstone => CommandRequest::Remove(RemoveCommand { key }),
        };
        let append_result = self.wal.append(req).await.map_err(SetError::WalFailure);

        if let Ok(_) = append_result {
            mem_table
                .borrow_mut()
                .set(entry, true)
                .expect("mem table exceeded size despite pre-allocation");
        }

        // If it's last pass, gate is going to be immediately informed that draining has ended
        // Depending on how tasks are scheduled, it may be before we drop Rc to mem_table, and we would get panic that some Rc to mem tables are still alive
        // To prevent this, just drop cloned mem table before pass
        drop(mem_table);
        drop(pass);

        // Now, return error if it happened (after dropping mem table and pass)
        if append_result.is_err() {
            return append_result;
        }

        Ok(())
    }

    pub async fn get(&self, key: &DbKey) -> Result<Option<Rc<DbValue>>, GetError> {
        if let Some(value) = self.mem_table.borrow().borrow().get(key) {
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
            let mut entries_processed: u32 = 0;
            while !reader.is_eof() {
                let entry = reader.next_entry().await.map_err(GetError::ReadFailure)?;
                if entry.key == *key {
                    return Ok(Some(Rc::new(entry.value)));
                }

                entries_processed += 1;
                if entries_processed % 64 == 0 {
                    glommio_ng::executor().yield_if_needed().await;
                    entries_processed = 0;
                }
            }
            Ok(None)
        }
        .await;

        reader.close().await.map_err(GetError::ReadFailure)?;
        result
    }
}
