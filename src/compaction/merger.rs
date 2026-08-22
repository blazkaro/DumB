use crate::db_entry::{DbEntry, DbKey};
use crate::manifest::handler::ManifestHandler;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::ss_table::metadata::{SsTableId, SsTableLevel, SsTableMetadata};
use crate::ss_table::reader::SsTableReader;
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use glommio::GlommioError;
use glommio::io::{Directory, DmaFile};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::rc::Rc;

pub struct SsTableMerger {
    cpu_shard_id: u32,
    dir: PathBuf,
    id_generator: Rc<SsTableIdGenerator>,
    manifest: ManifestHandler,
    storage_config: Rc<StorageConfig>,
}

struct HeapEntry {
    db_entry: DbEntry,
    level: SsTableLevel,
    ss_table_id: SsTableId,
    reader_idx: usize,
}

impl Eq for HeapEntry {}

impl PartialEq<Self> for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed on purpose: BinaryHeap is a max-heap, so this makes
        // pop() return the smallest key (ascending merge order), and among
        // equal keys, the one from the lower (newer) level.
        other
            .db_entry
            .key
            .cmp(&self.db_entry.key)
            .then_with(|| other.level.cmp(&self.level))
            .then_with(|| self.ss_table_id.cmp(&other.ss_table_id)) // when key and level is equal, these tables are at level 0. Higher ss table id means newer entry
    }
}

impl SsTableMerger {
    pub fn new(
        cpu_shard_id: u32,
        dir: PathBuf,
        id_generator: Rc<SsTableIdGenerator>,
        manifest: ManifestHandler,
        storage_config: Rc<StorageConfig>,
    ) -> Self {
        Self {
            cpu_shard_id,
            dir,
            id_generator,
            manifest,
            storage_config,
        }
    }

    pub async fn merge_ss_tables(
        &self,
        ss_table_ids: Vec<SsTableId>,
        target_level: SsTableLevel,
    ) -> Result<(), GlommioError<()>> {
        let mut readers: Vec<(SsTableReader, SsTableId)> = self.get_readers(&ss_table_ids).await?;
        let mut writer: Option<SsTableWriter> = None;
        let result = self
            .merge_ss_tables_inner(&mut readers, &mut writer, target_level)
            .await;

        // Async cleanup (relying on ? would use sync Drop), that is why we use outer/inner
        for (reader, _) in readers.into_iter() {
            let _ = reader.close().await;
        }

        if let Some(w) = writer {
            let _ = w.finish().await;
        }

        // TODO: remove written files on error

        result
    }

    async fn merge_ss_tables_inner(
        &self,
        readers: &mut [(SsTableReader, SsTableId)],
        writer: &mut Option<SsTableWriter>,
        target_level: SsTableLevel,
    ) -> Result<(), GlommioError<()>> {
        // Initialize heap with first entry of every ss table
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let mut reader_idx = 0;
        for (reader, id) in readers.iter_mut() {
            Self::advance_reader(reader, &mut heap, reader_idx, reader.get_level(), *id).await?;
            reader_idx += 1;
        }

        if heap.is_empty() {
            return Ok(());
        }

        let maximum_output_size: u64 = self.storage_config.ss_table_level_1_target_size_bytes
            as u64
            * (self.storage_config.ss_table_level_growth_factor as u64).pow(target_level as u32);

        let (mut writer_temp, mut current_id) = self.new_ss_table_output(target_level).await?;
        *writer = Some(writer_temp);

        let mut new_ss_tables: Vec<SsTableMetadata> = Vec::with_capacity(readers.len() / 2); // compaction produces fewer tables than there were initially, that capacity is purely magic number because it's not worth calculating
        let mut min_key: DbKey = Vec::default();
        let mut entry_count = 0u32;
        let mut total_processed_count = 0u32;
        while !heap.is_empty() {
            let top = heap.pop().unwrap();
            total_processed_count += 1;

            if entry_count == 0 {
                min_key = top.db_entry.key.clone();
            }

            Self::advance_reader(
                &mut readers[top.reader_idx].0,
                &mut heap,
                top.reader_idx,
                top.level,
                top.ss_table_id,
            )
            .await?;

            // Discard duplicates
            // This guarantees we only run the bottom logic once per unique key.
            // Remember that heap provides order, so duplicates are popping out first if they exist
            while let Some(next) = heap.peek() {
                if next.db_entry.key != top.db_entry.key {
                    break;
                }

                let stale = heap.pop().unwrap(); // duplicate
                total_processed_count += 1;
                Self::advance_reader(
                    &mut readers[stale.reader_idx].0,
                    &mut heap,
                    stale.reader_idx,
                    stale.level,
                    stale.ss_table_id,
                )
                .await?;
            }

            writer
                .as_mut()
                .unwrap()
                .write_entry(&top.db_entry.key, &top.db_entry.value)
                .await?;

            entry_count += 1;

            // Check for size (rotate writer when maximum size is exceeded and heap still has items to be processed)
            if writer.as_ref().unwrap().bytes_written() >= maximum_output_size || heap.is_empty() {
                new_ss_tables.push(SsTableMetadata {
                    id: current_id,
                    entry_count,
                    size_bytes: writer.as_ref().unwrap().bytes_written(),
                    level: target_level,
                    min_key: min_key.clone(),
                    max_key: top.db_entry.key.clone(), // last item processed in this ss table, also valid for duplicated key
                });

                // DURABILITY: flush to disk
                writer.take().unwrap().finish().await?;

                if heap.is_empty() {
                    break;
                }

                entry_count = 0;
                (writer_temp, current_id) = self.new_ss_table_output(target_level).await?;
                *writer = Some(writer_temp);
            }

            if total_processed_count
                % self
                    .storage_config
                    .ss_table_compaction_yield_check_processed_threshold
                == 0
            {
                glommio::yield_if_needed().await;
            }
        }

        // Sync metadata
        let parent = Directory::open(&self.dir).await?;
        parent.sync().await?;
        parent.close().await?;

        // Register in manifest after durable changes
        self.manifest
            .apply_compaction(new_ss_tables, readers.iter().map(|(_, id)| *id).collect())
            .await?;

        Ok(())
    }

    async fn get_readers(
        &self,
        ss_table_ids: &[SsTableId],
    ) -> Result<Vec<(SsTableReader, SsTableId)>, GlommioError<()>> {
        let mut readers: Vec<(SsTableReader, SsTableId)> = Vec::with_capacity(ss_table_ids.len());
        for id in ss_table_ids {
            let path = self
                .dir
                .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));

            let file = match DmaFile::open(&path).await {
                Ok(f) => f,
                Err(e) => {
                    for (r, _) in readers {
                        let _ = r.close().await;
                    }
                    return Err(e);
                }
            };

            let reader = match SsTableReader::init(file, Rc::clone(&self.storage_config)).await {
                Ok(r) => r,
                Err(e) => {
                    for (r, _) in readers {
                        let _ = r.close().await;
                    }
                    return Err(e);
                }
            };

            readers.push((reader, *id));
        }

        Ok(readers)
    }

    async fn advance_reader(
        reader: &mut SsTableReader,
        heap: &mut BinaryHeap<HeapEntry>,
        reader_idx: usize,
        level: SsTableLevel,
        ss_table_id: SsTableId,
    ) -> Result<(), GlommioError<()>> {
        match reader.next_entry().await {
            Ok(next) => heap.push(HeapEntry {
                db_entry: next,
                level,
                ss_table_id,
                reader_idx,
            }),
            Err(GlommioError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {} // exhausted, fine
            Err(e) => return Err(e), // anything else is a real failure — propagate it
        }

        Ok(())
    }

    async fn new_ss_table_output(
        &self,
        level: SsTableLevel,
    ) -> Result<(SsTableWriter, SsTableId), GlommioError<()>> {
        let id = self.id_generator.next_id();
        let path = self
            .dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));

        let output_file = DmaFile::create(&path).await?;

        let writer =
            SsTableWriter::init(output_file, level, Rc::clone(&self.storage_config)).await?;

        Ok((writer, id))
    }
}
