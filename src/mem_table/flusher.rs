use crate::compaction::errors::CompactionTriggerError;
use crate::compaction::trigger::CompactionTrigger;
use crate::errors::classified::{ClassifiedError, classify_glommio_error};
use crate::errors::retryable::{RetryableError, is_io_error_transient};
use crate::manifest::errors::ManifestWriteError;
use crate::manifest::handler::ManifestHandler;
use crate::mem_table::errors::FlushError;
use crate::mem_table::immutable::ImmutableMemTables;
use crate::mem_table::mem_table::MemTable;
use crate::retries::retry::{RetryPolicy, retry_if};
use crate::ss_table::errors::SsTableWriteError;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::ss_table::metadata::SsTableMetadata;
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use futures::StreamExt;
use glommio_ng::channels::local_channel::{LocalReceiver, LocalSender};
use glommio_ng::io::{Directory, DmaFile};
use glommio_ng::{Latency, Shares};
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Clone)]
pub struct MemTableFlushHandler<MT: MemTable + 'static> {
    sender: Rc<LocalSender<Rc<MT>>>,
}

impl<MT: MemTable + 'static> MemTableFlushHandler<MT> {
    pub async fn new(
        dir: PathBuf,
        cpu_shard_id: u32,
        storage_config: Rc<StorageConfig>,
        manifest: ManifestHandler,
        compaction_trigger: CompactionTrigger,
        id_generator: Rc<SsTableIdGenerator>,
        retry_policy: RetryPolicy,
        shares: Shares,
        latency: Latency,
    ) -> Result<Self, FlushError> {
        let parent = Directory::open(&dir)
            .await
            .map_err(|e| FlushError::DirOpenFailed(e.into()))?;

        let internal = MemTableFlusherInternal {
            cpu_shard_id,
            storage_config,
            dir,
            manifest,
            compaction_trigger,
            parent,
            id_generator,
            immutable_mem_tables: ImmutableMemTables::new(),
            retry_policy,
        };

        let (sender, receiver) = glommio_ng::channels::local_channel::new_unbounded();

        let queue_name = format!("mem-table-flusher-{cpu_shard_id}");
        let task_queue =
            glommio_ng::executor().create_task_queue(shares, latency, queue_name.as_str());

        glommio_ng::spawn_local_into(internal.run(receiver), task_queue)
            .expect("failed to spawn mem table flusher onto its task queue")
            .detach();

        Ok(Self {
            sender: Rc::new(sender),
        })
    }

    pub fn flush(&self, mem_table: Rc<MT>) -> Result<(), FlushError> {
        self.sender
            .try_send(mem_table)
            .map_err(|e| match classify_glommio_error(e) {
                ClassifiedError::ChannelClosed(_) => FlushError::FlusherGone,
                _ => panic!("unexpected result sending to flush channel"),
            })
    }
}

#[derive(Debug)]
enum FlushInternalError {
    CreateFileFailed(std::io::Error),
    WriterFailure(SsTableWriteError),
    DirectorySyncFailed(std::io::Error),
    ManifestRegistrationFailed(ManifestWriteError),
    CompactionTriggerFailed(CompactionTriggerError),
}

impl RetryableError for FlushInternalError {
    fn is_retryable(&self) -> bool {
        match self {
            FlushInternalError::CreateFileFailed(e)
            | FlushInternalError::DirectorySyncFailed(e) => is_io_error_transient(e),
            FlushInternalError::WriterFailure(e) => e.is_retryable(),
            FlushInternalError::ManifestRegistrationFailed(e) => e.is_retryable(),
            FlushInternalError::CompactionTriggerFailed(e) => e.is_retryable(),
        }
    }
}

struct MemTableFlusherInternal<MT: MemTable> {
    cpu_shard_id: u32,
    storage_config: Rc<StorageConfig>,
    dir: PathBuf,
    manifest: ManifestHandler,
    compaction_trigger: CompactionTrigger,
    parent: Directory,
    id_generator: Rc<SsTableIdGenerator>,
    immutable_mem_tables: ImmutableMemTables<MT>,
    retry_policy: RetryPolicy,
}

impl<MT: MemTable> MemTableFlusherInternal<MT> {
    async fn run(self, receiver: LocalReceiver<Rc<MT>>) {
        let mut mem_tables = receiver.stream();

        while let Some(mem_table) = mem_tables.next().await {
            match retry_if(
                &self.retry_policy,
                async || self.flush(Rc::clone(&mem_table)).await,
                |e| e.is_retryable(),
            )
            .await
            {
                Ok(()) => self.immutable_mem_tables.pop(), // because both channel (local receiver) and immutable mem tables are FIFO, it removes currently processed mem table
                Err(e) => {
                    // Serious problem here: in memory data couldn't be durably saved, so we need something in order to not loss it forever
                    // This happens because we don't have WAL yet
                    panic!("Flush failed permanently: {:?}", e);
                }
            }
        }
    }

    async fn flush(&self, mem_table: Rc<MT>) -> Result<(), FlushInternalError> {
        // ATOMICITY: we're not flushing to temp file, but we won't list this ss table in manifest unless completely saved
        let id = self.id_generator.next_id();
        let path = self
            .dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));
        let file = DmaFile::create(&path)
            .await
            .map_err(|e| FlushInternalError::CreateFileFailed(e.into()))?;

        let mut writer = SsTableWriter::init(file, 0, Rc::clone(&self.storage_config))
            .await
            .map_err(FlushInternalError::WriterFailure)?;

        for (key, value) in mem_table.iter() {
            writer
                .write_entry(key, value)
                .await
                .map_err(FlushInternalError::WriterFailure)?;
        }

        let bytes_written = writer.bytes_written();

        // DURABILITY: flush to disk then update dir metadata
        writer
            .finish()
            .await
            .map_err(FlushInternalError::WriterFailure)?;
        self.parent
            .sync()
            .await
            .map_err(|e| FlushInternalError::DirectorySyncFailed(e.into()))?;

        // AFTER durable save, make ss table reachable
        self.manifest
            .add_ss_table(SsTableMetadata {
                id,
                entry_count: mem_table.entry_count(),
                size_bytes: bytes_written,
                level: 0,
                min_key: mem_table.min_key().clone(),
                max_key: mem_table.max_key().clone(),
            })
            .await
            .map_err(FlushInternalError::ManifestRegistrationFailed)?;

        self.compaction_trigger
            .notify()
            .map_err(FlushInternalError::CompactionTriggerFailed)?;

        Ok(())
    }
}
