use crate::compaction::trigger::CompactionTrigger;
use crate::manifest::handler::ManifestHandler;
use crate::mem_table::immutable::ImmutableMemTables;
use crate::mem_table::mem_table::MemTable;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::ss_table::metadata::SsTableMetadata;
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use futures::StreamExt;
use glommio_ng::channels::local_channel::{LocalReceiver, LocalSender};
use glommio_ng::io::{Directory, DmaFile};
use glommio_ng::{GlommioError, Latency, Shares};
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
        shares: Shares,
        latency: Latency,
    ) -> Result<Self, GlommioError<()>> {
        let parent = Directory::open(&dir).await?;

        let internal = MemTableFlusherInternal {
            cpu_shard_id,
            storage_config,
            dir,
            manifest,
            compaction_trigger,
            parent,
            id_generator,
            immutable_mem_tables: ImmutableMemTables::new(),
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

    pub fn flush(&self, mem_table: Rc<MT>) -> Result<(), GlommioError<Rc<MT>>> {
        self.sender.try_send(mem_table)
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
}

impl<MT: MemTable> MemTableFlusherInternal<MT> {
    async fn run(mut self, receiver: LocalReceiver<Rc<MT>>) {
        let mut mem_tables = receiver.stream();

        while let Some(mem_table) = mem_tables.next().await {
            match self.flush(mem_table.as_ref()).await {
                Ok(()) => self.immutable_mem_tables.pop(), // because both channel (local receiver) and immutable mem tables are FIFO, it removes currently processed mem table
                Err(e) => {
                    // Serious problem here: in memory data wasn't durably saved, so we need something in order to not loss it forever
                    // TODO: retry policy
                }
            }
        }
    }

    async fn flush(&mut self, mem_table: &MT) -> Result<(), GlommioError<()>> {
        // ATOMICITY: we're not flushing to temp file, but we won't list this ss table in manifest unless completely saved
        let id = self.id_generator.next_id();
        let path = self
            .dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));
        let file = DmaFile::create(&path).await?;

        let mut writer = SsTableWriter::init(file, 0, Rc::clone(&self.storage_config)).await?;

        for (key, value) in mem_table.iter() {
            writer.write_entry(key, value).await?;
        }

        let bytes_written = writer.bytes_written();

        // DURABILITY: flush to disk then update dir metadata
        writer.finish().await?;
        self.parent.sync().await?;

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
            .await?;

        self.compaction_trigger.notify();

        Ok(())
    }
}
