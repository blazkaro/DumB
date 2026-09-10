use crate::commands::handler::CommandHandler;
use crate::compaction::compactor::SsTableCompactor;
use crate::compaction::trigger::CompactionTrigger;
use crate::manifest::handler::ManifestHandler;
use crate::mem_table::btree_mem_table::BTreeMemTable;
use crate::mem_table::flusher::MemTableFlushHandler;
use crate::retries::retry::RetryPolicy;
use crate::shards::router::ShardRouter;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::storage_config::StorageConfig;
use crate::tcp::listener::Listener;
use glommio_ng::{CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, Shares};
use std::future::pending;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

pub mod commands;
pub mod compaction;
pub mod db_entry;
pub mod errors;
pub mod le_reader;
pub mod manifest;
pub mod mem_table;
pub mod retries;
pub mod shards;
pub mod ss_table;
pub mod storage_config;
pub mod tcp;

fn main() {
    let shards_count = CpuSet::online()
        .expect("Could not get online CPU cores")
        .len(); // Counts logical cores, for true thread-per-core disable Hyper-Threading

    let placement = PoolPlacement::MaxSpread(shards_count, None);
    let home_path = PathBuf::from(std::env::var_os("HOME").expect("Failed to get home directory"));
    let handles = LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let cpu_shard_id = glommio_ng::executor().id() as u32;
            let shard_dir = home_path.join(format!("DumB_data/{cpu_shard_id}"));
            std::fs::create_dir_all(&shard_dir).expect("Failed to create CPU's shar dir");

            // TODO: custom configuration (shares as well)

            let storage_config: Rc<StorageConfig> = Rc::new(StorageConfig {
                // Mem table & flushing
                memory_table_bytes_max_size: 64 * 1024 * 1024, // 64MB
                ss_table_flush_buffer_size_bytes: 512 * 1024,  // 512 KB
                ss_table_flush_buffer_writes_behind: 4,

                // Compaction
                ss_table_compaction_buffer_size_bytes: 1024 * 1024, // 1 MB
                ss_table_compaction_buffer_writes_behind: 4,
                ss_table_compaction_yield_check_processed_threshold: 1024,

                // LSM architecture
                ss_table_level_0_target_tables_count: 4,
                ss_table_level_1_target_size_bytes: 256 * 1024 * 1024, // 256MB (4 files * 64MB),
                ss_table_level_growth_factor: 10,
                ss_table_target_size_bytes: 64 * 1024 * 1024, // 64MB per file,

                // Internal ss table config
                ss_table_block_target_size_bytes: 16 * 1024, // 16KB per block
                ss_table_read_buffer_size: 128 * 1024,       // 128KB,
                ss_table_buffer_read_ahead: 2,
            });

            let ss_tables_dir = shard_dir.join("ss");
            std::fs::create_dir_all(&ss_tables_dir).expect("Failed to create SS Tables dir");
            let ss_table_id_generator: Rc<SsTableIdGenerator> = Rc::new(SsTableIdGenerator::new());

            let manifest_path = shard_dir.join("manifest");
            std::fs::create_dir_all(&manifest_path).expect("Failed to create Manifest dir");

            let manifest = ManifestHandler::open_or_create(
                &manifest_path,
                cpu_shard_id,
                Shares::Static(200),
                Latency::NotImportant,
            )
            .await
            .expect("Failed to open or create manifest");

            let (compaction_trigger, compaction_receiver) = CompactionTrigger::new();
            let flusher = MemTableFlushHandler::<BTreeMemTable>::new(
                ss_tables_dir.clone(),
                cpu_shard_id,
                Rc::clone(&storage_config),
                manifest.clone(),
                compaction_trigger,
                Rc::clone(&ss_table_id_generator),
                RetryPolicy {
                    max_attempts: 5,
                    base_delay: Duration::from_millis(100),
                    max_delay: Duration::from_millis(5000),
                },
                Shares::Static(120),
                Latency::NotImportant,
            )
            .await
            .expect("Failed to run flusher");

            let compactor = SsTableCompactor::new(
                manifest.clone(),
                compaction_receiver,
                cpu_shard_id,
                Rc::clone(&storage_config),
                ss_tables_dir.clone(),
                Rc::clone(&ss_table_id_generator),
                RetryPolicy {
                    max_attempts: 5,
                    base_delay: Duration::from_millis(100),
                    max_delay: Duration::from_millis(5000),
                },
            )
            .await
            .expect("Failed to create compactor");

            compactor.spawn(Shares::Static(50), Latency::NotImportant);

            let command_handler = CommandHandler::new(
                flusher,
                manifest.clone(),
                ss_tables_dir.clone(),
                cpu_shard_id,
                Rc::clone(&storage_config),
            );

            let shard_router = ShardRouter::new(cpu_shard_id, shards_count as u32, command_handler);
            Listener::listen(
                cpu_shard_id,
                shard_router,
                Shares::Static(30),
                Latency::NotImportant,
                Shares::Static(600),
                Latency::NotImportant,
            );

            pending::<()>().await;
        })
        .expect("Failed to build executor pool");

    handles.join_all();
}
