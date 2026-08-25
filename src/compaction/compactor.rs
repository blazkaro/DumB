use crate::compaction::merger::SsTableMerger;
use crate::manifest::handler::ManifestHandler;
use crate::manifest::snapshot::ManifestSnapshot;
use crate::manifest::snapshot_lookup::SnapshotLookup;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::ss_table::metadata::SsTableLevel;
use crate::storage_config::StorageConfig;
use futures::StreamExt;
use glommio::channels::local_channel::LocalReceiver;
use glommio::io::Directory;
use glommio::{GlommioError, Latency, Shares};
use std::cmp::{max, min};
use std::path::PathBuf;
use std::rc::Rc;

pub struct SsTableCompactor {
    cpu_shard_id: u32,
    trigger_receiver: LocalReceiver<()>,
    manifest: ManifestHandler,
    storage_config: Rc<StorageConfig>,
    parent: Directory,
    merger: SsTableMerger,
}

impl SsTableCompactor {
    pub async fn new(
        manifest: ManifestHandler,
        trigger_receiver: LocalReceiver<()>,
        cpu_shard_id: u32,
        storage_config: Rc<StorageConfig>,
        dir: PathBuf,
        id_generator: Rc<SsTableIdGenerator>,
    ) -> Result<Self, GlommioError<()>> {
        let parent = Directory::open(&dir).await?;
        let merger = SsTableMerger::new(
            cpu_shard_id,
            dir.clone(),
            Rc::clone(&id_generator),
            manifest.clone(),
            Rc::clone(&storage_config),
        );

        Ok(Self {
            cpu_shard_id,
            trigger_receiver,
            manifest,
            storage_config,
            parent,
            merger,
        })
    }

    pub async fn spawn(self, shares: Shares, latency: Latency) {
        let queue_name = format!("ss-table-compactor-{}", self.cpu_shard_id);
        let task_queue =
            glommio::executor().create_task_queue(shares, latency, queue_name.as_str());

        glommio::spawn_local_into(self.run(), task_queue)
            .expect("failed to spawn compactor onto its task queue")
            .detach();
    }

    async fn run(mut self) {
        let manifest = self.manifest.clone();
        let mut signals = self.trigger_receiver.stream();

        while signals.next().await.is_some() {
            let _ = self.checked_compaction(&manifest).await;
        }
    }

    async fn checked_compaction(&self, manifest: &ManifestHandler) -> Result<(), GlommioError<()>> {
        let mut snapshot = manifest.snapshot();
        loop {
            let l0_score = snapshot.table_count_at_level(0) as f32
                / self.storage_config.ss_table_level_0_target_tables_count as f32;

            let candidate = std::iter::once((0 as SsTableLevel, l0_score))
                .chain(
                    (1..snapshot.levels_count())
                        .map(|level| level as SsTableLevel)
                        .map(|level| {
                            let target_size = self.storage_config.ss_table_level_1_target_size_bytes
                                as u64
                                * (self.storage_config.ss_table_level_growth_factor as u64)
                                    .pow(level as u32);

                            let score = snapshot.get_level_size(level) as f32 / target_size as f32;
                            (level, score)
                        }),
                )
                .filter(|(_, score)| *score >= 1.0)
                .max_by(|a, b| a.1.total_cmp(&b.1));

            let Some((level, _score)) = candidate else {
                break;
            };

            if level == 0 {
                self.merge_l0_into_l1(&snapshot).await?;
            } else {
                self.merge_target_level(level + 1, &snapshot).await?;
            }

            snapshot = manifest.snapshot();
        }

        Ok(())
    }

    async fn merge_l0_into_l1(&self, snapshot: &ManifestSnapshot) -> Result<(), GlommioError<()>> {
        // Get global range (min key, max key) for level 0 and get files from level 1 which contains this range
        let level_0 = snapshot
            .get_level(0)
            .expect("compaction should not run when level 0 is empty");

        // O(n), but level 0 stays small by design (capped well below u8::MAX tables)
        let mut tables = level_0.iter();
        let first = tables
            .next()
            .expect("level 0 must have at least one table in order to perform compaction");

        let (mut level_0_min_key, mut level_0_max_key) = (&first.min_key, &first.max_key);

        for table in tables {
            level_0_min_key = min(level_0_min_key, &table.min_key);
            level_0_max_key = max(level_0_max_key, &table.max_key);
        }

        let level_1_overlap =
            SnapshotLookup::get_overlap(1, level_0_min_key.clone(), level_0_max_key, snapshot);

        let table_ids = level_0
            .iter()
            .map(|t| t.id)
            .chain(level_1_overlap.into_iter().map(|t| t.id))
            .collect();

        self.merger.merge_ss_tables(table_ids, 1).await
    }

    async fn merge_target_level(
        &self,
        target_level: SsTableLevel,
        snapshot: &ManifestSnapshot,
    ) -> Result<(), GlommioError<()>> {
        // Get first ss table and merge it into overlapping range
        let prev_level = target_level - 1;
        if prev_level <= 0 {
            panic!("This method only works on target level > 1");
        }

        let mut prev_level = snapshot.get_level(prev_level).unwrap_or_default();
        if prev_level.is_empty() {
            return Ok(());
        }

        let prev_level_ss_table = prev_level.first().unwrap();

        let mut table_ids = Vec::with_capacity(1);
        table_ids.push(prev_level_ss_table.id);

        let target_level_overlap = SnapshotLookup::get_overlap(
            target_level,
            prev_level_ss_table.min_key.clone(),
            &prev_level_ss_table.max_key,
            &snapshot,
        );
        table_ids.extend(target_level_overlap.into_iter().map(|t| t.id));

        self.merger.merge_ss_tables(table_ids, target_level).await
    }
}
