mod entry;
mod manifest_internal;
pub mod snapshot;

use crate::manifest::manifest_internal::ManifestInternal;
use crate::manifest::snapshot::ManifestSnapshot;
use crate::ss_table_metadata::{SsTableId, SsTableMetadata};
use futures::StreamExt;
use glommio::channels::local_channel;
use glommio::channels::local_channel::{LocalReceiver, LocalSender};
use glommio::{GlommioError, Latency, Shares};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

type ManifestReply = LocalSender<Result<(), GlommioError<()>>>;

pub enum ManifestCommand {
    AddSsTable {
        metadata: SsTableMetadata,
        reply: ManifestReply,
    },
    RemoveSsTable {
        id: SsTableId,
        reply: ManifestReply,
    },
}

#[derive(Clone)]
pub struct Manifest {
    sender: Rc<LocalSender<ManifestCommand>>,

    // First (outer) Rc is there so every cloned Manifest point at the same cell, so actor loop updates are visible to all of them,
    // RefCell is there so one writer (actor loop) can swap snapshot
    // Last (most inner) Rc is there so manifest clones are cheap (no need to reconstruct snapshot from scratch)
    // REMEMBER TO NEVER BORROW-THEN-AWAIT AS IT MAY LEAD TO ASYNC RACES AND PANIC !!!
    current_snapshot: Rc<RefCell<Rc<ManifestSnapshot>>>,
}

impl Manifest {
    pub async fn open_or_create(
        dir: &Path,
        cpu_shard_id: u32,
        shares: Shares,
        latency: Latency,
    ) -> Result<Self, GlommioError<()>> {
        let manifest = ManifestInternal::open_or_create(dir, cpu_shard_id).await?;
        Ok(Self::spawn(manifest, cpu_shard_id, shares, latency))
    }

    fn spawn(
        manifest: ManifestInternal,
        cpu_shard_id: u32,
        shares: Shares,
        latency: Latency,
    ) -> Self {
        let current_snapshot = Rc::new(RefCell::new(Rc::new(manifest.snapshot())));

        let queue_name = format!("manifest-writer-{cpu_shard_id}");
        let task_queue =
            glommio::executor().create_task_queue(shares, latency, queue_name.as_str());

        let (sender, receiver) = local_channel::new_unbounded();

        glommio::spawn_local_into(
            Self::actor_loop(manifest, receiver, Rc::clone(&current_snapshot)),
            task_queue,
        )
        .expect("failed to spawn manifest actor onto its task queue")
        .detach();

        Self {
            sender: Rc::new(sender),
            current_snapshot,
        }
    }

    async fn actor_loop(
        mut manifest: ManifestInternal,
        receiver: LocalReceiver<ManifestCommand>,
        current_snapshot: Rc<RefCell<Rc<ManifestSnapshot>>>,
    ) {
        let mut commands = receiver.stream();
        while let Some(cmd) = commands.next().await {
            match cmd {
                ManifestCommand::AddSsTable { metadata, reply } => {
                    let result = manifest.add_ss_table(metadata).await;
                    if result.is_ok() {
                        *current_snapshot.borrow_mut() = Rc::new(manifest.snapshot());
                    }
                    let _ = reply.try_send(result); // caller may have gone away, ignore
                }
                ManifestCommand::RemoveSsTable { id, reply } => {
                    let result = manifest.remove_ss_table(id).await;
                    if result.is_ok() {
                        *current_snapshot.borrow_mut() = Rc::new(manifest.snapshot());
                    }
                    let _ = reply.try_send(result);
                }
            }
        }
    }

    pub async fn add_ss_table(&self, metadata: SsTableMetadata) -> Result<(), GlommioError<()>> {
        let (reply_tx, reply_rx) = local_channel::new_bounded(1);
        self.sender
            .send(ManifestCommand::AddSsTable {
                metadata,
                reply: reply_tx,
            })
            .await
            .expect("manifest actor task is no longer running");

        reply_rx
            .stream()
            .next()
            .await
            .expect("manifest actor dropped without responding")
    }

    pub async fn remove_ss_table(&self, id: SsTableId) -> Result<(), GlommioError<()>> {
        let (reply_tx, reply_rx) = local_channel::new_bounded(1);
        self.sender
            .send(ManifestCommand::RemoveSsTable {
                id,
                reply: reply_tx,
            })
            .await
            .expect("manifest actor task is no longer running");

        reply_rx
            .stream()
            .next()
            .await
            .expect("manifest actor dropped without responding")
    }

    pub fn snapshot(&self) -> Rc<ManifestSnapshot> {
        self.current_snapshot.borrow().clone()
    }
}
