mod entry;
mod manifest_internal;
pub mod snapshot;

use crate::manifest::manifest_internal::ManifestInternal;
use crate::ss_table_metadata::{SsTableId, SsTableMetadata};
use futures::StreamExt;
use glommio::channels::local_channel;
use glommio::channels::local_channel::{LocalReceiver, LocalSender};
use glommio::{GlommioError, Latency, Shares};
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
        let queue_name = format!("manifest-writer-{cpu_shard_id}");
        let task_queue =
            glommio::executor().create_task_queue(shares, latency, queue_name.as_str());

        let (sender, receiver) = local_channel::new_unbounded();

        glommio::spawn_local_into(Self::actor_loop(manifest, receiver), task_queue)
            .expect("failed to spawn manifest actor onto its task queue")
            .detach();

        Self {
            sender: Rc::new(sender),
        }
    }

    async fn actor_loop(mut manifest: ManifestInternal, receiver: LocalReceiver<ManifestCommand>) {
        let mut commands = receiver.stream();
        while let Some(cmd) = commands.next().await {
            match cmd {
                ManifestCommand::AddSsTable { metadata, reply } => {
                    let result = manifest.add_ss_table(metadata).await;
                    let _ = reply.try_send(result); // caller may have gone away, ignore
                }
                ManifestCommand::RemoveSsTable { id, reply } => {
                    let result = manifest.remove_ss_table(id).await;
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
}
